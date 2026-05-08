"""Celery tasks for post-call processing.

The task fetches the recording, runs LLM analysis (rate-limited via
RateLimiter), then dispatches signal jobs and the lead-stage update.

When the rate limiter declines a call (RateLimitDeferred) the worker
retries with the limiter-supplied countdown rather than the default
fixed delay — so deferred work returns precisely when headroom is
expected, not arbitrarily later.

Generic processing failures still flow through the legacy retry queue
plus Celery's own retry. The Postgres-outbox replacement for that pair
is documented in the design doc but not implemented in this commit.
"""

import asyncio
from datetime import datetime
from typing import Any, Dict

from src.tasks.celery_app import celery_app
from src.services.post_call_processor import (
    PostCallProcessor,
    PostCallContext,
    RateLimitDeferred,
)
from src.services.recording import fetch_and_upload_recording
from src.services.signal_jobs import trigger_signal_jobs, update_lead_stage
from src.services.retry_queue import retry_queue
from src.services.metrics import metrics_tracker
from src.utils import audit


@celery_app.task(
    name="process_interaction_end_background_task",
    bind=True,
    max_retries=5,
    default_retry_delay=60,
    acks_late=True,
    queue="postcall_processing",
)
def process_interaction_end_background_task(self, payload: Dict[str, Any]):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    interaction_id = payload.get("interaction_id")
    correlation_id = payload.get("correlation_id")

    try:
        loop.run_until_complete(_process_interaction(self, payload))
    except RateLimitDeferred as e:
        countdown = max(e.retry_after_ms // 1000, 1)
        audit.emit(
            "worker", "rate_limit_deferred",
            status="retry",
            correlation_id=correlation_id,
            interaction_id=interaction_id,
            decision=e.decision_result,
            countdown_s=countdown,
            reason=e.reason,
            attempt=self.request.retries,
        )
        raise self.retry(exc=e, countdown=countdown)
    except Exception as e:
        audit.emit(
            "worker", "task_failed",
            status="fail",
            correlation_id=correlation_id,
            interaction_id=interaction_id,
            error=str(e),
            attempt=self.request.retries,
        )
        loop.run_until_complete(
            retry_queue.enqueue_retry(
                interaction_id=interaction_id,
                error=str(e),
                payload=payload,
            )
        )
        raise self.retry(exc=e)
    finally:
        loop.close()


async def _process_interaction(task, payload: Dict[str, Any]):
    interaction_id = payload["interaction_id"]
    correlation_id = payload.get("correlation_id")

    await metrics_tracker.track_processing_started(interaction_id)

    ctx = PostCallContext(
        interaction_id=interaction_id,
        session_id=payload["session_id"],
        lead_id=payload["lead_id"],
        campaign_id=payload["campaign_id"],
        customer_id=payload["customer_id"],
        agent_id=payload["agent_id"],
        call_sid=payload.get("call_sid", ""),
        transcript_text=payload.get("transcript_text", ""),
        conversation_data=payload.get("conversation_data", {}),
        additional_data=payload.get("additional_data", {}),
        ended_at=datetime.fromisoformat(payload["ended_at"]),
        exotel_account_id=payload.get("exotel_account_id"),
        correlation_id=correlation_id,
        priority=payload.get("priority", "cold"),
    )

    await fetch_and_upload_recording(
        interaction_id=ctx.interaction_id,
        call_sid=ctx.call_sid,
        exotel_account_id=ctx.exotel_account_id or "",
        correlation_id=correlation_id,
        customer_id=ctx.customer_id,
    )

    processor = PostCallProcessor()
    result = await processor.process_post_call(ctx, single_prompt=True)

    await metrics_tracker.track_processing_completed(
        interaction_id, result.tokens_used, result.latency_ms
    )

    try:
        await trigger_signal_jobs(
            interaction_id=ctx.interaction_id,
            session_id=ctx.session_id,
            campaign_id=ctx.campaign_id,
            analysis_result=result.raw_response,
        )
    except Exception as e:
        audit.emit(
            "signal_jobs", "dispatch_failed",
            status="fail",
            correlation_id=correlation_id,
            interaction_id=interaction_id,
            customer_id=ctx.customer_id,
            error=str(e),
        )

    try:
        await update_lead_stage(
            lead_id=ctx.lead_id,
            interaction_id=ctx.interaction_id,
            call_stage=result.call_stage,
        )
    except Exception as e:
        audit.emit(
            "lead_stage", "update_failed",
            status="fail",
            correlation_id=correlation_id,
            interaction_id=interaction_id,
            customer_id=ctx.customer_id,
            error=str(e),
        )

    audit.emit(
        "worker", "task_done",
        correlation_id=correlation_id,
        interaction_id=interaction_id,
        customer_id=ctx.customer_id,
        call_stage=result.call_stage,
        tokens_used=result.tokens_used,
    )
