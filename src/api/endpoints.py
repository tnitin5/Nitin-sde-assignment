"""
FastAPI endpoint for ending an interaction.

POST /session/{session_id}/interaction/{interaction_id}/end

Called by Exotel when a call disconnects. Exotel has a 5-second timeout,
so heavy work is handed off to Celery and the endpoint returns immediately.

A correlation_id is generated here at intake and threaded through every
downstream step (Celery payload, audit events, signal jobs).

Short transcripts (<4 turns) skip the LLM entirely and fire downstream
actions inline. For long transcripts the endpoint enqueues to Celery
and returns. Signal jobs run from the worker after analysis completes —
no pre-firing with empty payloads.
"""

import asyncio
from datetime import datetime
from typing import Any, Dict, Optional
from uuid import UUID

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel

from src.services.signal_jobs import trigger_signal_jobs, update_lead_stage
from src.tasks.celery_tasks import process_interaction_end_background_task
from src.utils import audit

router = APIRouter()


class InteractionEndRequest(BaseModel):
    call_sid: Optional[str] = None
    duration_seconds: Optional[int] = None
    call_status: Optional[str] = None
    additional_data: Optional[Dict[str, Any]] = None


class InteractionEndResponse(BaseModel):
    status: str
    interaction_id: str
    correlation_id: str
    message: str


@router.post(
    "/session/{session_id}/interaction/{interaction_id}/end",
    response_model=InteractionEndResponse,
)
async def end_interaction(
    session_id: UUID,
    interaction_id: UUID,
    request: InteractionEndRequest,
    background_tasks: BackgroundTasks,
):
    correlation_id = audit.new_correlation_id()

    try:
        interaction = await _load_interaction(interaction_id)

        if not interaction:
            audit.emit(
                "intake", "interaction_not_found",
                status="fail",
                correlation_id=correlation_id,
                interaction_id=str(interaction_id),
            )
            raise HTTPException(status_code=404, detail="Interaction not found")

        customer_id = interaction["customer_id"]
        campaign_id = interaction["campaign_id"]

        audit.emit(
            "intake", "webhook_received",
            correlation_id=correlation_id,
            interaction_id=str(interaction_id),
            customer_id=customer_id,
            campaign_id=campaign_id,
            call_sid=request.call_sid,
            duration_seconds=request.duration_seconds,
        )

        await _update_interaction_status(
            interaction_id=str(interaction_id),
            status="ENDED",
            ended_at=datetime.utcnow(),
            duration=request.duration_seconds,
            call_sid=request.call_sid,
        )

        transcript = interaction.get("conversation_data", {}).get("transcript", [])
        is_short = len(transcript) < 4

        if is_short:
            audit.emit(
                "intake", "short_transcript_fast_path",
                correlation_id=correlation_id,
                interaction_id=str(interaction_id),
                customer_id=customer_id,
                turn_count=len(transcript),
            )
            asyncio.create_task(
                trigger_signal_jobs(
                    interaction_id=str(interaction_id),
                    session_id=str(session_id),
                    campaign_id=campaign_id,
                    analysis_result={"call_stage": "short_call"},
                )
            )
            asyncio.create_task(
                update_lead_stage(
                    lead_id=interaction["lead_id"],
                    interaction_id=str(interaction_id),
                    call_stage="short_call",
                )
            )

        else:
            transcript_text = "\n".join(
                f"{turn.get('role', 'unknown')}: {turn.get('content', '')}"
                for turn in transcript
            )

            celery_payload = {
                "interaction_id": str(interaction_id),
                "session_id": str(session_id),
                "lead_id": interaction["lead_id"],
                "campaign_id": campaign_id,
                "customer_id": customer_id,
                "agent_id": interaction["agent_id"],
                "call_sid": request.call_sid,
                "transcript_text": transcript_text,
                "conversation_data": interaction.get("conversation_data", {}),
                "additional_data": request.additional_data or {},
                "ended_at": datetime.utcnow().isoformat(),
                "exotel_account_id": interaction.get("exotel_account_id"),
                "correlation_id": correlation_id,
            }

            task = process_interaction_end_background_task.apply_async(
                args=[celery_payload],
                queue="postcall_processing",
            )

            audit.emit(
                "intake", "postcall_enqueued",
                correlation_id=correlation_id,
                interaction_id=str(interaction_id),
                customer_id=customer_id,
                celery_task_id=task.id,
                turn_count=len(transcript),
            )

        return InteractionEndResponse(
            status="ok",
            interaction_id=str(interaction_id),
            correlation_id=correlation_id,
            message="Interaction ended, processing enqueued",
        )

    except HTTPException:
        raise
    except Exception as e:
        audit.emit(
            "intake", "endpoint_failed",
            status="fail",
            correlation_id=correlation_id,
            interaction_id=str(interaction_id),
            error=str(e),
        )
        raise HTTPException(status_code=500, detail="Internal server error")


async def _load_interaction(interaction_id: UUID) -> Optional[Dict[str, Any]]:
    """Mock — returns a realistic sample for local development."""
    return {
        "id": str(interaction_id),
        "lead_id": "mock-lead-id",
        "campaign_id": "mock-campaign-id",
        "customer_id": "mock-customer-id",
        "agent_id": "mock-agent-id",
        "exotel_account_id": "mock-exotel-account",
        "conversation_data": {
            "transcript": [
                {"role": "agent", "content": "Hello, am I speaking with Mr. Sharma?"},
                {"role": "customer", "content": "Yes, speaking."},
                {"role": "agent", "content": "I'm calling from XYZ about your recent inquiry."},
                {"role": "customer", "content": "Oh yes, I was looking at the product."},
                {"role": "agent", "content": "Would you like to schedule a demo?"},
                {"role": "customer", "content": "Sure, let's do tomorrow at 3 PM."},
                {"role": "agent", "content": "Perfect, I've booked a demo for tomorrow at 3 PM."},
                {"role": "customer", "content": "Thank you, bye."},
            ]
        },
    }


async def _update_interaction_status(
    interaction_id: str,
    status: str,
    ended_at: datetime,
    duration: Optional[int],
    call_sid: Optional[str],
) -> None:
    """Mock — production would UPDATE interactions row."""
    return
