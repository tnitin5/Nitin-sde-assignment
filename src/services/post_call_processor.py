"""PostCallProcessor — runs LLM analysis on a completed call transcript.

Every long-transcript interaction reaches this class. Before the LLM call
fires, the rate limiter is consulted: if the customer's token budget or
the platform's last-60s usage cannot cover the estimated cost the call
is deferred (RateLimitDeferred) and the worker retries it after the
limiter's suggested delay.

The estimated token count is reserved with the limiter, the LLM call
runs, and the actual tokens_used is reconciled via finalize() so the
sliding window reflects real consumption rather than the estimate.

The single_prompt path runs entity extraction, classification and
summarisation in one call. Splitting them is a future optimisation
documented in the design.
"""

import json
from datetime import datetime
from typing import Any, Dict, Optional
from dataclasses import dataclass, field

from src.config import settings
from src.services.rate_limiter import (
    AcquireResult,
    rate_limiter as default_rate_limiter,
    RateLimiter,
)
from src.utils import audit


class RateLimitDeferred(Exception):
    """Raised when the rate limiter declines to admit a call.

    The worker catches this and reschedules with the limiter-supplied
    retry_after_ms — distinct from a generic processing failure.
    """

    def __init__(self, decision_result: str, retry_after_ms: int, reason: str):
        self.decision_result = decision_result
        self.retry_after_ms = retry_after_ms
        self.reason = reason
        super().__init__(f"rate_limit_deferred: {decision_result} ({reason})")


@dataclass
class PostCallContext:
    interaction_id: str
    session_id: str
    lead_id: str
    campaign_id: str
    customer_id: str
    agent_id: str
    call_sid: str
    transcript_text: str
    conversation_data: dict
    additional_data: dict
    ended_at: datetime
    exotel_account_id: Optional[str] = None
    correlation_id: Optional[str] = None
    priority: str = "cold"


@dataclass
class AnalysisResult:
    call_stage: str
    entities: Dict[str, Any]
    summary: str
    raw_response: Dict[str, Any]
    tokens_used: int
    latency_ms: float
    provider: str
    model: str


def estimate_tokens(transcript_text: str) -> int:
    # ~1.3 tokens per word for input, plus a response budget. Always
    # pessimistic — the limiter refunds via finalize() once the actual
    # tokens_used comes back from the provider.
    word_estimate = int(max(len(transcript_text.split()), 1) * 1.3)
    return max(word_estimate + 500, settings.LLM_AVG_TOKENS_PER_CALL)


class PostCallProcessor:

    def __init__(self, rate_limiter: Optional[RateLimiter] = None):
        self._rate_limiter = rate_limiter or default_rate_limiter

    async def process_post_call(
        self, ctx: PostCallContext, single_prompt: bool = True
    ) -> AnalysisResult:
        est_tokens = estimate_tokens(ctx.transcript_text)

        decision = await self._rate_limiter.try_acquire(
            ctx.customer_id,
            est_tokens,
            priority=ctx.priority,
            correlation_id=ctx.correlation_id,
            interaction_id=ctx.interaction_id,
        )

        if decision.result not in (AcquireResult.OK, AcquireResult.OK_OVERFLOW):
            raise RateLimitDeferred(
                decision_result=decision.result.value,
                retry_after_ms=decision.retry_after_ms,
                reason=decision.reason,
            )

        reservation_id = decision.reservation_id
        assert reservation_id is not None

        audit.emit(
            "llm", "analysis_started",
            correlation_id=ctx.correlation_id,
            interaction_id=ctx.interaction_id,
            customer_id=ctx.customer_id,
            campaign_id=ctx.campaign_id,
            est_tokens=est_tokens,
            reservation_id=reservation_id,
            priority=ctx.priority,
        )

        prompt = self._build_analysis_prompt(
            ctx.transcript_text, ctx.additional_data, single_prompt,
        )

        start_time = datetime.utcnow()
        try:
            response = await self._call_llm(prompt)
        except Exception as e:
            await self._rate_limiter.release(reservation_id, ctx.customer_id)
            audit.emit(
                "llm", "analysis_failed",
                status="fail",
                correlation_id=ctx.correlation_id,
                interaction_id=ctx.interaction_id,
                customer_id=ctx.customer_id,
                reservation_id=reservation_id,
                error=str(e),
            )
            raise

        elapsed_ms = (datetime.utcnow() - start_time).total_seconds() * 1000
        result = self._parse_response(response, elapsed_ms)

        await self._rate_limiter.finalize(
            reservation_id, result.tokens_used, customer_id=ctx.customer_id,
        )

        await self._update_interaction_metadata(ctx.interaction_id, result)

        audit.emit(
            "llm", "analysis_complete",
            correlation_id=ctx.correlation_id,
            interaction_id=ctx.interaction_id,
            customer_id=ctx.customer_id,
            campaign_id=ctx.campaign_id,
            call_stage=result.call_stage,
            tokens_estimated=est_tokens,
            tokens_used=result.tokens_used,
            latency_ms=result.latency_ms,
            reservation_id=reservation_id,
        )

        return result

    def _build_analysis_prompt(
        self,
        transcript: str,
        additional_data: dict,
        single_prompt: bool,
    ) -> str:
        system_prompt = """You are a call analysis assistant. Analyze the following
call transcript and extract:
1. call_stage: The outcome/disposition of the call
2. entities: Key information mentioned (dates, times, amounts, names, preferences)
3. summary: A brief summary of what happened in the call

Respond in JSON format:
{
    "call_stage": "...",
    "entities": {...},
    "summary": "..."
}"""

        return (
            f"{system_prompt}\n\n"
            f"Transcript:\n{transcript}\n\n"
            f"Additional context:\n{json.dumps(additional_data)}"
        )

    async def _call_llm(self, prompt: str) -> dict:
        # Mock — production hits httpx POST to the provider's API and parses
        # `usage.total_tokens` from the response.
        return {
            "call_stage": "unknown",
            "entities": {},
            "summary": "Mock analysis result",
            "usage": {"total_tokens": 1500},
        }

    def _parse_response(self, response: dict, latency_ms: float) -> AnalysisResult:
        return AnalysisResult(
            call_stage=response.get("call_stage", "unknown"),
            entities=response.get("entities", {}),
            summary=response.get("summary", ""),
            raw_response=response,
            tokens_used=response.get("usage", {}).get("total_tokens", 0),
            latency_ms=latency_ms,
            provider=settings.LLM_PROVIDER,
            model=settings.LLM_MODEL,
        )

    async def _update_interaction_metadata(
        self, interaction_id: str, result: AnalysisResult
    ) -> None:
        # Production: UPDATE interactions SET interaction_metadata = ... WHERE id = $1
        return


post_call_processor = PostCallProcessor()
