"""Per-customer LLM rate limiter with sliding-window token tracking.

Sits between the worker and the LLM provider. try_acquire() admits or
defers a call based on (a) the customer's reserved TPM budget and
(b) the platform's last-60s usage.

After the LLM responds the caller calls finalize() with the actual
tokens_used to reconcile the estimated reservation. release() drops a
reservation if the call errored before producing a token count.

Reservations live in two Redis sorted sets — one global, one per
customer — with the reservation_id and token count encoded in each
member. A 90s key TTL ensures a crashed worker's reservations decay
even if finalize/release is never called.
"""

import asyncio
import time
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Optional

from src.config import settings
from src.utils import audit
from src.utils.redis_client import redis_client


WINDOW_MS = 60 * 1000
RESERVATION_TTL_SECONDS = 90
GLOBAL_KEY = "rl:tpm:global"


class AcquireResult(str, Enum):
    OK = "ok"
    OK_OVERFLOW = "ok_overflow"
    WAIT = "wait"
    REJECT_TO_COLD = "reject_to_cold"
    REJECT_DEFER = "reject_defer"


@dataclass
class AcquireDecision:
    result: AcquireResult
    reservation_id: Optional[str] = None
    retry_after_ms: int = 0
    reason: str = ""
    customer_used_tpm: int = 0
    global_used_tpm: int = 0


@dataclass
class CustomerBudget:
    customer_id: str
    reserved_tpm: int
    allow_overflow: bool = True
    hot_quota_pct: float = 0.30


def _default_budget(customer_id: str) -> CustomerBudget:
    # Fallback when no row exists in customer_budgets — 5% of platform TPM.
    return CustomerBudget(
        customer_id=customer_id,
        reserved_tpm=int(settings.LLM_TOKENS_PER_MINUTE * 0.05),
        allow_overflow=True,
        hot_quota_pct=0.30,
    )


def _customer_key(customer_id: str) -> str:
    return f"rl:tpm:cust:{customer_id}"


def _encode(reservation_id: str, tokens: int) -> str:
    return f"{reservation_id}:{tokens}"


def _decode_tokens(member: str) -> int:
    try:
        _, tokens_str = member.rsplit(":", 1)
        return int(tokens_str)
    except (ValueError, IndexError):
        return 0


BudgetLoader = Callable[[str], "CustomerBudget"]


class RateLimiter:

    def __init__(self, get_budget: Optional[BudgetLoader] = None):
        self._get_budget = get_budget or _default_budget

    async def try_acquire(
        self,
        customer_id: str,
        est_tokens: int,
        *,
        priority: str = "cold",
        correlation_id: Optional[str] = None,
        interaction_id: Optional[str] = None,
    ) -> AcquireDecision:
        budget = await self._resolve_budget(customer_id)

        now_ms = int(time.time() * 1000)
        cutoff_ms = now_ms - WINDOW_MS

        cust_key = _customer_key(customer_id)
        await asyncio.gather(
            redis_client.zremrangebyscore(cust_key, 0, cutoff_ms),
            redis_client.zremrangebyscore(GLOBAL_KEY, 0, cutoff_ms),
        )

        cust_used, global_used = await asyncio.gather(
            self._sum_tokens(cust_key),
            self._sum_tokens(GLOBAL_KEY),
        )

        platform_tpm = settings.LLM_TOKENS_PER_MINUTE
        global_headroom = max(platform_tpm - global_used, 0)
        cust_headroom = max(budget.reserved_tpm - cust_used, 0)

        decision = self._decide(
            est_tokens, cust_used, global_used,
            cust_headroom, global_headroom, budget, priority,
        )

        if decision.result in (AcquireResult.OK, AcquireResult.OK_OVERFLOW):
            decision.reservation_id = await self._reserve(
                cust_key, est_tokens, now_ms
            )

        self._audit(decision, customer_id, est_tokens, priority,
                    correlation_id, interaction_id)
        return decision

    async def finalize(
        self,
        reservation_id: str,
        actual_tokens: int,
        *,
        customer_id: str,
    ) -> None:
        cust_key = _customer_key(customer_id)
        await asyncio.gather(
            self._replace_reservation(cust_key, reservation_id, actual_tokens),
            self._replace_reservation(GLOBAL_KEY, reservation_id, actual_tokens),
        )

    async def release(self, reservation_id: str, customer_id: str) -> None:
        cust_key = _customer_key(customer_id)
        await asyncio.gather(
            self._drop_reservation(cust_key, reservation_id),
            self._drop_reservation(GLOBAL_KEY, reservation_id),
        )

    async def _resolve_budget(self, customer_id: str) -> CustomerBudget:
        result = self._get_budget(customer_id)
        if asyncio.iscoroutine(result):
            return await result
        return result

    def _decide(
        self,
        est_tokens: int,
        cust_used: int,
        global_used: int,
        cust_headroom: int,
        global_headroom: int,
        budget: CustomerBudget,
        priority: str,
    ) -> AcquireDecision:
        base = {
            "customer_used_tpm": cust_used,
            "global_used_tpm": global_used,
        }

        if est_tokens <= cust_headroom and est_tokens <= global_headroom:
            return AcquireDecision(result=AcquireResult.OK, **base)

        if est_tokens <= global_headroom and budget.allow_overflow:
            return AcquireDecision(
                result=AcquireResult.OK_OVERFLOW,
                reason="customer over reserved, drawing from shared headroom",
                **base,
            )

        retry_after_ms = WINDOW_MS // 4

        if priority == "hot":
            return AcquireDecision(
                result=AcquireResult.REJECT_TO_COLD,
                retry_after_ms=retry_after_ms,
                reason="global saturated, demoting hot to cold lane",
                **base,
            )

        if est_tokens <= cust_headroom:
            return AcquireDecision(
                result=AcquireResult.WAIT,
                retry_after_ms=retry_after_ms,
                reason="customer has budget but platform saturated",
                **base,
            )

        return AcquireDecision(
            result=AcquireResult.REJECT_DEFER,
            retry_after_ms=retry_after_ms,
            reason="customer over budget and platform saturated",
            **base,
        )

    async def _reserve(self, cust_key: str, tokens: int, now_ms: int) -> str:
        reservation_id = str(uuid.uuid4())
        member = _encode(reservation_id, tokens)
        await asyncio.gather(
            redis_client.zadd(cust_key, {member: now_ms}),
            redis_client.zadd(GLOBAL_KEY, {member: now_ms}),
        )
        await asyncio.gather(
            redis_client.expire(cust_key, RESERVATION_TTL_SECONDS),
            redis_client.expire(GLOBAL_KEY, RESERVATION_TTL_SECONDS),
        )
        return reservation_id

    async def _sum_tokens(self, key: str) -> int:
        members = await redis_client.zrange(key, 0, -1)
        return sum(_decode_tokens(m) for m in members)

    async def _replace_reservation(
        self, key: str, reservation_id: str, new_tokens: int
    ) -> None:
        members = await redis_client.zrange(key, 0, -1, withscores=True)
        prefix = f"{reservation_id}:"
        for member, score in members:
            if member.startswith(prefix):
                await redis_client.zrem(key, member)
                new_member = _encode(reservation_id, new_tokens)
                await redis_client.zadd(key, {new_member: score})
                return

    async def _drop_reservation(self, key: str, reservation_id: str) -> None:
        members = await redis_client.zrange(key, 0, -1)
        prefix = f"{reservation_id}:"
        for member in members:
            if member.startswith(prefix):
                await redis_client.zrem(key, member)
                return

    def _audit(
        self,
        decision: AcquireDecision,
        customer_id: str,
        est_tokens: int,
        priority: str,
        correlation_id: Optional[str],
        interaction_id: Optional[str],
    ) -> None:
        if decision.result in (AcquireResult.OK, AcquireResult.OK_OVERFLOW):
            status = "ok"
        elif decision.result == AcquireResult.WAIT:
            status = "retry"
        else:
            status = "fail"
        audit.emit(
            "rate_limiter",
            f"acquire_{decision.result.value}",
            status=status,
            correlation_id=correlation_id,
            interaction_id=interaction_id,
            customer_id=customer_id,
            est_tokens=est_tokens,
            priority=priority,
            reservation_id=decision.reservation_id,
            customer_used_tpm=decision.customer_used_tpm,
            global_used_tpm=decision.global_used_tpm,
            retry_after_ms=decision.retry_after_ms,
            reason=decision.reason,
        )


rate_limiter = RateLimiter()
