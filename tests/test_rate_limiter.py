"""Tests for the per-customer rate limiter.

Validates AC1 (no LLM call exceeds rate limits) and AC2 (one customer's
budget cannot consume another's allocation). Uses an in-memory FakeRedis
that implements the sorted-set operations the limiter relies on.
"""

from typing import Dict
from unittest.mock import patch

import pytest

from src.services.rate_limiter import (
    AcquireResult,
    CustomerBudget,
    GLOBAL_KEY,
    RateLimiter,
    _customer_key,
)


class FakeRedis:
    """In-memory stand-in for the sorted-set ops the limiter uses."""

    def __init__(self):
        self._zsets: Dict[str, Dict[str, float]] = {}

    async def zadd(self, key, mapping):
        z = self._zsets.setdefault(key, {})
        z.update(mapping)
        return len(mapping)

    async def zrange(self, key, start, end, withscores=False):
        z = self._zsets.get(key, {})
        items = sorted(z.items(), key=lambda kv: kv[1])
        items = items[start:] if end == -1 else items[start : end + 1]
        if withscores:
            return [(m, s) for m, s in items]
        return [m for m, _ in items]

    async def zremrangebyscore(self, key, min_score, max_score):
        z = self._zsets.get(key, {})
        to_remove = [m for m, s in z.items() if min_score <= s <= max_score]
        for m in to_remove:
            del z[m]
        return len(to_remove)

    async def zrem(self, key, *members):
        z = self._zsets.get(key, {})
        removed = 0
        for m in members:
            if m in z:
                del z[m]
                removed += 1
        return removed

    async def expire(self, key, seconds):
        return 1


def _make_budget(customer_id: str) -> CustomerBudget:
    if customer_id == "cust-A":
        return CustomerBudget(customer_id, reserved_tpm=10_000, allow_overflow=True)
    if customer_id == "cust-B":
        return CustomerBudget(customer_id, reserved_tpm=10_000, allow_overflow=True)
    if customer_id == "cust-strict":
        return CustomerBudget(customer_id, reserved_tpm=1_000, allow_overflow=False)
    return CustomerBudget(customer_id, reserved_tpm=5_000, allow_overflow=True)


@pytest.fixture
def limiter():
    fake = FakeRedis()
    with patch("src.services.rate_limiter.redis_client", fake):
        yield RateLimiter(get_budget=_make_budget)


@pytest.mark.asyncio
async def test_acquire_within_budget_returns_ok(limiter):
    decision = await limiter.try_acquire("cust-A", est_tokens=1500)
    assert decision.result == AcquireResult.OK
    assert decision.reservation_id is not None


@pytest.mark.asyncio
async def test_burst_exceeding_global_rejects(limiter, monkeypatch):
    monkeypatch.setattr(
        "src.services.rate_limiter.settings.LLM_TOKENS_PER_MINUTE", 5_000
    )

    first = await limiter.try_acquire("cust-A", est_tokens=4_500)
    assert first.result == AcquireResult.OK

    second = await limiter.try_acquire("cust-A", est_tokens=2_000)
    assert second.result in (
        AcquireResult.WAIT,
        AcquireResult.REJECT_DEFER,
        AcquireResult.REJECT_TO_COLD,
    )
    assert second.reservation_id is None


@pytest.mark.asyncio
async def test_customer_isolation(limiter):
    # Customer A burns through several reservations.
    for _ in range(6):
        await limiter.try_acquire("cust-A", est_tokens=1_500)

    # Customer B still gets the full reserved allocation.
    decision = await limiter.try_acquire("cust-B", est_tokens=5_000)
    assert decision.result == AcquireResult.OK


@pytest.mark.asyncio
async def test_finalize_replaces_estimate_with_actual(limiter):
    decision = await limiter.try_acquire("cust-A", est_tokens=2_000)
    assert decision.result == AcquireResult.OK

    await limiter.finalize(
        decision.reservation_id, actual_tokens=800, customer_id="cust-A"
    )

    used = await limiter._sum_tokens(_customer_key("cust-A"))
    assert used == 800


@pytest.mark.asyncio
async def test_release_drops_reservation(limiter):
    decision = await limiter.try_acquire("cust-A", est_tokens=2_000)
    assert decision.result == AcquireResult.OK

    await limiter.release(decision.reservation_id, customer_id="cust-A")

    used_cust = await limiter._sum_tokens(_customer_key("cust-A"))
    used_global = await limiter._sum_tokens(GLOBAL_KEY)
    assert used_cust == 0
    assert used_global == 0


@pytest.mark.asyncio
async def test_no_overflow_customer_rejected_when_over_budget(limiter, monkeypatch):
    monkeypatch.setattr(
        "src.services.rate_limiter.settings.LLM_TOKENS_PER_MINUTE", 90_000
    )

    first = await limiter.try_acquire("cust-strict", est_tokens=900)
    assert first.result == AcquireResult.OK

    # Plenty of global headroom but the customer can't overflow.
    second = await limiter.try_acquire("cust-strict", est_tokens=500)
    assert second.result != AcquireResult.OK_OVERFLOW
    assert second.result in (
        AcquireResult.WAIT,
        AcquireResult.REJECT_DEFER,
        AcquireResult.REJECT_TO_COLD,
    )


@pytest.mark.asyncio
async def test_hot_priority_demotes_to_cold_when_global_saturated(
    limiter, monkeypatch
):
    monkeypatch.setattr(
        "src.services.rate_limiter.settings.LLM_TOKENS_PER_MINUTE", 5_000
    )

    await limiter.try_acquire("cust-A", est_tokens=4_500)

    decision = await limiter.try_acquire(
        "cust-A", est_tokens=2_000, priority="hot"
    )
    assert decision.result == AcquireResult.REJECT_TO_COLD
    assert decision.retry_after_ms > 0


@pytest.mark.asyncio
async def test_overflow_used_when_customer_over_budget_but_global_has_room(
    limiter, monkeypatch
):
    monkeypatch.setattr(
        "src.services.rate_limiter.settings.LLM_TOKENS_PER_MINUTE", 90_000
    )

    # cust-A is at 9000 (within 10k reserved). Next 2000 would push to 11k —
    # over their reserved budget but well under global headroom.
    for _ in range(6):
        await limiter.try_acquire("cust-A", est_tokens=1_500)

    decision = await limiter.try_acquire("cust-A", est_tokens=2_000)
    assert decision.result == AcquireResult.OK_OVERFLOW
    assert decision.reservation_id is not None
