"""Tests for the recording exponential-backoff poller — AC4.

Validates that fetch_and_upload_recording retries on the documented
schedule, emits structured audit events at every attempt, returns the
S3 key on success, and emits an alertable permanent-failure event when
all attempts are exhausted.

asyncio.sleep is patched out so tests run instantly instead of waiting
the full 155 seconds.
"""

from unittest.mock import AsyncMock, patch

import pytest

from src.services import recording


@pytest.fixture
def captured_events():
    events = []

    def fake_emit(stage, event, **kwargs):
        events.append({"stage": stage, "event": event, **kwargs})

    with patch.object(recording.audit, "emit", side_effect=fake_emit):
        yield events


@pytest.fixture
def fast_sleep():
    with patch.object(recording.asyncio, "sleep", new=AsyncMock(return_value=None)):
        yield


@pytest.mark.asyncio
async def test_recording_succeeds_on_first_attempt(captured_events, fast_sleep):
    with patch.object(
        recording, "_fetch_exotel_recording_url",
        new=AsyncMock(return_value="https://exotel.example/rec.mp3"),
    ):
        result = await recording.fetch_and_upload_recording(
            interaction_id="i-1",
            call_sid="c-1",
            exotel_account_id="acc-1",
            correlation_id="cor-1",
            customer_id="cust-1",
        )

    assert result == "recordings/i-1.mp3"

    uploaded = [e for e in captured_events if e["event"] == "uploaded"]
    assert len(uploaded) == 1
    assert uploaded[0]["attempt"] == 1
    assert uploaded[0]["correlation_id"] == "cor-1"
    assert uploaded[0]["customer_id"] == "cust-1"


@pytest.mark.asyncio
async def test_recording_retries_then_succeeds(captured_events, fast_sleep):
    fetch_mock = AsyncMock(
        side_effect=[None, None, "https://exotel.example/rec.mp3"]
    )

    with patch.object(recording, "_fetch_exotel_recording_url", new=fetch_mock):
        result = await recording.fetch_and_upload_recording(
            interaction_id="i-2",
            call_sid="c-2",
            exotel_account_id="acc-2",
            correlation_id="cor-2",
            customer_id="cust-2",
        )

    assert result == "recordings/i-2.mp3"
    assert fetch_mock.await_count == 3

    not_yet = [e for e in captured_events if e["event"] == "not_yet_available"]
    assert len(not_yet) == 2
    assert all(e["status"] == "retry" for e in not_yet)
    assert not_yet[0]["attempt"] == 1
    assert not_yet[1]["attempt"] == 2

    uploaded = [e for e in captured_events if e["event"] == "uploaded"]
    assert len(uploaded) == 1
    assert uploaded[0]["attempt"] == 3


@pytest.mark.asyncio
async def test_recording_permanent_failure_after_all_attempts(
    captured_events, fast_sleep
):
    with patch.object(
        recording, "_fetch_exotel_recording_url",
        new=AsyncMock(return_value=None),
    ):
        result = await recording.fetch_and_upload_recording(
            interaction_id="i-3",
            call_sid="c-3",
            exotel_account_id="acc-3",
            correlation_id="cor-3",
            customer_id="cust-3",
        )

    assert result is None

    not_yet = [e for e in captured_events if e["event"] == "not_yet_available"]
    assert len(not_yet) == len(recording.POLL_DELAYS_SECONDS)

    permanent = [e for e in captured_events if e["event"] == "permanently_unavailable"]
    assert len(permanent) == 1
    assert permanent[0]["status"] == "fail"
    assert permanent[0]["attempts"] == len(recording.POLL_DELAYS_SECONDS)
    assert permanent[0]["total_wait_seconds"] == sum(recording.POLL_DELAYS_SECONDS)


@pytest.mark.asyncio
async def test_recording_handles_fetch_exception_and_continues(
    captured_events, fast_sleep
):
    fetch_mock = AsyncMock(
        side_effect=[Exception("network blip"), "https://exotel.example/rec.mp3"]
    )

    with patch.object(recording, "_fetch_exotel_recording_url", new=fetch_mock):
        result = await recording.fetch_and_upload_recording(
            interaction_id="i-4",
            call_sid="c-4",
            exotel_account_id="acc-4",
        )

    assert result == "recordings/i-4.mp3"

    fetch_errors = [e for e in captured_events if e["event"] == "fetch_error"]
    assert len(fetch_errors) == 1
    assert fetch_errors[0]["status"] == "retry"
    assert "network blip" in fetch_errors[0]["error"]

    uploaded = [e for e in captured_events if e["event"] == "uploaded"]
    assert len(uploaded) == 1
    assert uploaded[0]["attempt"] == 2


@pytest.mark.asyncio
async def test_recording_upload_failure_emits_fail_event_and_returns_none(
    captured_events, fast_sleep
):
    with patch.object(
        recording, "_fetch_exotel_recording_url",
        new=AsyncMock(return_value="https://exotel.example/rec.mp3"),
    ), patch.object(
        recording, "_upload_to_s3",
        new=AsyncMock(side_effect=Exception("S3 down")),
    ):
        result = await recording.fetch_and_upload_recording(
            interaction_id="i-5",
            call_sid="c-5",
            exotel_account_id="acc-5",
        )

    assert result is None

    upload_failures = [e for e in captured_events if e["event"] == "upload_failed"]
    assert len(upload_failures) == 1
    assert upload_failures[0]["status"] == "fail"
    assert "S3 down" in upload_failures[0]["error"]

    uploaded = [e for e in captured_events if e["event"] == "uploaded"]
    assert len(uploaded) == 0
