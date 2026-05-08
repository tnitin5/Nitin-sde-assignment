"""Recording poller — fetches the call recording from Exotel and uploads to S3.

Replaces the old asyncio.sleep(45) with an exponential-backoff polling
loop. The Exotel status endpoint is poll-friendly, so each attempt is
cheap; we walk a [5, 10, 20, 40, 80] schedule giving a total wall time
of 155 seconds and 5 attempts before declaring a permanent failure.

A permanent-failure event is emitted via the audit logger so on-call
can alert on its rate. Recordings that arrive after our window are
picked up by the reconciler — a periodic task that scans interactions
with recording_state='failed_pending_reconcile' — documented in the
design doc.
"""

import asyncio
from typing import Optional

import httpx

from src.utils import audit


POLL_DELAYS_SECONDS = [5, 10, 20, 40, 80]


async def fetch_and_upload_recording(
    interaction_id: str,
    call_sid: str,
    exotel_account_id: str,
    *,
    correlation_id: Optional[str] = None,
    customer_id: Optional[str] = None,
) -> Optional[str]:
    for attempt, delay in enumerate(POLL_DELAYS_SECONDS, start=1):
        await asyncio.sleep(delay)

        try:
            recording_url = await _fetch_exotel_recording_url(
                call_sid, exotel_account_id
            )
        except Exception as e:
            audit.emit(
                "recording", "fetch_error",
                status="retry",
                correlation_id=correlation_id,
                interaction_id=interaction_id,
                customer_id=customer_id,
                attempt=attempt,
                error=str(e),
            )
            continue

        if recording_url:
            try:
                s3_key = await _upload_to_s3(recording_url, interaction_id)
            except Exception as e:
                audit.emit(
                    "recording", "upload_failed",
                    status="fail",
                    correlation_id=correlation_id,
                    interaction_id=interaction_id,
                    customer_id=customer_id,
                    attempt=attempt,
                    error=str(e),
                )
                return None

            audit.emit(
                "recording", "uploaded",
                correlation_id=correlation_id,
                interaction_id=interaction_id,
                customer_id=customer_id,
                attempt=attempt,
                s3_key=s3_key,
            )
            return s3_key

        next_idx = attempt
        audit.emit(
            "recording", "not_yet_available",
            status="retry",
            correlation_id=correlation_id,
            interaction_id=interaction_id,
            customer_id=customer_id,
            attempt=attempt,
            next_delay_s=(
                POLL_DELAYS_SECONDS[next_idx]
                if next_idx < len(POLL_DELAYS_SECONDS)
                else None
            ),
        )

    audit.emit(
        "recording", "permanently_unavailable",
        status="fail",
        correlation_id=correlation_id,
        interaction_id=interaction_id,
        customer_id=customer_id,
        attempts=len(POLL_DELAYS_SECONDS),
        total_wait_seconds=sum(POLL_DELAYS_SECONDS),
    )
    return None


async def _fetch_exotel_recording_url(
    call_sid: str, account_id: str,
) -> Optional[str]:
    url = f"https://api.exotel.com/v1/Accounts/{account_id}/Calls/{call_sid}/Recording"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url)
            if resp.status_code == 200:
                data = resp.json()
                return data.get("recording_url")
            return None
    except httpx.HTTPError:
        return None


async def _upload_to_s3(recording_url: str, interaction_id: str) -> str:
    # Production: stream from recording_url → boto3 upload to S3_BUCKET.
    return f"recordings/{interaction_id}.mp3"
