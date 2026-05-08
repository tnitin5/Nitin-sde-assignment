"""Structured audit logging for the post-call pipeline.

Every pipeline stage emits one event via emit(). Each event is rendered
as a single JSON line so on-call engineers can reconstruct an
interaction's timeline by grepping correlation_id or interaction_id.
"""

import json
import logging
import re
import time
import uuid
from typing import Any, Optional

_logger = logging.getLogger("postcall.audit")

# Redact phone numbers and emails from free-text before they reach logs.
_PHONE_RE = re.compile(r"\+?\d[\d \-()]{7,}\d")
_EMAIL_RE = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")


def new_correlation_id() -> str:
    return str(uuid.uuid4())


def redact(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    value = _PHONE_RE.sub("[PHONE]", value)
    value = _EMAIL_RE.sub("[EMAIL]", value)
    return value


def emit(
    stage: str,
    event: str,
    *,
    status: str = "ok",
    correlation_id: Optional[str] = None,
    interaction_id: Optional[str] = None,
    customer_id: Optional[str] = None,
    campaign_id: Optional[str] = None,
    **fields: Any,
) -> None:
    payload: dict[str, Any] = {
        "ts": time.time(),
        "stage": stage,
        "event": event,
        "status": status,
    }
    for key, value in (
        ("correlation_id", correlation_id),
        ("interaction_id", interaction_id),
        ("customer_id", customer_id),
        ("campaign_id", campaign_id),
    ):
        if value is not None:
            payload[key] = value
    payload.update(fields)

    if status == "fail":
        level = logging.ERROR
    elif status == "retry":
        level = logging.WARNING
    else:
        level = logging.INFO

    _logger.log(level, json.dumps(payload, default=str))
