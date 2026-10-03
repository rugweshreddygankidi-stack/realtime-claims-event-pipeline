"""Business rules for claim events (pure Python).

The Spark job applies the same rules in claims/spark_rules.py; tests/test_spark_parity.py
checks that both implementations agree.
"""
from __future__ import annotations

import math
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any

CLAIM_STATUSES = ("SUBMITTED", "APPROVED", "DENIED", "PENDING_REVIEW")
MAX_CLAIM_AMOUNT = 1_000_000.0
MAX_FUTURE_SKEW_SECONDS = 300
# ICD-10-like: a letter, two digits, optional dot + 1-4 alphanumerics (e.g. E11.9, I10, S72.001A)
DIAGNOSIS_REGEX = r"^[A-Z][0-9]{2}(\.[0-9A-Z]{1,4})?$"
_DIAGNOSIS = re.compile(DIAGNOSIS_REGEX)

# Order matters: Spark and Python return codes in this order.
RULE_CODES = (
    "missing_claim_event_id",
    "missing_claim_id",
    "missing_member_id",
    "missing_provider_id",
    "invalid_claim_status",
    "invalid_claim_amount",
    "invalid_diagnosis_code",
    "service_date_in_future",
    "event_ts_in_future",
)


def _blank(value: Any) -> bool:
    return not isinstance(value, str) or not value.strip()


def validate_claim(record: dict, now: datetime | None = None) -> list[str]:
    """Return the list of violated rule codes (empty list means the record is valid)."""
    now = now or datetime.now(timezone.utc)
    errors: list[str] = []

    for field in ("claim_event_id", "claim_id", "member_id", "provider_id"):
        if _blank(record.get(field)):
            errors.append(f"missing_{field}")

    if record.get("claim_status") not in CLAIM_STATUSES:
        errors.append("invalid_claim_status")

    amount = record.get("claim_amount")
    if (isinstance(amount, bool) or not isinstance(amount, (int, float))
            or math.isnan(amount) or not 0 < amount <= MAX_CLAIM_AMOUNT):
        errors.append("invalid_claim_amount")

    code = record.get("diagnosis_code")
    if not isinstance(code, str) or not _DIAGNOSIS.match(code):
        errors.append("invalid_diagnosis_code")

    service_date = record.get("service_date")
    if not isinstance(service_date, date) or isinstance(service_date, datetime) or service_date > now.date():
        errors.append("service_date_in_future")

    event_ts = record.get("event_ts")
    if not isinstance(event_ts, datetime) or event_ts > now + timedelta(seconds=MAX_FUTURE_SKEW_SECONDS):
        errors.append("event_ts_in_future")

    return errors
