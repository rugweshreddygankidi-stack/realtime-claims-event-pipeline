"""Spark column expressions that mirror claims/rules.py (same rule codes, same order)."""
from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

from .rules import CLAIM_STATUSES, DIAGNOSIS_REGEX, MAX_CLAIM_AMOUNT, MAX_FUTURE_SKEW_SECONDS


def _flag(is_valid: Column, code: str) -> Column:
    """Return the rule code when the check fails (null or false), otherwise null."""
    return F.when(~F.coalesce(is_valid, F.lit(False)), F.lit(code))


def rule_error_codes(payload: Column, now: Column) -> Column:
    """Array<string> of violated business-rule codes for a decoded claim struct.

    When `payload` itself is null (decode failure) every rule passes here, because the
    decode failure is reported separately by the job.
    """
    def check(valid: Column, code: str) -> Column:
        return _flag(payload.isNull() | valid, code)

    nonblank = lambda name: F.length(F.trim(payload[name])) > 0  # noqa: E731
    checks = [
        check(nonblank("claim_event_id"), "missing_claim_event_id"),
        check(nonblank("claim_id"), "missing_claim_id"),
        check(nonblank("member_id"), "missing_member_id"),
        check(nonblank("provider_id"), "missing_provider_id"),
        check(payload["claim_status"].isin(*CLAIM_STATUSES), "invalid_claim_status"),
        check((payload["claim_amount"] > 0) & (payload["claim_amount"] <= MAX_CLAIM_AMOUNT),
              "invalid_claim_amount"),
        check(payload["diagnosis_code"].rlike(DIAGNOSIS_REGEX), "invalid_diagnosis_code"),
        check(payload["service_date"] <= F.to_date(now), "service_date_in_future"),
        check(F.unix_timestamp(payload["event_ts"]) <= F.unix_timestamp(now) + F.lit(MAX_FUTURE_SKEW_SECONDS),
              "event_ts_in_future"),
    ]
    return F.filter(F.array(*checks), lambda x: x.isNotNull())
