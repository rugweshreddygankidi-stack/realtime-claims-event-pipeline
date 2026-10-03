from datetime import date, datetime, timedelta, timezone

import pytest

from claims.rules import MAX_CLAIM_AMOUNT, RULE_CODES, validate_claim

NOW = datetime(2026, 3, 10, 15, 0, 0, tzinfo=timezone.utc)


def good(**overrides):
    record = {
        "claim_event_id": "evt-1", "claim_id": "CLM1", "member_id": "MBR1", "provider_id": "PRV1",
        "claim_status": "APPROVED", "claim_amount": 125.50, "diagnosis_code": "E11.9",
        "service_date": date(2026, 3, 1), "event_ts": NOW,
    }
    record.update(overrides)
    return record


def test_valid_record_passes():
    assert validate_claim(good(), NOW) == []


@pytest.mark.parametrize("field", ["claim_event_id", "claim_id", "member_id", "provider_id"])
@pytest.mark.parametrize("bad", [None, "", "   ", 42])
def test_blank_identifier_fields(field, bad):
    assert f"missing_{field}" in validate_claim(good(**{field: bad}), NOW)


@pytest.mark.parametrize("status", ["approved", "UNKNOWN", None, ""])
def test_invalid_status(status):
    assert validate_claim(good(claim_status=status), NOW) == ["invalid_claim_status"]


@pytest.mark.parametrize("status", ["SUBMITTED", "APPROVED", "DENIED", "PENDING_REVIEW"])
def test_all_valid_statuses(status):
    assert validate_claim(good(claim_status=status), NOW) == []


@pytest.mark.parametrize("amount", [0, -5, -0.01, float("nan"), MAX_CLAIM_AMOUNT + 1, None, "10", True])
def test_invalid_amounts(amount):
    assert validate_claim(good(claim_amount=amount), NOW) == ["invalid_claim_amount"]


@pytest.mark.parametrize("amount", [0.01, 1, 99999.99, MAX_CLAIM_AMOUNT])
def test_valid_amount_boundaries(amount):
    assert validate_claim(good(claim_amount=amount), NOW) == []


@pytest.mark.parametrize("code", ["E11.9", "I10", "J45.909", "S72.001A", "Z00.00", "A00"])
def test_valid_diagnosis_codes(code):
    assert validate_claim(good(diagnosis_code=code), NOW) == []


@pytest.mark.parametrize("code", ["", "e11.9", "11.9", "E1", "E11.", "E11.12345", "not-a-code", None, "E11.9 "])
def test_invalid_diagnosis_codes(code):
    assert validate_claim(good(diagnosis_code=code), NOW) == ["invalid_diagnosis_code"]


def test_service_date_today_is_allowed():
    assert validate_claim(good(service_date=NOW.date()), NOW) == []


def test_service_date_tomorrow_rejected():
    assert validate_claim(good(service_date=NOW.date() + timedelta(days=1)), NOW) == ["service_date_in_future"]


def test_service_date_must_be_a_date_not_datetime():
    assert validate_claim(good(service_date=NOW), NOW) == ["service_date_in_future"]


def test_event_ts_within_skew_allowed():
    assert validate_claim(good(event_ts=NOW + timedelta(minutes=4)), NOW) == []


def test_event_ts_beyond_skew_rejected():
    assert validate_claim(good(event_ts=NOW + timedelta(minutes=10)), NOW) == ["event_ts_in_future"]


def test_missing_event_ts_rejected():
    assert validate_claim(good(event_ts=None), NOW) == ["event_ts_in_future"]


def test_multiple_errors_reported_in_rule_order():
    errors = validate_claim(good(claim_id="", claim_amount=-1, diagnosis_code="x"), NOW)
    assert errors == ["missing_claim_id", "invalid_claim_amount", "invalid_diagnosis_code"]
    assert errors == [c for c in RULE_CODES if c in errors]


def test_empty_record_triggers_every_rule():
    assert validate_claim({}, NOW) == list(RULE_CODES)
