import json
import random
import struct
from datetime import datetime, timezone

import pytest

from claims.generator import BAD_KINDS, make_business_invalid_claim, make_claim, make_raw_bad_payload
from claims.rules import validate_claim

NOW = datetime(2026, 3, 10, 15, 0, 0, 123_456, tzinfo=timezone.utc)
BUSINESS = ("negative_amount", "bad_diagnosis", "future_service_date")
RAW = ("garbage_bytes", "json_text", "unknown_schema_id")


def test_generated_claims_are_valid():
    rng = random.Random(1)
    for _ in range(1000):
        assert validate_claim(make_claim(rng, NOW), NOW) == []


def test_claim_ids_are_unique():
    rng = random.Random(2)
    ids = {make_claim(rng, NOW)["claim_event_id"] for _ in range(2000)}
    assert len(ids) == 2000


def test_generator_is_deterministic():
    assert make_claim(random.Random(9), NOW) == make_claim(random.Random(9), NOW)


def test_event_ts_is_millisecond_precision():
    assert make_claim(random.Random(3), NOW)["event_ts"].microsecond % 1000 == 0


@pytest.mark.parametrize("kind,expected", [
    ("negative_amount", "invalid_claim_amount"),
    ("bad_diagnosis", "invalid_diagnosis_code"),
    ("future_service_date", "service_date_in_future"),
])
def test_business_invalid_claims_trip_exactly_the_intended_rule(kind, expected):
    record = make_business_invalid_claim(random.Random(4), kind, NOW)
    assert validate_claim(record, NOW) == [expected]


def test_business_invalid_rejects_unknown_kind():
    with pytest.raises(ValueError):
        make_business_invalid_claim(random.Random(1), "garbage_bytes", NOW)


def test_garbage_bytes_never_start_with_magic_byte():
    rng = random.Random(5)
    for _ in range(200):
        assert make_raw_bad_payload(rng, "garbage_bytes")[0] != 0


def test_json_text_payload_is_json_not_avro():
    payload = make_raw_bad_payload(random.Random(1), "json_text")
    assert json.loads(payload)["claim_id"] == "CLM1"


def test_unknown_schema_id_has_valid_framing_but_unregistered_id():
    payload = make_raw_bad_payload(random.Random(1), "unknown_schema_id")
    assert payload[0] == 0
    assert struct.unpack(">I", payload[1:5])[0] == 987_654


def test_raw_payload_rejects_unknown_kind():
    with pytest.raises(ValueError):
        make_raw_bad_payload(random.Random(1), "negative_amount")


def test_bad_kinds_cover_both_families():
    assert set(BAD_KINDS) == set(BUSINESS) | set(RAW)
