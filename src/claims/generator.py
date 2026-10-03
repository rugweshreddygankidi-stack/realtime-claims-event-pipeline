"""Generate realistic and deliberately-bad claim events for the demo producer."""
from __future__ import annotations

import random
import struct
import uuid
from datetime import date, datetime, timedelta, timezone

from .rules import CLAIM_STATUSES

DIAGNOSIS_CODES = ("E11.9", "I10", "J45.909", "M54.5", "K21.9", "F41.1", "N39.0", "S72.001A", "Z00.00")
BAD_KINDS = ("negative_amount", "bad_diagnosis", "future_service_date", "garbage_bytes", "json_text", "unknown_schema_id")


def make_claim(rng: random.Random, now: datetime | None = None) -> dict:
    now = now or datetime.now(timezone.utc)
    return {
        "claim_event_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
        "claim_id": f"CLM{rng.randint(1, 5_000_000):08d}",
        "member_id": f"MBR{rng.randint(1, 500_000):07d}",
        "provider_id": f"PRV{rng.randint(1, 20_000):06d}",
        "claim_status": rng.choices(CLAIM_STATUSES, weights=(40, 35, 10, 15))[0],
        "claim_amount": round(rng.lognormvariate(5.0, 1.0), 2) or 1.0,
        "diagnosis_code": rng.choice(DIAGNOSIS_CODES),
        "service_date": (now - timedelta(days=rng.randint(0, 30))).date(),
        "event_ts": now.replace(microsecond=(now.microsecond // 1000) * 1000),
    }


def make_business_invalid_claim(rng: random.Random, kind: str, now: datetime | None = None) -> dict:
    """A record that is valid Avro but violates a business rule."""
    now = now or datetime.now(timezone.utc)
    record = make_claim(rng, now)
    if kind == "negative_amount":
        record["claim_amount"] = -abs(record["claim_amount"])
    elif kind == "bad_diagnosis":
        record["diagnosis_code"] = "not-a-code"
    elif kind == "future_service_date":
        record["service_date"] = date.fromordinal(now.date().toordinal() + 30)
    else:
        raise ValueError(f"not a business-rule kind: {kind}")
    return record


def make_raw_bad_payload(rng: random.Random, kind: str) -> bytes:
    """A payload that is NOT valid Confluent-framed Avro for our schema."""
    if kind == "garbage_bytes":
        return bytes([rng.randint(1, 255)]) + rng.randbytes(40)  # first byte != 0 -> bad magic byte
    if kind == "json_text":
        return b'{"claim_id": "CLM1", "claim_amount": 12.5}'
    if kind == "unknown_schema_id":
        return b"\x00" + struct.pack(">I", 987_654) + rng.randbytes(30)
    raise ValueError(f"not a raw-payload kind: {kind}")
