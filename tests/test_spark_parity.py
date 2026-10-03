"""Checks that the Spark rule expressions agree with the pure-Python rules.

Needs pyspark and Java; skipped automatically when pyspark is not installed.
"""
import random
from datetime import datetime, timezone

import pytest

pyspark = pytest.importorskip("pyspark")

from claims.generator import make_business_invalid_claim, make_claim  # noqa: E402
from claims.rules import validate_claim  # noqa: E402

NOW = datetime(2026, 3, 10, 15, 0, 0, tzinfo=timezone.utc)


def test_spark_and_python_rules_agree():
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    from claims.spark_rules import rule_error_codes

    spark = SparkSession.builder.master("local[1]").appName("parity").config("spark.ui.enabled", "false").getOrCreate()
    rng = random.Random(42)
    records = [make_claim(rng, NOW) for _ in range(30)]
    for kind in ("negative_amount", "bad_diagnosis", "future_service_date"):
        records += [make_business_invalid_claim(rng, kind, NOW) for _ in range(5)]
    records.append({**make_claim(rng, NOW), "member_id": "  "})
    records.append({**make_claim(rng, NOW), "claim_status": "BOGUS"})
    # keep event_ts away from the "now" boundary so second-level rounding cannot matter
    records.append({**make_claim(rng, NOW), "event_ts": datetime(2026, 3, 10, 16, 0, 0, tzinfo=timezone.utc)})

    rows = [(i, r["claim_event_id"], r["claim_id"], r["member_id"], r["provider_id"], r["claim_status"],
             float(r["claim_amount"]), r["diagnosis_code"], r["service_date"], r["event_ts"].replace(tzinfo=None))
            for i, r in enumerate(records)]
    cols = ["i", "claim_event_id", "claim_id", "member_id", "provider_id", "claim_status", "claim_amount",
            "diagnosis_code", "service_date", "event_ts"]
    df = spark.createDataFrame(rows, cols)
    payload = F.struct(*[F.col(c) for c in cols[1:]])
    now = F.lit(NOW.replace(tzinfo=None)).cast("timestamp")
    result = df.select("i", rule_error_codes(payload, now).alias("errors")).orderBy("i").collect()

    spark_errors = {row["i"]: list(row["errors"]) for row in result}
    for i, record in enumerate(records):
        assert spark_errors[i] == validate_claim(record, NOW), f"mismatch for record {i}"
    spark.stop()
