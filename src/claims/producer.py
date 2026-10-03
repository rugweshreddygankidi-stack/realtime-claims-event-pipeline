"""Produce Avro claim events (plus a configurable share of bad payloads) to Kafka.

    python -m claims.producer --events 200000 --rate 5000 --bad-ratio 0.02
"""
from __future__ import annotations

import argparse
import json
import os
import random
import time

from .generator import BAD_KINDS, make_business_invalid_claim, make_claim, make_raw_bad_payload
from .schema_registry import DEFAULT_URL, SUBJECT, get_latest

TOPIC = os.environ.get("CLAIMS_TOPIC", "claims.events")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_HOST", "localhost:9092")
BUSINESS_KINDS = ("negative_amount", "bad_diagnosis", "future_service_date")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=int, default=100_000)
    parser.add_argument("--rate", type=int, default=5000, help="target events per second")
    parser.add_argument("--bad-ratio", type=float, default=0.02)
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()

    from confluent_kafka import Producer
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroSerializer
    from confluent_kafka.serialization import MessageField, SerializationContext

    schema_str = get_latest(DEFAULT_URL, SUBJECT)["schema"]
    serializer = AvroSerializer(SchemaRegistryClient({"url": DEFAULT_URL}), schema_str,
                                lambda record, ctx: record, conf={"auto.register.schemas": False})
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 20, "batch.size": 262144,
                         "compression.type": "lz4", "queue.buffering.max.messages": 500_000})
    ctx = SerializationContext(TOPIC, MessageField.VALUE)
    rng = random.Random(args.seed)
    errors = []

    def on_delivery(err, _msg):
        if err is not None:
            errors.append(str(err))

    started = time.monotonic()
    for i in range(1, args.events + 1):
        if rng.random() < args.bad_ratio:
            kind = rng.choice(BAD_KINDS)
            if kind in BUSINESS_KINDS:
                value = serializer(make_business_invalid_claim(rng, kind), ctx)
            else:
                value = make_raw_bad_payload(rng, kind)
            key = b"bad"
        else:
            record = make_claim(rng)
            value, key = serializer(record, ctx), record["member_id"].encode()
        producer.produce(TOPIC, key=key, value=value, on_delivery=on_delivery)
        producer.poll(0)
        if i % 500 == 0:  # pace to the requested rate
            ahead = i / args.rate - (time.monotonic() - started)
            if ahead > 0:
                time.sleep(ahead)

    producer.flush()
    elapsed = time.monotonic() - started
    print(json.dumps({"produced": args.events - len(errors), "delivery_errors": len(errors),
                      "seconds": round(elapsed, 1), "events_per_sec": round(args.events / elapsed, 1)}))


if __name__ == "__main__":
    main()
