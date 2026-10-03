# Estimated results

> **Estimated values.** Derived from service limits, the pipeline configuration and typical laptop performance; actual figures vary by machine and environment.

| Metric | Estimated | How the estimate was derived |
|---|---|---|
| Producer rate | ~5,000 events/s (target) | `--rate 5000`; confluent-kafka with Avro serialization normally exceeds this on a laptop, so the pacing loop is the limit |
| Spark sustained throughput | ~3,000-8,000 rows/s | Ceiling is 10,000 rows/s (`maxOffsetsPerTrigger` 50,000 / 5 s trigger); the Iceberg MERGE commit per micro-batch usually takes a few seconds locally |
| Event-to-table latency p50 / p95 | ~5-10 s / ~15-30 s | 5 s trigger plus decode, validate and MERGE time per batch |
| Dead-letter share | ~2% of produced | Producer default `--bad-ratio 0.02`; every bad kind is rejected in unit tests |
| Duplicate rows after a forced restart | 0 | Idempotent `MERGE ... WHEN NOT MATCHED` on `claim_event_id` |
| Schema evolution | compatible v2 accepted, breaking v2 rejected | Registry set to BACKWARD; v2 adds a field with a default, the breaking v2 changes a type |
| Unit tests | 86 test cases + 1 Spark parity test | Counted locally including parametrized cases |

## Experiments to record
1. **Replay safety:** stop the job mid-run, restart it, confirm `rows == distinct_events`.
2. **Schema evolution:** paste the output of `make evolve-ok` and `make evolve-bad`.
3. **Bad data:** run with `--bad-ratio 0.05` and confirm the DLQ holds about 5%.
