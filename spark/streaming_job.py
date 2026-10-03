"""Kafka (Avro + Schema Registry) -> validate -> Apache Iceberg, bad records -> dead-letter topic.

Delivery semantics: Kafka offsets are checkpointed (at-least-once), and the Iceberg write is an
idempotent MERGE on claim_event_id, so replays after a failure do not create duplicates.
"""
import json
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.streaming import StreamingQueryListener

from claims import schema_registry as registry
from claims.spark_rules import rule_error_codes

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
SUBJECT = os.environ.get("SCHEMA_SUBJECT", "claims.events-value")
TOPIC = os.environ.get("CLAIMS_TOPIC", "claims.events")
DLQ_TOPIC = os.environ.get("DLQ_TOPIC", "claims.events.dlq")
WAREHOUSE = os.environ.get("WAREHOUSE", "/warehouse")
CHECKPOINT = os.environ.get("CHECKPOINT", "/checkpoints/claims")
METRICS_FILE = os.environ.get("METRICS_FILE", "/metrics/progress.jsonl")
STARTING_OFFSETS = os.environ.get("STARTING_OFFSETS", "earliest")
MAX_OFFSETS_PER_TRIGGER = os.environ.get("MAX_OFFSETS_PER_TRIGGER", "50000")
TRIGGER_SECONDS = os.environ.get("TRIGGER_SECONDS", "5")
DEDUPE_DAYS = int(os.environ.get("DEDUPE_DAYS", "3"))

TABLE = "local.claims.claim_events"
TABLE_COLUMNS = ["claim_event_id", "claim_id", "member_id", "provider_id", "claim_status", "claim_amount",
                 "diagnosis_code", "service_date", "event_ts", "kafka_partition", "kafka_offset", "ingested_at"]


class ProgressLogger(StreamingQueryListener):
    """Append one JSON line per micro-batch; scripts/summarize_metrics.py turns it into numbers."""

    def onQueryStarted(self, event):
        pass

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        pass

    def onQueryProgress(self, event):
        p = event.progress
        line = {"ts": time.time(), "batch_id": p.batchId, "num_input_rows": p.numInputRows,
                "input_rows_per_sec": p.inputRowsPerSecond, "processed_rows_per_sec": p.processedRowsPerSecond,
                "duration_ms": dict(p.durationMs)}
        os.makedirs(os.path.dirname(METRICS_FILE), exist_ok=True)
        with open(METRICS_FILE, "a") as fh:
            fh.write(json.dumps(line) + "\n")


def build_spark() -> SparkSession:
    return (SparkSession.builder.appName("claims-streaming")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", WAREHOUSE)
            .config("spark.sql.shuffle.partitions", "8")
            .getOrCreate())


def create_table(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.claims")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
          claim_event_id string, claim_id string, member_id string, provider_id string,
          claim_status string, claim_amount double, diagnosis_code string,
          service_date date, event_ts timestamp,
          kafka_partition int, kafka_offset bigint, ingested_at timestamp)
        USING iceberg
        PARTITIONED BY (days(event_ts))
    """)


def decode(raw, schema_str: str, expected_schema_id: int):
    """Split the Confluent wire format (magic byte, 4-byte schema id, Avro body) and decode it."""
    framed = raw.select(
        F.col("value").alias("raw_value"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
    )
    magic_ok = (F.length("raw_value") > 5) & (F.hex(F.expr("substring(raw_value, 1, 1)")) == F.lit("00"))
    schema_id = F.conv(F.hex(F.expr("substring(raw_value, 2, 4)")), 16, 10).cast("long")
    avro_body = F.expr("substring(raw_value, 6, length(raw_value) - 5)")

    framed = (framed
              .withColumn("magic_ok", magic_ok)
              .withColumn("schema_id", F.when(F.col("magic_ok"), schema_id))
              .withColumn("payload", F.when(F.col("magic_ok") & (F.col("schema_id") == expected_schema_id),
                                            from_avro(avro_body, schema_str, {"mode": "PERMISSIVE"}))))

    envelope_errors = [
        F.when(~F.col("magic_ok"), F.lit("bad_magic_byte")),
        F.when(F.col("magic_ok") & (F.col("schema_id") != expected_schema_id), F.lit("schema_id_mismatch")),
        F.when(F.col("magic_ok") & (F.col("schema_id") == expected_schema_id) & F.col("payload").isNull(),
               F.lit("avro_decode_failed")),
    ]
    all_errors = F.concat(F.array(*envelope_errors), rule_error_codes(F.col("payload"), F.current_timestamp()))
    return framed.withColumn("errors", F.filter(all_errors, lambda x: x.isNotNull()))


def make_batch_processor():
    def process_batch(batch_df, batch_id):
        spark = batch_df.sparkSession
        batch_df.persist()
        try:
            good = (batch_df.filter(F.size("errors") == 0)
                    .select("payload.*", "kafka_partition", "kafka_offset")
                    .withColumn("ingested_at", F.current_timestamp())
                    .select(*TABLE_COLUMNS)
                    .dropDuplicates(["claim_event_id"]))
            good.createOrReplaceTempView("incoming")
            insert_cols = ", ".join(TABLE_COLUMNS)
            insert_vals = ", ".join(f"s.{c}" for c in TABLE_COLUMNS)
            spark.sql(f"""
                MERGE INTO {TABLE} t
                USING incoming s
                ON t.claim_event_id = s.claim_event_id
                   AND t.event_ts >= date_sub(current_date(), {DEDUPE_DAYS})
                WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
            """)

            bad = batch_df.filter(F.size("errors") > 0)
            (bad.select(F.to_json(F.struct(
                    F.col("errors"),
                    F.base64("raw_value").alias("raw_value_b64"),
                    F.col("kafka_partition"), F.col("kafka_offset"),
                    F.lit(TOPIC).alias("source_topic"),
                    F.current_timestamp().alias("failed_at"))).alias("value"))
                .write.format("kafka")
                .option("kafka.bootstrap.servers", BOOTSTRAP)
                .option("topic", DLQ_TOPIC)
                .save())
        finally:
            batch_df.unpersist()
    return process_batch


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    spark.streams.addListener(ProgressLogger())
    create_table(spark)

    latest = registry.get_latest(REGISTRY_URL, SUBJECT)
    print(f"Using schema id={latest['id']} version={latest['version']} for subject {SUBJECT}")

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", BOOTSTRAP)
           .option("subscribe", TOPIC)
           .option("startingOffsets", STARTING_OFFSETS)
           .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
           .option("failOnDataLoss", "false")
           .load())

    query = (decode(raw, latest["schema"], latest["id"]).writeStream
             .foreachBatch(make_batch_processor())
             .option("checkpointLocation", CHECKPOINT)
             .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
             .start())
    query.awaitTermination()


if __name__ == "__main__":
    main()
