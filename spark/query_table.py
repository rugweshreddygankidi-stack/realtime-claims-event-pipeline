"""Quick health report for the Iceberg table: row counts, duplicates, latency, snapshots."""
import os

from pyspark.sql import SparkSession

WAREHOUSE = os.environ.get("WAREHOUSE", "/warehouse")

spark = (SparkSession.builder.appName("claims-report")
         .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
         .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
         .config("spark.sql.catalog.local.type", "hadoop")
         .config("spark.sql.catalog.local.warehouse", WAREHOUSE)
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

print("\n== Row counts ==")
spark.sql("""SELECT count(*) AS rows, count(DISTINCT claim_event_id) AS distinct_events
             FROM local.claims.claim_events""").show()

print("== Claims by status ==")
spark.sql("""SELECT claim_status, count(*) AS claims, round(sum(claim_amount), 2) AS total_amount
             FROM local.claims.claim_events GROUP BY claim_status ORDER BY claims DESC""").show()

print("== Event-to-table latency (seconds) ==")
spark.sql("""SELECT percentile_approx(unix_timestamp(ingested_at) - unix_timestamp(event_ts), 0.5) AS p50,
                    percentile_approx(unix_timestamp(ingested_at) - unix_timestamp(event_ts), 0.95) AS p95
             FROM local.claims.claim_events""").show()

print("== Iceberg snapshots (one per micro-batch commit) ==")
spark.sql("SELECT count(*) AS snapshots FROM local.claims.claim_events.snapshots").show()
