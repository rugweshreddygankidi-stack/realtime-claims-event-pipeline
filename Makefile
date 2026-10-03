# Convenience wrappers. Every target is a plain command you can also copy-paste.
PYTHON ?= python3
export PYTHONPATH := src

.PHONY: up topics schema stream produce report metrics evolve-ok evolve-bad dlq-peek test down clean

up:            ## start Kafka + Schema Registry
	docker compose up -d kafka schema-registry

topics:        ## create the main topic and the dead-letter topic
	docker compose exec kafka kafka-topics --create --if-not-exists --topic claims.events --partitions 6 --replication-factor 1 --bootstrap-server kafka:29092
	docker compose exec kafka kafka-topics --create --if-not-exists --topic claims.events.dlq --partitions 1 --replication-factor 1 --bootstrap-server kafka:29092

schema:        ## register the data contract (sets BACKWARD compatibility)
	$(PYTHON) -m claims.schema_registry register schemas/claim_event.avsc

stream:        ## run the Spark Structured Streaming job (Ctrl+C to stop)
	docker compose run --rm spark

produce:       ## send 200k events at ~5k/s, 2% deliberately bad
	$(PYTHON) -m claims.producer --events 200000 --rate 5000 --bad-ratio 0.02

report:        ## row counts, duplicates and latency from the Iceberg table
	docker compose run --rm --no-deps spark /opt/spark/bin/spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2 --conf spark.jars.ivy=/tmp/.ivy2 /app/spark/query_table.py

metrics:       ## throughput numbers from the streaming job's progress log
	$(PYTHON) -m claims.metrics metrics/progress.jsonl

evolve-ok:     ## a backward-compatible change is accepted
	$(PYTHON) -m claims.schema_registry check schemas/claim_event_v2_compatible.avsc

evolve-bad:    ## a breaking change is rejected
	$(PYTHON) -m claims.schema_registry check schemas/claim_event_v2_breaking.avsc

dlq-peek:      ## look at a few dead-lettered records
	docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic claims.events.dlq --from-beginning --max-messages 5

test:
	$(PYTHON) -m pytest -q

down:
	docker compose down

clean:         ## also delete the Iceberg warehouse, checkpoints and metrics
	docker compose down -v
	rm -rf warehouse checkpoints metrics/progress.jsonl
