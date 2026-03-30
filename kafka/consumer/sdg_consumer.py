"""
SDG Kafka Consumer
====================
Reads SDG indicator messages from Kafka topics, batches them,
and writes to MinIO (S3-compatible) as JSON files.

Flow:
  Kafka topics → Consumer → Batch (5 min) → MinIO → PostgreSQL

The consumer runs continuously. Every 5 minutes it flushes
whatever messages it has accumulated into a timestamped JSON
file in MinIO. Airflow then picks these up and loads them
into PostgreSQL raw tables.

Run modes:
  python sdg_consumer.py run    — continuous consumption
  python sdg_consumer.py once   — consume for 30 seconds then exit
  python sdg_consumer.py test   — verify connectivity only
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from collections import defaultdict

import boto3
from botocore.client import Config
from confluent_kafka import Consumer, KafkaError, KafkaException
from dotenv import load_dotenv

load_dotenv()

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Kafka configuration ───────────────────────────────────────────────────────
KAFKA_CONFIG = {
    "bootstrap.servers":  os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
    "group.id":           os.getenv("KAFKA_GROUP_ID", "sdg-consumer-group"),
    "auto.offset.reset":  "earliest",   # start from beginning if no offset stored
    "enable.auto.commit": False,        # we commit manually after successful write
    "max.poll.interval.ms": 300000,     # 5 minutes max between polls
    "session.timeout.ms":   30000,
}

# Topics to consume from
TOPICS = [
    "sdg-indicators-raw",
    "sdg-world-bank-raw",
    "sdg-africa-indicators",
    "sdg-validated-records",
    "sdg-failed-records",
]

# ── MinIO / S3 configuration ──────────────────────────────────────────────────
MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT",   "localhost:9000")
MINIO_ACCESS    = os.getenv("MINIO_ACCESS_KEY",  "minioadmin")
MINIO_SECRET    = os.getenv("MINIO_SECRET_KEY",  "minioadmin123")
MINIO_BUCKET    = os.getenv("MINIO_BUCKET",      "sdg-progress-pipeline")
BATCH_SECONDS   = int(os.getenv("BATCH_SECONDS", "300"))  # 5 minutes

# S3 path per topic
TOPIC_S3_PREFIX = {
    "sdg-indicators-raw":    "streaming/kafka-batches/all",
    "sdg-world-bank-raw":    "streaming/kafka-batches/world-bank",
    "sdg-africa-indicators": "streaming/kafka-batches/africa",
    "sdg-validated-records": "streaming/kafka-batches/validated",
    "sdg-failed-records":    "streaming/failed-records",
}


def get_minio_client():
    """
    Create and return a boto3 S3 client pointed at MinIO.
    This is identical to how you'd connect to AWS S3 —
    just with a custom endpoint_url pointing to localhost.
    Swapping to AWS S3 = changing endpoint_url + credentials.
    """
    return boto3.client(
        "s3",
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def write_batch_to_minio(s3_client, topic: str,
                          messages: list[dict]) -> str | None:
    """
    Write a batch of messages to MinIO as a single JSON file.

    File naming convention:
      streaming/kafka-batches/africa/2026/03/30/batch_20260330_121500.json

    This partitioned path structure means:
    - Easy to query by date in SQL (WHERE date = '2026-03-30')
    - Airflow can pick up files by date partition
    - Compatible with AWS S3 Glue crawlers when we swap to AWS

    Returns the S3 key of the written file, or None on failure.
    """
    if not messages:
        return None

    now = datetime.now(timezone.utc)
    timestamp = now.strftime("%Y%m%d_%H%M%S")
    date_path = now.strftime("%Y/%m/%d")

    prefix = TOPIC_S3_PREFIX.get(topic, "streaming/kafka-batches/unknown")
    s3_key = f"{prefix}/{date_path}/batch_{timestamp}.json"

    # Build the batch file content
    batch = {
        "batch_metadata": {
            "topic":        topic,
            "record_count": len(messages),
            "written_at":   now.isoformat(),
            "s3_key":       s3_key,
        },
        "records": messages
    }

    try:
        s3_client.put_object(
            Bucket=MINIO_BUCKET,
            Key=s3_key,
            Body=json.dumps(batch, indent=2).encode("utf-8"),
            ContentType="application/json",
        )
        log.info(f"✓ Wrote {len(messages)} records to s3://{MINIO_BUCKET}/{s3_key}")
        return s3_key

    except Exception as e:
        log.error(f"Failed to write to MinIO: {e}")
        return None


def flush_batches(s3_client, batches: dict) -> dict:
    """
    Write all accumulated message batches to MinIO.
    Called every BATCH_SECONDS (5 minutes) or on shutdown.

    Returns stats about what was written.
    """
    stats = {"files_written": 0, "records_written": 0, "failed_topics": []}

    for topic, messages in batches.items():
        if not messages:
            continue

        s3_key = write_batch_to_minio(s3_client, topic, messages)
        if s3_key:
            stats["files_written"] += 1
            stats["records_written"] += len(messages)
        else:
            stats["failed_topics"].append(topic)

    return stats


def run_consumer(duration_seconds: int | None = None):
    """
    Main consumer loop.

    Reads messages from all SDG Kafka topics, accumulates them
    into per-topic batches, and flushes to MinIO every 5 minutes.

    Args:
        duration_seconds: Run for this many seconds then exit.
                         None = run forever (production mode)
    """
    consumer  = Consumer(KAFKA_CONFIG)
    s3_client = get_minio_client()

    # Per-topic message batches
    batches: dict[str, list] = defaultdict(list)
    last_flush = time.time()
    messages_consumed = 0
    started_at = time.time()

    log.info(f"Starting consumer — subscribed to {len(TOPICS)} topics")
    log.info(f"Batch interval: {BATCH_SECONDS}s | "
             f"MinIO: {MINIO_ENDPOINT}/{MINIO_BUCKET}")

    try:
        consumer.subscribe(TOPICS)

        while True:
            # Check if we've hit the duration limit
            if duration_seconds and (time.time() - started_at) > duration_seconds:
                log.info(f"Duration limit reached ({duration_seconds}s) — stopping")
                break

            # Poll for messages (1 second timeout)
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # No message — check if it's time to flush
                pass

            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # Reached end of partition — normal, not an error
                    log.debug(f"End of partition: {msg.topic()} "
                              f"[{msg.partition()}]")
                else:
                    raise KafkaException(msg.error())

            else:
                # Successfully received a message
                try:
                    record = json.loads(msg.value().decode("utf-8"))
                    batches[msg.topic()].append(record)
                    messages_consumed += 1

                    if messages_consumed % 50 == 0:
                        log.info(f"Consumed {messages_consumed} messages total | "
                                 f"Batch sizes: "
                                 f"{ {t: len(m) for t, m in batches.items() if m} }")

                except json.JSONDecodeError as e:
                    log.error(f"Failed to decode message: {e}")

            # Flush to MinIO every BATCH_SECONDS
            if time.time() - last_flush >= BATCH_SECONDS:
                total_pending = sum(len(m) for m in batches.values())

                if total_pending > 0:
                    log.info(f"Flushing {total_pending} messages to MinIO...")
                    stats = flush_batches(s3_client, batches)
                    log.info(f"Flush complete: {stats['files_written']} files, "
                             f"{stats['records_written']} records")

                    # Clear batches after successful flush
                    batches = defaultdict(list)

                    # Commit offsets after successful write to MinIO
                    # This ensures we don't re-process messages
                    consumer.commit(asynchronous=False)

                last_flush = time.time()

    except KeyboardInterrupt:
        log.info("Consumer stopped by user")

    finally:
        # Flush any remaining messages before shutdown
        total_pending = sum(len(m) for m in batches.values())
        if total_pending > 0:
            log.info(f"Flushing {total_pending} remaining messages before shutdown...")
            flush_batches(s3_client, batches)
            consumer.commit(asynchronous=False)

        consumer.close()
        log.info(f"Consumer shut down. Total messages consumed: {messages_consumed}")


def test_connectivity():
    """
    Test Kafka and MinIO connectivity without consuming any messages.
    """
    log.info("Testing connectivity...")

    # Test Kafka
    try:
        from confluent_kafka.admin import AdminClient
        admin = AdminClient({"bootstrap.servers":
                             os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")})
        metadata = admin.list_topics(timeout=5)
        topics = [t for t in metadata.topics if not t.startswith("_")]
        log.info(f"✓ Kafka connected — {len(topics)} topics: {topics}")
    except Exception as e:
        log.error(f"✗ Kafka connection failed: {e}")
        return False

    # Test MinIO
    try:
        s3 = get_minio_client()
        response = s3.list_buckets()
        buckets = [b["Name"] for b in response["Buckets"]]
        log.info(f"✓ MinIO connected — buckets: {buckets}")

        # Check our bucket exists
        if MINIO_BUCKET in buckets:
            log.info(f"✓ Target bucket '{MINIO_BUCKET}' exists")
        else:
            log.warning(f"✗ Target bucket '{MINIO_BUCKET}' not found")
            return False

    except Exception as e:
        log.error(f"✗ MinIO connection failed: {e}")
        return False

    log.info("All connectivity checks passed!")
    return True


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "test"

    if mode == "run":
        # Production — run forever
        run_consumer(duration_seconds=None)

    elif mode == "once":
        # Consume for 30 seconds then exit — used by Airflow
        log.info("Running for 30 seconds...")
        run_consumer(duration_seconds=30)

    elif mode == "test":
        # Just test connectivity
        success = test_connectivity()
        sys.exit(0 if success else 1)

    else:
        print(f"Unknown mode: {mode}. Use: run | once | test")
        sys.exit(1)
