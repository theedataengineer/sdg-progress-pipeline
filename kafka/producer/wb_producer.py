"""
World Bank SDG Kafka Producer
================================
Polls the World Bank API for SDG indicator updates and
publishes each record as a message to the appropriate Kafka topic.

Run modes:
  python wb_producer.py stream   — continuous polling every 6 hours
  python wb_producer.py once     — single poll then exit (used by Airflow)
  python wb_producer.py test     — send 5 test messages then exit
"""

import json
import logging
import os
import sys
import time
import hashlib
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer, KafkaException
from dotenv import load_dotenv

# Add project root to path so we can import ingestion module
sys.path.append(str(Path(__file__).parent.parent.parent))
from ingestion.extract_world_bank import extract_recent_updates, SDG_INDICATORS

load_dotenv()

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Kafka configuration ───────────────────────────────────────────────────────
KAFKA_CONFIG = {
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
    "client.id":         "sdg-wb-producer",

    # Reliability settings
    "acks":              "all",       # wait for all replicas to confirm
    "retries":           3,
    "retry.backoff.ms":  500,

    # Performance settings
    "batch.size":        16384,       # batch up to 16KB before sending
    "linger.ms":         10,          # wait 10ms to fill batches
    "compression.type":  "snappy",    # compress messages
}

# ── Topic routing ─────────────────────────────────────────────────────────────
TOPIC_ALL        = "sdg-indicators-raw"       # every record
TOPIC_WORLD_BANK = "sdg-world-bank-raw"       # world bank specific
TOPIC_AFRICA     = "sdg-africa-indicators"    # africa filtered
TOPIC_VALIDATED  = "sdg-validated-records"    # passed validation
TOPIC_FAILED     = "sdg-failed-records"       # dead letter queue

# African country codes for filtering
AFRICAN_ISO3 = {
    "DZA","AGO","BEN","BWA","BFA","BDI","CPV","CMR","CAF","TCD",
    "COM","COD","COG","CIV","DJI","EGY","GNQ","ERI","SWZ","ETH",
    "GAB","GMB","GHA","GIN","GNB","KEN","LSO","LBR","LBY","MDG",
    "MWI","MLI","MRT","MUS","MAR","MOZ","NAM","NER","NGA","RWA",
    "STP","SEN","SLE","SOM","ZAF","SSD","SDN","TZA","TGO","TUN",
    "UGA","ZMB","ZWE","SHN"
}


def make_record_id(record: dict) -> str:
    """
    Generate a deterministic unique ID for each record.
    Same country + indicator + year always produces the same ID.
    This lets the consumer deduplicate streaming vs batch data.
    """
    key = f"{record['country_code']}_{record['indicator_code']}_{record['year']}"
    return hashlib.md5(key.encode()).hexdigest()


def validate_record(record: dict) -> tuple[bool, str]:
    """
    Validate a record before publishing to Kafka.
    Failed records go to the dead letter topic.

    Returns: (is_valid, reason_if_invalid)
    """
    required_fields = [
        "goal", "indicator_code", "country_code",
        "country_name", "year", "value"
    ]

    for field in required_fields:
        if field not in record or record[field] is None:
            return False, f"Missing required field: {field}"

    if not (1 <= record["goal"] <= 17):
        return False, f"Invalid goal number: {record['goal']}"

    if not (2000 <= record["year"] <= 2030):
        return False, f"Invalid year: {record['year']}"

    if record["value"] < 0 and record["indicator_code"] not in [
        "NY.GDP.MKTP.KD.ZG"   # GDP growth can be negative
    ]:
        return False, f"Unexpected negative value: {record['value']}"

    if len(record["country_code"]) != 3:
        return False, f"Invalid country code: {record['country_code']}"

    return True, ""


def delivery_callback(err, msg):
    """
    Called by Kafka after each message is delivered (or fails).
    Logs success or failure for monitoring.
    """
    if err:
        log.error(f"Message delivery failed: {err} | "
                  f"Topic: {msg.topic()} | "
                  f"Key: {msg.key()}")
    else:
        log.debug(f"Delivered to {msg.topic()} "
                  f"[partition {msg.partition()}] "
                  f"offset {msg.offset()}")


def publish_record(producer: Producer, record: dict) -> dict:
    """
    Publish a single SDG record to the appropriate Kafka topics.

    Each record is published to:
    - sdg-indicators-raw    (always — all records)
    - sdg-world-bank-raw    (always — source-specific topic)
    - sdg-africa-indicators (only if African country)
    - sdg-validated-records (if passes validation)
    - sdg-failed-records    (if fails validation)

    The message key is the country_code — this ensures all records
    for the same country go to the same partition (ordering guarantee).

    Returns: dict with publish results
    """
    # Add unique record ID
    record["record_id"] = make_record_id(record)
    record["published_at"] = datetime.now(timezone.utc).isoformat() + "Z"

    message = json.dumps(record).encode("utf-8")
    key = record["country_code"].encode("utf-8")

    results = {"record_id": record["record_id"], "topics": []}

    # Validate the record
    is_valid, reason = validate_record(record)

    if not is_valid:
        # Send to dead letter queue
        failed_record = {**record, "failure_reason": reason}
        producer.produce(
            topic=TOPIC_FAILED,
            key=key,
            value=json.dumps(failed_record).encode("utf-8"),
            callback=delivery_callback
        )
        results["topics"].append(TOPIC_FAILED)
        results["valid"] = False
        results["failure_reason"] = reason
        log.warning(f"Invalid record → dead letter: {reason} | "
                    f"{record.get('indicator_code')} / {record.get('country_code')}")
        return results

    # Publish to main topics
    producer.produce(
        topic=TOPIC_ALL,
        key=key,
        value=message,
        callback=delivery_callback
    )
    results["topics"].append(TOPIC_ALL)

    producer.produce(
        topic=TOPIC_WORLD_BANK,
        key=key,
        value=message,
        callback=delivery_callback
    )
    results["topics"].append(TOPIC_WORLD_BANK)

    # Africa-specific topic
    if record.get("country_code") in AFRICAN_ISO3:
        producer.produce(
            topic=TOPIC_AFRICA,
            key=key,
            value=message,
            callback=delivery_callback
        )
        results["topics"].append(TOPIC_AFRICA)

    # Validated records topic
    producer.produce(
        topic=TOPIC_VALIDATED,
        key=key,
        value=message,
        callback=delivery_callback
    )
    results["topics"].append(TOPIC_VALIDATED)

    results["valid"] = True
    return results


def run_producer_once() -> dict:
    """
    Single production run — fetch recent updates and publish all to Kafka.
    Called by Airflow every 6 hours.
    """
    log.info("Starting SDG Kafka producer run...")
    producer = Producer(KAFKA_CONFIG)

    stats = {
        "started_at":   datetime.now(timezone.utc).isoformat() + "Z",
        "records_published": 0,
        "records_failed":    0,
        "topics_written":    set()
    }

    try:
        # Fetch recent SDG updates from World Bank
        records = extract_recent_updates(days_back=7)
        log.info(f"Fetched {len(records)} records from World Bank API")

        for record in records:
            result = publish_record(producer, record)

            if result["valid"]:
                stats["records_published"] += 1
                stats["topics_written"].update(result["topics"])
            else:
                stats["records_failed"] += 1

            # Flush every 100 messages to avoid memory buildup
            if (stats["records_published"] + stats["records_failed"]) % 100 == 0:
                producer.flush(timeout=10)

        # Final flush — wait for all messages to be delivered
        log.info("Flushing remaining messages...")
        producer.flush(timeout=30)

        stats["finished_at"] = datetime.now(timezone.utc).isoformat() + "Z"
        stats["topics_written"] = list(stats["topics_written"])

        log.info(f"Producer run complete: "
                 f"{stats['records_published']} published, "
                 f"{stats['records_failed']} failed")

        return stats

    except KafkaException as e:
        log.error(f"Kafka error: {e}")
        raise
    finally:
        producer.flush(timeout=10)


def run_streaming(poll_interval_hours: int = 6):
    """
    Continuous streaming mode — polls every N hours indefinitely.
    Used when running the producer as a long-running service.
    """
    log.info(f"Starting streaming mode — polling every {poll_interval_hours} hours")

    while True:
        try:
            stats = run_producer_once()
            log.info(f"Next poll in {poll_interval_hours} hours...")
            time.sleep(poll_interval_hours * 3600)

        except KeyboardInterrupt:
            log.info("Streaming stopped by user")
            break
        except Exception as e:
            log.error(f"Producer error: {e} — retrying in 5 minutes")
            time.sleep(300)


def run_test():
    """
    Test mode — publish 5 sample messages and verify delivery.
    Used to confirm Kafka connectivity before full runs.
    """
    log.info("TEST MODE — publishing 5 sample messages")
    producer = Producer(KAFKA_CONFIG)

    test_records = [
        {
            "goal": 1,
            "indicator_code": "SI.POV.DDAY",
            "indicator_name": "Poverty headcount ratio $2.15/day (%)",
            "country_code": "KEN",
            "country_name": "Kenya",
            "year": 2022,
            "value": 46.9,
            "unit": "",
            "obs_status": "",
            "source": "World Bank Open Data",
            "extracted_at": datetime.now(timezone.utc).isoformat() + "Z"
        },
        {
            "goal": 3,
            "indicator_code": "SH.DYN.MORT",
            "indicator_name": "Under-5 mortality rate per 1000",
            "country_code": "NGA",
            "country_name": "Nigeria",
            "year": 2022,
            "value": 111.4,
            "unit": "per 1000",
            "obs_status": "",
            "source": "World Bank Open Data",
            "extracted_at": datetime.now(timezone.utc).isoformat() + "Z"
        },
        {
            "goal": 4,
            "indicator_code": "SE.PRM.ENRR",
            "indicator_name": "Primary school enrollment gross (%)",
            "country_code": "ETH",
            "country_name": "Ethiopia",
            "year": 2022,
            "value": 103.2,
            "unit": "%",
            "obs_status": "",
            "source": "World Bank Open Data",
            "extracted_at": datetime.now(timezone.utc).isoformat() + "Z"
        },
        {
            "goal": 5,
            "indicator_code": "SG.GEN.PARL.ZS",
            "indicator_name": "Women in parliament (%)",
            "country_code": "RWA",
            "country_name": "Rwanda",
            "year": 2023,
            "value": 61.3,
            "unit": "%",
            "obs_status": "",
            "source": "World Bank Open Data",
            "extracted_at": datetime.now(timezone.utc).isoformat() + "Z"
        },
        {
            "goal": 13,
            "indicator_code": "EN.ATM.CO2E.PC",
            "indicator_name": "CO2 emissions per capita (tonnes)",
            "country_code": "ZAF",
            "country_name": "South Africa",
            "year": 2021,
            "value": 6.95,
            "unit": "tonnes",
            "obs_status": "",
            "source": "World Bank Open Data",
            "extracted_at": datetime.now(timezone.utc).isoformat() + "Z"
        }
    ]

    published = 0
    for record in test_records:
        result = publish_record(producer, record)
        if result["valid"]:
            published += 1
            log.info(f"✓ Published: {record['country_name']} | "
                     f"SDG {record['goal']} | "
                     f"{record['indicator_code']} | "
                     f"{record['year']}: {record['value']}")
        else:
            log.warning(f"✗ Failed: {result['failure_reason']}")

    producer.flush(timeout=15)
    log.info(f"\nTest complete: {published}/5 messages published successfully")
    log.info("Check http://localhost:8090 to see messages in Kafka UI")


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "test"

    if mode == "stream":
        run_streaming(poll_interval_hours=6)
    elif mode == "once":
        stats = run_producer_once()
        print(json.dumps(stats, indent=2, default=str))
    elif mode == "test":
        run_test()
    else:
        print(f"Unknown mode: {mode}. Use: test | once | stream")
        sys.exit(1)
