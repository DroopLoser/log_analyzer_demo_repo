import json
import logging
import os
import time
from datetime import datetime, timezone

import psycopg
from confluent_kafka import Consumer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "app-logs")
GROUP = os.getenv("KAFKA_GROUP", "log-processor")
DSN = os.getenv("PG_DSN", "postgresql://logs_user:logs_pass@localhost:5433/logs_db")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
POLL_TIMEOUT = 1.0  # seconds



logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("processor")

INSERT_SQL = """
INSERT INTO logs (service, level, message, ts, raw, kafka_partition, kafka_offset)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (kafka_partition, kafka_offset) DO NOTHING
"""


def parse(value: bytes, partition: int, offset: int):
    """Turn one Kafka message into a DB row. Returns None if the message is bad."""
    try:
        data = json.loads(value)
        ts_raw = data.get("timestamp") or data.get("ts")
        ts = (
            datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
            if ts_raw
            else datetime.now(timezone.utc)
        )
        return (
            data.get("service", "unknown"),
            str(data.get("level", "ERROR")).upper(),
            data.get("message", ""),
            ts,
            json.dumps(data),
            partition,
            offset,
        )
    except (ValueError, TypeError, AttributeError) as exc:
        log.warning("skipping bad message at partition %s offset %s: %s", partition, offset, exc)
        return None


def main():
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC])
    conn = psycopg.connect(DSN)
    print("consuming topic", TOPIC, flush=True)

    rows, last_msg = [], None
    last_flush = time.monotonic()
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is not None:
                if msg.error():
                    print("kafka error:", msg.error(), flush=True)
                else:
                    last_msg = msg
                    row = parse(msg.value(), msg.partition(), msg.offset())
                    if row:
                        rows.append(row)
                        print("received offset", msg.offset(), flush=True)

            # flush when the batch is full or 2 seconds have passed
            due = time.monotonic() - last_flush >= 2.0
            if last_msg is not None and (len(rows) >= BATCH_SIZE or due):
                if rows:
                    with conn.cursor() as cur:
                        cur.executemany(INSERT_SQL, rows)
                    conn.commit()
                consumer.commit(message=last_msg, asynchronous=False)
                print(f"stored {len(rows)} rows", flush=True)
                rows, last_msg = [], None
                last_flush = time.monotonic()
    except KeyboardInterrupt:
        print("shutting down", flush=True)
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()