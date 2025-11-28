#!/usr/bin/env python3
import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

import redis.asyncio as redis
from aiobotocore.session import get_session
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from model import ScanResult

# ----------------- CONFIG -----------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_INPUT_TOPIC = os.getenv("KAFKA_INPUT_TOPIC", "files_to_scan")
KAFKA_OUTPUT_TOPIC = os.getenv("KAFKA_OUTPUT_TOPIC", "scan_results")

S3_ENDPOINT = os.getenv("S3_ENDPOINT_URL", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
S3_BUCKET = os.getenv("S3_BUCKET", "scans")
S3_SCAN_RESULT = os.getenv("S3_SCAN_RESULT", "processed")
S3_SCAN_QUARANTINE = os.getenv("S3_SCAN_QUARANTINE", "quarantine")

CLAMD_HOST = os.getenv("CLAMD_HOST", "clamav")
CLAMD_PORT = int(os.getenv("CLAMD_PORT", 3310))

WORKER_POOL = int(os.getenv("WORKER_POOL", 4))
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
LOCK_TIMEOUT = 15  # seconds

LOGGER = logging.getLogger(__name__)
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL)
# LOGGER.setLevel(LOG_LEVEL)

# --- aiobotocore session ---
session = get_session()


# -------------------- Mutex S3 via Redis --------------------
async def acquire_s3_lock(
    redis_client, bucket: str, key: str, timeout: int = LOCK_TIMEOUT
):
    lock_key = f"lock:{bucket}/{key}"
    return await redis_client.set(lock_key, "1", nx=True, ex=timeout)


async def release_s3_lock(redis_client, bucket: str, key: str):
    lock_key = f"lock:{bucket}/{key}"
    await redis_client.delete(lock_key)


async def scan_s3_object_async(bucket: str, key: str) -> dict[str, Any]:
    """
    Stream file from S3 to ClamAV via INSTREAM command
    """
    try:
        async with session.create_client(
            "s3",
            endpoint_url=S3_ENDPOINT,
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY,
        ) as client:
            resp = await client.get_object(Bucket=bucket, Key=key)
            body = resp["Body"]

            reader, writer = await asyncio.open_connection(CLAMD_HOST, CLAMD_PORT)
            writer.write(b"zINSTREAM\0")
            await writer.drain()

            while True:
                chunk = await body.read(1024 * 1024)
                if not chunk:
                    break
                writer.write(len(chunk).to_bytes(4, "big") + chunk)
                await writer.drain()

            writer.write(b"\x00\x00\x00\x00")
            await writer.drain()

            data = await reader.read(1024)
            response = data.decode().strip()
            writer.close()
            await writer.wait_closed()

            if "OK" in response:
                return {"status": "CLEAN", "virus": None}
            elif "FOUND" in response:
                virus = response.split("FOUND")[0].strip()
                return {"status": "INFECTED", "virus": virus}
            else:
                return {"status": "ERROR", "virus": None, "details": response}
    except Exception as e:
        return {"status": "ERROR", "virus": None, "details": str(e)}


async def move_s3_object_async(bucket: str, old_key: str, new_key: str):
    async with session.create_client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
    ) as client:
        try:
            await client.copy_object(
                Bucket=bucket,
                CopySource={"Bucket": bucket, "Key": old_key},
                Key=new_key,
            )
            await client.delete_object(Bucket=bucket, Key=old_key)
        except client.exceptions.NoSuchKey:
            LOGGER.warning(f"[move] Key not found: {old_key}")


# ----------------- WORKER -----------------
async def worker(
    name: int, queue: asyncio.Queue, producer: AIOKafkaProducer, redis_client
):
    while True:
        payload = await queue.get()
        try:
            record_id = payload["id"]
            bucket = payload["bucket"]
            key = payload["key"]
            original_filename = payload["original_filename"]

            LOGGER.info(f"[worker-{name}] Start scan {key}")

            # Acquire lock
            if not await acquire_s3_lock(redis_client, bucket, key):
                LOGGER.warning(f"[worker-{name}] File locked, skipping {key}")
                queue.task_done()
                continue

            try:
                scan = await scan_s3_object_async(bucket, key)
                status = scan["status"]
                new_key = (
                    f"{S3_SCAN_RESULT}/{key}"
                    if status == "CLEAN"
                    else f"{S3_SCAN_QUARANTINE}/{key}"
                )
                await move_s3_object_async(bucket, key, new_key)

                result = ScanResult(
                    id=record_id,
                    bucket=bucket,
                    key=new_key,
                    status=status,
                    virus=scan.get("virus"),
                    details=scan.get("details", ""),
                    original_filename=original_filename,
                    timestamp=datetime.now(timezone.utc).isoformat(),
                )  # Validate result model

                # Store result in Redis for API access
                await redis_client.set(
                    f"scan_result:{record_id}", result.json(), ex=3600
                )

                # Optionally send result back to Kafka
                await producer.send_and_wait(
                    KAFKA_OUTPUT_TOPIC, result.json().encode("utf-8")
                )

                LOGGER.info(f"[worker-{name}] Scanned {key} → {status}")

            finally:
                await release_s3_lock(redis_client, bucket, key)

        except Exception as e:
            LOGGER.error(f"[worker-{name}] Error: {e}")
        finally:
            queue.task_done()


# ----------------- CONSUMER -----------------
async def consume_loop(queue: asyncio.Queue):
    consumer = AIOKafkaConsumer(
        KAFKA_INPUT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="scanner-group",
        enable_auto_commit=True,
    )
    await consumer.start()
    try:
        async for msg in consumer:
            payload = json.loads(msg.value.decode("utf-8"))
            await queue.put(payload)
    finally:
        await consumer.stop()


# ----------------- MAIN -----------------
async def main():
    global producer
    queue = asyncio.Queue(maxsize=WORKER_POOL * 2)
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    await producer.start()

    redis_client = redis.from_url(REDIS_URL)

    # Start workers
    workers = [
        asyncio.create_task(worker(i + 1, queue, producer, redis_client))
        for i in range(WORKER_POOL)
    ]
    # Start consumer
    consumer_task = asyncio.create_task(consume_loop(queue))

    try:
        await consumer_task
    finally:
        for w in workers:
            w.cancel()
        await producer.stop()
        await redis_client.aclose()
        await redis_client.connection_pool.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
