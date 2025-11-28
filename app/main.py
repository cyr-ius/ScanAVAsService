# main.py
import asyncio
import json
import logging
import os
import uuid
from datetime import datetime, timezone

import redis.asyncio as redis
from aiobotocore.session import get_session
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi import FastAPI, HTTPException, UploadFile
from fastapi.responses import StreamingResponse
from model import ScanResult
from starlette.background import BackgroundTask

# --- Config ---
S3_ENDPOINT = os.getenv("S3_ENDPOINT_URL", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
S3_BUCKET = os.getenv("S3_BUCKET", "scans")
S3_SCAN_RESULT = os.getenv("S3_SCAN_RESULT", "processed")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_INPUT_TOPIC = os.getenv("KAFKA_INPUT_TOPIC", "files_to_scan")
KAFKA_OUTPUT_TOPIC = os.getenv("KAFKA_OUTPUT_TOPIC", "scan_results")

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
LOCK_TIMEOUT = 15  # seconds

LOGGER = logging.getLogger("api")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL)

app = FastAPI()
redis_client = redis.from_url(REDIS_URL)
session = get_session()
producer: AIOKafkaProducer = None


# ---------------- Upload endpoint ----------------
@app.post("/upload")
async def upload_file(file: UploadFile) -> ScanResult:
    unique_key = f"{uuid.uuid4()}_{file.filename}"
    data = await file.read()

    # Check if file exists
    async with session.create_client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
    ) as client:
        await client.put_object(Bucket=S3_BUCKET, Key=unique_key, Body=data)

    # Send Kafka message
    record_id = str(uuid.uuid4())
    payload = ScanResult(
        id=record_id,
        status="PENDING",
        bucket=S3_BUCKET,
        key=unique_key,
        original_filename=file.filename,
        timestamp=datetime.now(timezone.utc).isoformat(),
    )
    await producer.send_and_wait(
        KAFKA_INPUT_TOPIC, value=payload.json().encode("utf-8")
    )

    return payload


# ---------------- Download scanned file ----------------
@app.get("/download/{id}")
async def download_scanned_file(id: str, force: bool = False):
    kafka_result = await fetch_scan_result(id)
    if kafka_result.status == "NO_FOUND":
        raise HTTPException(status_code=404, detail="File is not found")
    if kafka_result.status == "PENDING":
        raise HTTPException(status_code=202, detail="File is pending scan")
    if kafka_result.status != "CLEAN" and not force:
        raise HTTPException(status_code=403, detail="File is not clean for download")
    try:
        # Create S3 client (do NOT use async-with to avoid auto-close)
        s3_client = await session.create_client(
            "s3",
            endpoint_url=S3_ENDPOINT,
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY,
        ).__aenter__()  # manually enter the async context

        resp = await s3_client.get_object(Bucket=S3_BUCKET, Key=kafka_result.key)
        body = resp["Body"]

        headers = {
            "Content-Disposition": f"attachment; filename={kafka_result.original_filename}"
        }
        return StreamingResponse(
            body,
            media_type="application/octet-stream",
            headers=headers,
            background=BackgroundTask(body.close),
        )

    except s3_client.exceptions.ClientError as e:
        raise HTTPException(
            status_code=404, detail="File not found in processed"
        ) from e
    except s3_client.exceptions.NoSuchKey:
        raise HTTPException(status_code=404, detail="File not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Download error: {e}")


# ---------------- Scan status endpoint ----------------
@app.get("/result/{id}")
async def scan_status(id: str) -> ScanResult:
    result = await fetch_scan_result(id)
    return result


# ---------------- Startup / Shutdown ----------------
@app.on_event("startup")
async def startup_event():
    global producer
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    await producer.start()
    LOGGER.info("Kafka producer started")


@app.on_event("shutdown")
async def shutdown_event():
    await producer.stop()
    await redis_client.close()
    await redis_client.connection_pool.disconnect()
    LOGGER.info("Shutdown complete")


async def fetch_scan_result(record_id: str, timeout=5) -> ScanResult:
    consumer = AIOKafkaConsumer(
        KAFKA_OUTPUT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=f"api-tracker-{uuid.uuid4()}",
    )

    await consumer.start()
    try:
        deadline = asyncio.get_event_loop().time() + timeout

        while True:
            remaining = deadline - asyncio.get_event_loop().time()
            if remaining <= 0:
                break

            # poll avec timeout
            batch = await consumer.getmany(timeout_ms=int(remaining * 1000))

            for tp, messages in batch.items():
                for msg in messages:
                    payload = json.loads(msg.value.decode())
                    if payload.get("id") == record_id:
                        return ScanResult(**payload)
        return ScanResult(
            id=record_id, status="NO_FOUND", details="Result not ready yet"
        )

    finally:
        await consumer.stop()
