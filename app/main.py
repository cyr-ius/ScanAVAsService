# main.py
import asyncio
import json
import logging
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from aiobotocore.session import get_session
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi import FastAPI, HTTPException, UploadFile
from fastapi.responses import StreamingResponse
from model import ScanResult
from starlette.background import BackgroundTask

# --- Config ---
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_INPUT_TOPIC = os.getenv("KAFKA_INPUT_TOPIC", "files_to_scan")
KAFKA_OUTPUT_TOPIC = os.getenv("KAFKA_OUTPUT_TOPIC", "scan_results")

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_BUCKET = os.getenv("S3_BUCKET", "scans")
S3_ENDPOINT = os.getenv("S3_ENDPOINT_URL", "http://minio:9000")
S3_SCAN_RESULT = os.getenv("S3_SCAN_RESULT", "processed")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")

SEARCH_TIMEOUT = os.getenv("SEARCH_TIMEOUT", 5)  # seconds
VERSION = os.getenv("APP_VERSION", "unknown")

session = get_session()
producer: AIOKafkaProducer = None
logger = logging.getLogger("api")
logging.basicConfig(level=LOG_LEVEL)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize Kafka producer and Redis client."""
    global producer
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    await producer.start()
    logger.info("Kafka producer started")
    yield
    await producer.stop()
    logger.info("Shutdown complete")


app = FastAPI(title="ScanAV API", lifespan=lifespan, version=VERSION)


@app.post("/upload")
async def upload_file_to_scan(file: UploadFile) -> ScanResult:
    """Upload file to S3 and send scan request to Kafka."""
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


@app.get("/download/{id}")
async def download_scanned_file(id: str, force: bool = False):
    """Download scanned file by ID if clean or force is True."""
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


@app.get("/result/{id}")
async def scan_status(id: str) -> ScanResult:
    """Fetch scan result by ID."""
    result = await fetch_scan_result(id)
    return result


async def fetch_scan_result(record_id: str, timeout=SEARCH_TIMEOUT) -> ScanResult:
    """Fetch scan result from Kafka with timeout."""
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
