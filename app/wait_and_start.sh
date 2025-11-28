#!/usr/bin/env bash
set -euo pipefail

# wait_and_start.sh
# waits for ClamAV, MinIO, Kafka, then launches FastAPI + scanner worker

: "${CLAMD_HOST:=clamav}"
: "${CLAMD_PORT:=3310}"
: "${S3_ENDPOINT_URL:=http://minio:9000}"
: "${KAFKA_BOOTSTRAP_SERVERS:=kafka:9092}"
: "${FASTAPI_PORT:=8000}"
: "${STARTUP_WAIT_SECS:=120}"

echo "Waiting up to ${STARTUP_WAIT_SECS}s for dependencies..."

end=$((SECONDS + STARTUP_WAIT_SECS))

# --- Wait for ClamAV TCP port ---
until nc -z "${CLAMD_HOST}" "${CLAMD_PORT}" >/dev/null 2>&1; do
  if (( SECONDS >= end )); then
    echo "Timeout waiting for ClamAV (${CLAMD_HOST}:${CLAMD_PORT})"
    exit 1
  fi
  echo "Waiting for ClamAV..."
  sleep 2
done
echo "ClamAV reachable."

# --- Wait for MinIO HTTP health ---
MINIO_HOST=$(echo "${S3_ENDPOINT_URL}" | sed -E 's#^https?://##' | cut -d'/' -f1)
until curl -sSf "http://${MINIO_HOST}/minio/health/live" >/dev/null 2>&1; do
  if (( SECONDS >= end )); then
    echo "Timeout waiting for MinIO (${MINIO_HOST})"
    exit 1
  fi
  echo "Waiting for MinIO..."
  sleep 2
done
echo "MinIO reachable."

# --- Wait for Kafka ---
KAFKA_HOST=$(echo "${KAFKA_BOOTSTRAP_SERVERS}" | cut -d',' -f1 | cut -d':' -f1)
KAFKA_PORT=$(echo "${KAFKA_BOOTSTRAP_SERVERS}" | cut -d',' -f1 | cut -d':' -f2)
until nc -z "${KAFKA_HOST}" "${KAFKA_PORT}" >/dev/null 2>&1; do
  if (( SECONDS >= end )); then
    echo "Timeout waiting for Kafka (${KAFKA_HOST}:${KAFKA_PORT})"
    exit 1
  fi
  echo "Waiting for Kafka..."
  sleep 2
done
echo "Kafka reachable."

# --- Start FastAPI in background ---
echo "Starting FastAPI on port ${FASTAPI_PORT}..."
uvicorn main:app --host 0.0.0.0 --port "${FASTAPI_PORT}" &
FASTAPI_PID=$!

# Wait for FastAPI to be ready
until curl -sSf "http://127.0.0.1:${FASTAPI_PORT}/docs" >/dev/null 2>&1; do
  if (( SECONDS >= end )); then
    echo "Timeout waiting for FastAPI"
    kill "${FASTAPI_PID}" || true
    exit 1
  fi
  echo "Waiting for FastAPI to start..."
  sleep 2
done
echo "FastAPI ready."

# --- Start scanner worker ---
echo "Starting scanner worker..."
exec python /app/scanner_multi.py
