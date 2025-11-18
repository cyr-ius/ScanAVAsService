#!/usr/bin/env bash
set -euo pipefail

# wait_and_start.sh
# waits for dependent services (minio, clamav, kafka) then launches scanner

: "${CLAMD_HOST:=clamav}"
: "${CLAMD_PORT:=3310}"
: "${S3_ENDPOINT_URL:=http://minio:9000}"
: "${KAFKA_BOOTSTRAP_SERVERS:=kafka:9092}"
: "${STARTUP_WAIT_SECS:=120}"

echo "Waiting up to ${STARTUP_WAIT_SECS}s for dependencies..."

end=$((SECONDS+STARTUP_WAIT_SECS))

# wait for clamd TCP port
until nc -z "${CLAMD_HOST}" "${CLAMD_PORT}" >/dev/null 2>&1; do
  if (( SECONDS >= end )); then
    echo "Timeout waiting for ClamAV (${CLAMD_HOST}:${CLAMD_PORT})"; exit 1
  fi
  echo "Waiting for ClamAV..."; sleep 2
done
echo "ClamAV reachable."

# wait for minio HTTP health
MINIO_HOST=$(echo "${S3_ENDPOINT_URL}" | sed -E 's#^https?://##' | cut -d'/' -f1)
until curl -sSf "http://${MINIO_HOST}/minio/health/live" >/dev/null 2>&1; do
  if (( SECONDS >= end )); then
    echo "Timeout waiting for MinIO (${MINIO_HOST})"; exit 1
  fi
  echo "Waiting for MinIO..."; sleep 2
done
echo "MinIO reachable."

# wait for kafka
KAFKA_HOST=$(echo "${KAFKA_BOOTSTRAP_SERVERS}" | cut -d',' -f1 | cut -d':' -f1)
KAFKA_PORT=$(echo "${KAFKA_BOOTSTRAP_SERVERS}" | cut -d',' -f1 | cut -d':' -f2)
until nc -z "${KAFKA_HOST}" "${KAFKA_PORT}" >/dev/null 2>&1; do
  if (( SECONDS >= end )); then
    echo "Timeout waiting for Kafka (${KAFKA_HOST}:${KAFKA_PORT})"; exit 1
  fi
  echo "Waiting for Kafka..."; sleep 2
done
echo "Kafka reachable."

echo "All dependencies up — starting scanner."
exec python /app/scanner_multi.py
