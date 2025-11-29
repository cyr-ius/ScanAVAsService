FROM python:3.11-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

RUN apt-get update
RUN apt-get install -y build-essential curl netcat-openbsd pkg-config libssl-dev ca-certificates
RUN rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ .
RUN chmod +x wait_and_start.sh

EXPOSE 8000

ARG VERSION=1.0.0
ENV APP_VERSION=${VERSION}

CMD ["/app/wait_and_start.sh"]
