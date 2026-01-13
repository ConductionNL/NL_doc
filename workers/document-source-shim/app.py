import json
import os
import socket
import sys
import threading
import time
import uuid
from datetime import datetime, timezone

import pika

INSTANCE_NAME = f"shim-document-source-{socket.gethostname()}"
WORKER_NAME = "worker-document-source"
WORKER_TYPE = "document-source"  # For routing keys: worker.<type>.acks
HEARTBEAT_INTERVAL = 60  # seconds


def get_env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def build_amqp_url() -> str:
    protocol = os.getenv("AMQP_PROTOCOL", "amqp")
    host = get_env("AMQP_HOST", "rabbitmq")
    port = os.getenv("AMQP_PORT", "5672")
    username = os.getenv("AMQP_USERNAME") or os.getenv("AMQP_USER") or "guest"
    password = os.getenv("AMQP_PASSWORD") or os.getenv("AMQP_PASS") or "guest"
    return f"{protocol}://{username}:{password}@{host}:{port}/"


def make_headers(trace_id: str | None = None) -> dict:
    """Build standard Kimi worker headers."""
    return {
        "x-kimi-worker-instance-name": INSTANCE_NAME,
        "x-kimi-worker-name": WORKER_NAME,
        "x-trace-id": trace_id or str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "priority": 5,
    }


def send_worker_signal(channel, exchange: str, signal_type: str):
    """Send worker started/heartbeat signal to register with the station."""
    routing_key = f"worker.{WORKER_TYPE}.acks"
    trace_id = str(uuid.uuid4())
    headers = make_headers(trace_id)

    # The station expects a specific format for worker signals
    body = json.dumps(
        {
            "workerName": WORKER_NAME,
            "workerInstance": INSTANCE_NAME,
            "signal": signal_type,  # "started" or "heartbeat"
        }
    ).encode("utf-8")

    channel.basic_publish(
        exchange=exchange,
        routing_key=routing_key,
        body=body,
        properties=pika.BasicProperties(
            content_type="application/json",
            delivery_mode=2,
            headers=headers,
        ),
    )
    print(f"[shim] Sent {signal_type} signal to {routing_key}", flush=True)


def send_job_ack(channel, exchange: str, job: dict, job_id: str, trace_id: str):
    """Send job acknowledgment to the station."""
    routing_key = f"worker.{WORKER_TYPE}.acks"
    headers = make_headers(trace_id)

    record_id = job.get("recordId", f"document|||{job.get('filename', 'unknown')}")
    body = json.dumps(
        {
            "workerName": WORKER_NAME,
            "workerInstance": INSTANCE_NAME,
            "jobId": job_id,
            "recordId": record_id,
        }
    ).encode("utf-8")

    channel.basic_publish(
        exchange=exchange,
        routing_key=routing_key,
        body=body,
        properties=pika.BasicProperties(
            content_type="application/json",
            delivery_mode=2,
            headers=headers,
        ),
    )
    print(f"[shim] Sent job ACK for {job_id}", flush=True)


def heartbeat_thread(amqp_url: str, exchange: str):
    """Background thread that sends periodic heartbeats."""
    params = pika.URLParameters(amqp_url)

    while True:
        try:
            with pika.BlockingConnection(params) as conn:
                ch = conn.channel()
                ch.exchange_declare(
                    exchange=exchange, exchange_type="topic", durable=True
                )

                while True:
                    send_worker_signal(ch, exchange, "heartbeat")
                    time.sleep(HEARTBEAT_INTERVAL)
        except Exception as e:
            print(
                f"[shim/heartbeat] Error: {e}. Retrying in 10s...",
                file=sys.stderr,
                flush=True,
            )
            time.sleep(10)


def main() -> None:
    amqp_url = build_amqp_url()
    exchange = "nldoc.topics"
    consume_routing_key = "worker.document-source.jobs"
    publish_result_rk = "worker.document-source.results.0"
    queue_name = "worker-document-source"

    print(f"[shim] Worker {INSTANCE_NAME} starting...", flush=True)
    print(f"[shim] Connecting to {amqp_url}", flush=True)
    params = pika.URLParameters(amqp_url)

    # Start heartbeat thread
    hb_thread = threading.Thread(
        target=heartbeat_thread, args=(amqp_url, exchange), daemon=True
    )
    hb_thread.start()

    while True:
        try:
            with pika.BlockingConnection(params) as connection:
                channel = connection.channel()
                channel.exchange_declare(
                    exchange=exchange, exchange_type="topic", durable=True
                )
                channel.queue_declare(queue=queue_name, durable=True)
                channel.queue_bind(
                    queue=queue_name, exchange=exchange, routing_key=consume_routing_key
                )
                channel.basic_qos(prefetch_count=1)

                # Send "Worker started" signal to register with the station
                send_worker_signal(channel, exchange, "started")

                def handle(ch, method, properties, body):
                    try:
                        payload = json.loads(body.decode("utf-8"))
                    except Exception as e:
                        print(f"[shim] Invalid JSON: {e}", file=sys.stderr, flush=True)
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                        return

                    # Extract job from envelope
                    if isinstance(payload, dict) and "workerJob" in payload:
                        job = payload["workerJob"]
                    else:
                        job = payload

                    if (
                        not isinstance(job, dict)
                        or "bucketName" not in job
                        or "filename" not in job
                    ):
                        print(
                            f"[shim] Missing fields in job: {job}",
                            file=sys.stderr,
                            flush=True,
                        )
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                        return

                    # Generate IDs for this job
                    job_id = str(uuid.uuid4())
                    trace_id = str(uuid.uuid4())
                    record_id = job.get("recordId", f"document|||{job.get('filename')}")

                    # Send job ACK to station (so it knows we're processing)
                    send_job_ack(channel, exchange, job, job_id, trace_id)

                    # Ensure mimeType
                    attrs = job.get("attributes") or {}
                    if "mimeType" not in attrs:
                        fn = str(job.get("filename", "")).lower()
                        attrs["mimeType"] = (
                            "application/pdf"
                            if fn.endswith(".pdf")
                            else "application/octet-stream"
                        )
                    job["attributes"] = attrs

                    # Build result with proper structure
                    result = {
                        "workerResult": {
                            "jobId": job_id,
                            "recordId": record_id,
                            "bucketName": job.get("bucketName"),
                            "filename": job.get("filename"),
                            "attributes": attrs,
                            "output": {
                                "fileType": attrs.get("mimeType"),
                                "filename": job.get("filename"),
                            },
                        }
                    }
                    result_body = json.dumps(result).encode("utf-8")

                    headers = make_headers(trace_id)

                    # Publish result to station
                    # Use job-specific routing key: worker.document-source.results.<jobId>
                    result_rk = f"worker.document-source.results.{job_id}"
                    channel.basic_publish(
                        exchange=exchange,
                        routing_key=result_rk,
                        body=result_body,
                        properties=pika.BasicProperties(
                            content_type="application/json",
                            delivery_mode=2,
                            headers=headers,
                        ),
                    )
                    print(
                        f"[shim] Published result to {result_rk} (trace={trace_id})",
                        flush=True,
                    )

                    ch.basic_ack(delivery_tag=method.delivery_tag)

                channel.basic_consume(queue=queue_name, on_message_callback=handle)
                print(f"[shim] Consuming {consume_routing_key}", flush=True)
                channel.start_consuming()

        except Exception as e:
            print(
                f"[shim] Connection error: {e}. Retrying in 5s...",
                file=sys.stderr,
                flush=True,
            )
            time.sleep(5)


if __name__ == "__main__":
    main()
