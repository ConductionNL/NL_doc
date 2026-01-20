"""
Page Worker Adapter Shim

This shim bridges the gap between:
- pdf.page-* stations (which use worker.pdf.page-*.jobs routing keys)
- folio.page-* workers (which only accept worker.folio.page-*.jobs routing keys)

It receives jobs from pdf stations and re-publishes them to folio workers
with the correct routing key format.
"""

import json
import os
import socket
import sys
import time
import uuid
from datetime import datetime, timezone

import pika


INSTANCE_NAME = f"adapter-page-worker-{socket.gethostname()}"


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


# Routing key translations
ROUTING_TRANSLATIONS = {
    "worker.pdf.page-regions.jobs": "worker.folio.page-regions.jobs",
    "worker.pdf.page-interpretedcontent.jobs": "worker.folio.page-interpretedcontent.jobs",
}


def main() -> None:
    amqp_url = build_amqp_url()
    exchange = "nldoc.topics"
    queue_name = "adapter-page-worker"

    print(f"[adapter] {INSTANCE_NAME} starting...", flush=True)
    print(f"[adapter] Connecting to {amqp_url.replace(amqp_url.split('@')[0].split('//')[1], '***')}", flush=True)
    params = pika.URLParameters(amqp_url)

    while True:
        try:
            with pika.BlockingConnection(params) as connection:
                channel = connection.channel()
                channel.exchange_declare(
                    exchange=exchange, exchange_type="topic", durable=True
                )
                channel.queue_declare(queue=queue_name, durable=True)

                # Bind to all pdf.page-* job routing keys
                for source_rk in ROUTING_TRANSLATIONS.keys():
                    channel.queue_bind(
                        queue=queue_name, exchange=exchange, routing_key=source_rk
                    )
                    print(f"[adapter] Bound to {source_rk}", flush=True)

                channel.basic_qos(prefetch_count=1)

                print(f"[adapter] Consuming and forwarding...", flush=True)

                def handle(ch, method, properties, body):
                    source_routing_key = method.routing_key
                    target_routing_key = ROUTING_TRANSLATIONS.get(source_routing_key)

                    if not target_routing_key:
                        print(
                            f"[adapter] Unknown routing key: {source_routing_key}",
                            file=sys.stderr,
                            flush=True,
                        )
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                        return

                    try:
                        payload = json.loads(body.decode("utf-8"))
                    except Exception as e:
                        print(f"[adapter] Invalid JSON: {e}", file=sys.stderr, flush=True)
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                        return

                    # Extract trace info
                    headers = properties.headers or {}
                    trace_id = headers.get("x-trace-id", str(uuid.uuid4()))

                    # Log the forwarding
                    record_id = "unknown"
                    if isinstance(payload, dict):
                        record_id = payload.get("recordId", payload.get("workerJob", {}).get("recordId", "unknown"))

                    print(
                        f"[adapter] Forwarding {source_routing_key} -> {target_routing_key} | record={record_id}",
                        flush=True,
                    )

                    # Re-publish with new routing key, keeping original properties
                    new_headers = dict(headers)
                    new_headers["x-adapter-forwarded"] = "true"
                    new_headers["x-original-routing-key"] = source_routing_key

                    channel.basic_publish(
                        exchange=exchange,
                        routing_key=target_routing_key,
                        body=body,  # Pass through unchanged
                        properties=pika.BasicProperties(
                            content_type=properties.content_type or "application/json",
                            delivery_mode=2,
                            headers=new_headers,
                        ),
                    )

                    ch.basic_ack(delivery_tag=method.delivery_tag)

                channel.basic_consume(queue=queue_name, on_message_callback=handle)
                channel.start_consuming()

        except Exception as e:
            print(
                f"[adapter] Connection error: {e}. Retrying in 5s...",
                file=sys.stderr,
                flush=True,
            )
            time.sleep(5)


if __name__ == "__main__":
    main()

