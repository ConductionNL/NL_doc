#!/usr/bin/env python3
"""
Sniff a single AMQP message for a given routing-key pattern on nldoc.topics.
Used to understand expected heartbeat payload schemas in a running cluster.
"""

import json
import os
import time
import pika


AMQP_HOST = os.environ.get("AMQP_HOST", "rabbitmq.nldoc.svc.cluster.local")
AMQP_PORT = int(os.environ.get("AMQP_PORT", "5672"))
AMQP_USERNAME = os.environ.get("AMQP_USERNAME", "guest")
AMQP_PASSWORD = os.environ.get("AMQP_PASSWORD", "guest")
ROUTING_KEY = os.environ.get("ROUTING_KEY", "worker.pdf-pdfmetadata.health.heartbeats.*")
EXCHANGE = os.environ.get("EXCHANGE", "nldoc.topics")


def main() -> None:
    credentials = pika.PlainCredentials(AMQP_USERNAME, AMQP_PASSWORD)
    conn = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=AMQP_HOST,
            port=AMQP_PORT,
            credentials=credentials,
            heartbeat=10,
        )
    )
    ch = conn.channel()
    q = ch.queue_declare(queue="", exclusive=True).method.queue
    ch.queue_bind(exchange=EXCHANGE, queue=q, routing_key=ROUTING_KEY)
    print(f"bound queue={q} exchange={EXCHANGE} routing_key={ROUTING_KEY}", flush=True)

    for _ in range(100):
        method, props, body = ch.basic_get(queue=q, auto_ack=True)
        if method:
            raw = body.decode("utf-8", "replace")
            print("raw:", raw[:500], flush=True)
            try:
                obj = json.loads(raw)
                print("json keys:", sorted(obj.keys()), flush=True)
                print("json:", json.dumps(obj, indent=2)[:2000], flush=True)
            except Exception as e:
                print("json parse error:", repr(e), flush=True)
            break
        time.sleep(0.2)

    conn.close()


if __name__ == "__main__":
    main()


