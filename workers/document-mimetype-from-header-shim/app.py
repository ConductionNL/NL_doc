import json
import os
import sys
import time
import pika


def env(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None:
        raise RuntimeError(f"Missing env: {name}")
    return v


def amqp_url() -> str:
    proto = os.getenv("AMQP_PROTOCOL", "amqp")
    host = env("AMQP_HOST", "rabbitmq")
    port = os.getenv("AMQP_PORT", "5672")
    user = os.getenv("AMQP_USERNAME") or os.getenv("AMQP_USER") or "guest"
    pw = os.getenv("AMQP_PASSWORD") or os.getenv("AMQP_PASS") or "guest"
    return f"{proto}://{user}:{pw}@{host}:{port}/"


def main() -> None:
    exchange = "nldoc.topics"
    jobs_rk = "worker.document-mimetype-from-header.jobs"
    results_rk = "worker.document-mimetype-from-header.results.0"
    queue = "worker-document-mimetype-from-header"

    params = pika.URLParameters(amqp_url())
    while True:
        try:
            with pika.BlockingConnection(params) as conn:
                ch = conn.channel()
                ch.exchange_declare(exchange=exchange, exchange_type="topic", durable=True)
                ch.queue_declare(queue=queue, durable=True)
                ch.queue_bind(queue=queue, exchange=exchange, routing_key=jobs_rk)
                ch.basic_qos(prefetch_count=1)
                print(f"[mime-shim] consuming {jobs_rk} -> {results_rk}", flush=True)

                def handle(ch_, method, props, body):
                    try:
                        payload = json.loads(body.decode("utf-8"))
                    except Exception as e:
                        print(f"[mime-shim] bad json: {e}", file=sys.stderr, flush=True)
                        ch_.basic_ack(delivery_tag=method.delivery_tag)
                        return

                    job = payload.get("workerJob") if isinstance(payload, dict) else payload
                    if not isinstance(job, dict):
                        job = {}
                    # Best-effort set mimetype if missing
                    attrs = job.get("attributes") or {}
                    if "mimeType" not in attrs:
                        # crude guess from filename extension
                        fn = str(job.get("filename", "")).lower()
                        mt = "application/pdf" if fn.endswith(".pdf") else "application/octet-stream"
                        attrs["mimeType"] = mt
                        job["attributes"] = attrs

                    ch.basic_publish(
                        exchange=exchange,
                        routing_key=results_rk,
                        body=json.dumps(job).encode("utf-8"),
                        properties=pika.BasicProperties(content_type="application/json", delivery_mode=2),
                    )
                    print(f"[mime-shim] published {results_rk}", flush=True)
                    ch_.basic_ack(delivery_tag=method.delivery_tag)

                ch.basic_consume(queue=queue, on_message_callback=handle)
                ch.start_consuming()
        except Exception as e:
            print(f"[mime-shim] connection error: {e}; retrying...", file=sys.stderr, flush=True)
            time.sleep(5)


if __name__ == "__main__":
    main()


