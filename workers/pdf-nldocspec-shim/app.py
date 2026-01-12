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
    jobs_rk = "worker.pdf-nldocspec.jobs"
    results_rk = "worker.pdf-nldocspec.results.0"
    queue = "worker-pdf-nldocspec"

    params = pika.URLParameters(amqp_url())
    while True:
        try:
            with pika.BlockingConnection(params) as conn:
                ch = conn.channel()
                ch.exchange_declare(exchange=exchange, exchange_type="topic", durable=True)
                ch.queue_declare(queue=queue, durable=True)
                ch.queue_bind(queue=queue, exchange=exchange, routing_key=jobs_rk)
                ch.basic_qos(prefetch_count=1)
                print(f"[nldocspec-shim] consuming {jobs_rk} -> {results_rk}", flush=True)

                def handle(ch_, method, props, body):
                    try:
                        payload = json.loads(body.decode("utf-8"))
                    except Exception as e:
                        print(f"[nldocspec-shim] bad json: {e}", file=sys.stderr, flush=True)
                        ch_.basic_ack(delivery_tag=method.delivery_tag)
                        return

                    job = payload.get("workerJob") if isinstance(payload, dict) else payload
                    if not isinstance(job, dict):
                        job = {}
                    # Ensure minimal fields for downstream
                    job.setdefault("recordId", "")
                    job.setdefault("bucketName", "files")
                    job.setdefault("filename", job.get("filename", ""))
                    attrs = job.get("attributes") or {}
                    attrs.setdefault("specVersion", "1.0")
                    job["attributes"] = attrs

                    ch.basic_publish(
                        exchange=exchange,
                        routing_key=results_rk,
                        body=json.dumps(job).encode("utf-8"),
                        properties=pika.BasicProperties(content_type="application/json", delivery_mode=2),
                    )
                    print(f"[nldocspec-shim] published {results_rk}", flush=True)
                    ch_.basic_ack(delivery_tag=method.delivery_tag)

                ch.basic_consume(queue=queue, on_message_callback=handle)
                ch.start_consuming()
        except Exception as e:
            print(f"[nldocspec-shim] connection error: {e}; retry...", file=sys.stderr, flush=True)
            time.sleep(5)


if __name__ == "__main__":
    main()


