import json
import os
import sys
import time
import pika


def get_env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def build_amqp_url() -> str:
    protocol = os.getenv("AMQP_PROTOCOL", "amqp")
    host = get_env("AMQP_HOST", "rabbitmq")
    port = os.getenv("AMQP_PORT", "5672")

    # Prefer AMQP_USERNAME/AMQP_PASSWORD; fall back to AMQP_USER/AMQP_PASS
    username = os.getenv("AMQP_USERNAME") or os.getenv("AMQP_USER") or "guest"
    password = os.getenv("AMQP_PASSWORD") or os.getenv("AMQP_PASS") or "guest"

    return f"{protocol}://{username}:{password}@{host}:{port}/"


def main() -> None:
    amqp_url = build_amqp_url()
    exchange = "nldoc.topics"
    consume_routing_key = "worker.document-source.jobs"
    # Simulate worker result expected by station: publish a result for document-source
    publish_routing_key = "worker.document-source.results.0"
    queue_name = "worker-document-source"

    print(f"[shim] Connecting to {amqp_url}", flush=True)
    params = pika.URLParameters(amqp_url)

    while True:
        try:
            with pika.BlockingConnection(params) as connection:
                channel = connection.channel()
                # Declare topic exchange (idempotent if same type)
                channel.exchange_declare(
                    exchange=exchange, exchange_type="topic", durable=True
                )

                # Declare and bind queue for document-source jobs
                channel.queue_declare(queue=queue_name, durable=True)
                channel.queue_bind(
                    queue=queue_name, exchange=exchange, routing_key=consume_routing_key
                )

                channel.basic_qos(prefetch_count=1)

                def handle(ch, method, properties, body):
                    try:
                        payload = json.loads(body.decode("utf-8"))
                    except Exception as e:
                        print(
                            f"[shim] Invalid JSON body: {e}",
                            file=sys.stderr,
                            flush=True,
                        )
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                        return

                    # Expect either full envelope with 'workerJob' or direct job body
                    if (
                        isinstance(payload, dict)
                        and "workerJob" in payload
                        and isinstance(payload["workerJob"], dict)
                    ):
                        job = payload["workerJob"]
                    else:
                        job = payload

                    # Minimal validation: require bucketName and filename
                    if (
                        not isinstance(job, dict)
                        or "bucketName" not in job
                        or "filename" not in job
                    ):
                        print(
                            f"[shim] Missing required fields in job: {job}",
                            file=sys.stderr,
                            flush=True,
                        )
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                        return

                    out_body = json.dumps(job).encode("utf-8")
                    channel.basic_publish(
                        exchange=exchange,
                        routing_key=publish_routing_key,
                        body=out_body,
                        properties=pika.BasicProperties(
                            content_type="application/json", delivery_mode=2
                        ),
                    )
                    print(
                        f"[shim] Republished job to {publish_routing_key}: {job.get('filename')}",
                        flush=True,
                    )
                    ch.basic_ack(delivery_tag=method.delivery_tag)

                channel.basic_consume(queue=queue_name, on_message_callback=handle)
                print(
                    f"[shim] Consuming {consume_routing_key} → publishing {publish_routing_key}",
                    flush=True,
                )
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
