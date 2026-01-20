#!/usr/bin/env python3
"""
SSE Bridge Worker - Adds x-stream-filter-value header to events for the API.

The NLdoc API expects events in the 'results' stream with an 'x-stream-filter-value'
header containing the document ID. The stations publish events to 'specs.{docId}'
but without this header, so the API cannot filter them correctly.

This bridge:
1. Consumes events from specs.* on nldoc.topics
2. Adds the x-stream-filter-value header with the document ID
3. Republishes to the 'results' exchange (which feeds the stream)
"""

import os
import sys
import json
import time
import pika

AMQP_HOST = os.environ.get("AMQP_HOST", "localhost")
AMQP_PORT = int(os.environ.get("AMQP_PORT", "5672"))
AMQP_USERNAME = os.environ.get("AMQP_USERNAME", os.environ.get("RABBITMQ_DEFAULT_USER", "guest"))
AMQP_PASSWORD = os.environ.get("AMQP_PASSWORD", os.environ.get("RABBITMQ_DEFAULT_PASS", "guest"))


def extract_document_id(routing_key, payload):
    """Extract document ID from routing key or payload."""
    # specs.{document_id}
    if routing_key.startswith("specs."):
        return routing_key.split(".", 1)[1]
    
    # documents.{document_id}
    if routing_key.startswith("documents."):
        return routing_key.split(".", 1)[1]
    
    # Try from payload
    if isinstance(payload, dict):
        if "documentId" in payload:
            return payload["documentId"]
        if "_documentId" in payload:
            return payload["_documentId"]
        if "context" in payload and "documentId" in payload["context"]:
            return payload["context"]["documentId"]
    
    return None


def main():
    print("[sse-bridge] Starting SSE Bridge Worker")
    print(f"[sse-bridge] Connecting to {AMQP_HOST}:{AMQP_PORT}")
    
    credentials = pika.PlainCredentials(AMQP_USERNAME, AMQP_PASSWORD)
    
    while True:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=AMQP_HOST,
                    port=AMQP_PORT,
                    credentials=credentials,
                    heartbeat=30
                )
            )
            channel = connection.channel()
            
            # Declare the bridge queue
            queue_name = "sse-bridge-queue"
            channel.queue_declare(queue=queue_name, durable=True)
            
            # Bind to specs.* and documents.* events
            channel.queue_bind(
                queue=queue_name,
                exchange="nldoc.topics",
                routing_key="specs.*"
            )
            channel.queue_bind(
                queue=queue_name,
                exchange="nldoc.topics",
                routing_key="documents.*"
            )
            
            print(f"[sse-bridge] Consuming from {queue_name} (specs.* and documents.*)")
            print("[sse-bridge] Bridging events to 'results' exchange with x-stream-filter-value header")
            
            def handle_event(ch, method, properties, body):
                routing_key = method.routing_key
                
                try:
                    payload = json.loads(body.decode("utf-8"))
                except:
                    payload = {}
                
                # Extract document ID
                doc_id = extract_document_id(routing_key, payload)
                
                if not doc_id:
                    print(f"[sse-bridge] Could not extract doc ID from {routing_key}")
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    return
                
                # Get existing headers
                headers = dict(properties.headers) if properties.headers else {}
                
                # Add the stream filter header
                headers["x-stream-filter-value"] = doc_id
                
                # Log the bridge
                event_type = payload.get("type", "unknown")
                print(f"[sse-bridge] Bridging {routing_key} -> results | doc={doc_id} | type={event_type}")
                
                # Republish to results exchange (which feeds the stream)
                ch.basic_publish(
                    exchange="results",
                    routing_key="",  # Fanout exchange ignores routing key
                    body=body,
                    properties=pika.BasicProperties(
                        content_type=properties.content_type,
                        content_encoding=properties.content_encoding,
                        headers=headers,
                        delivery_mode=2
                    )
                )
                
                ch.basic_ack(delivery_tag=method.delivery_tag)
            
            channel.basic_qos(prefetch_count=10)
            channel.basic_consume(queue=queue_name, on_message_callback=handle_event)
            channel.start_consuming()
            
        except pika.exceptions.AMQPConnectionError as e:
            print(f"[sse-bridge] Connection lost: {e}, reconnecting in 5s...")
            time.sleep(5)
        except Exception as e:
            print(f"[sse-bridge] Error: {e}, retrying in 5s...")
            time.sleep(5)


if __name__ == "__main__":
    main()

