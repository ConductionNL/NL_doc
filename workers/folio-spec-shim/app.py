#!/usr/bin/env python3
"""
Folio-spec shim worker for PoC.
Converts folio attributes to a basic TipTap JSON spec.
"""
import os
import json
import time
import uuid
import pika
from datetime import datetime
from minio import Minio

AMQP_URL = os.environ.get("AMQP_URL", "amqp://guest:guest@rabbitmq-server:5672")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minio123")

INSTANCE_NAME = f"folio-spec-shim-{uuid.uuid4().hex[:8]}"
WORKER_NAME = "folio-spec-shim"

def get_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

def make_headers(trace_id):
    return {
        "x-kimi-worker-instance-name": INSTANCE_NAME,
        "x-kimi-worker-name": WORKER_NAME,
        "x-trace-id": trace_id,
        "timestamp": datetime.utcnow().isoformat()
    }

def generate_tiptap_spec(page_count, doc_id):
    """Generate a basic TipTap JSON document from page count."""
    content = [
        {
            "type": "heading",
            "attrs": {"level": 1},
            "content": [{"type": "text", "text": f"Document: {doc_id}"}]
        },
        {
            "type": "paragraph",
            "content": [{"type": "text", "text": f"Dit document bevat {page_count} pagina's."}]
        }
    ]
    
    # Add a section for each page
    for i in range(1, page_count + 1):
        content.append({
            "type": "heading",
            "attrs": {"level": 2},
            "content": [{"type": "text", "text": f"Pagina {i}"}]
        })
        content.append({
            "type": "paragraph",
            "content": [{"type": "text", "text": f"Inhoud van pagina {i}."}]
        })
    
    return {"type": "doc", "content": content}

def handle_job(ch, method, properties, body):
    """Handle incoming folio-spec job."""
    try:
        job = json.loads(body)
        print(f"[shim] Received job: {json.dumps(job)[:200]}...")
        
        record_id = job.get("recordId", "")
        bucket_name = job.get("bucketName", "files")
        filename = job.get("filename", "")
        trace_id = properties.headers.get("x-trace-id", "") if properties.headers else ""
        
        # Extract document ID from record ID (format: folio|||<doc-id>)
        doc_id = record_id.split("|||")[-1] if "|||" in record_id else filename
        
        # Get page count from attributes
        page_count = 10  # default
        attributes = job.get("attributes", {})
        if "pageCount" in attributes:
            pc_attr = attributes["pageCount"]
            if "values" in pc_attr and len(pc_attr["values"]) > 0:
                page_count = int(pc_attr["values"][0].get("stringResult", 10))
        
        print(f"[shim] Processing doc {doc_id} with {page_count} pages")
        
        # Generate TipTap spec
        spec = generate_tiptap_spec(page_count, doc_id)
        spec_json = json.dumps(spec, ensure_ascii=False)
        
        # Upload to MinIO
        minio = get_minio_client()
        output_filename = f"{doc_id}.tiptap.json"
        
        from io import BytesIO
        spec_bytes = spec_json.encode('utf-8')
        minio.put_object(
            bucket_name,
            output_filename,
            BytesIO(spec_bytes),
            len(spec_bytes),
            content_type="application/json"
        )
        print(f"[shim] Uploaded spec to {bucket_name}/{output_filename}")
        
        # Publish success result
        job_id = str(uuid.uuid4())
        result = {
            "resultType": "specTiptapWorkerResult",
            "traceId": trace_id or doc_id,
            "recordId": record_id,
            "jobId": job_id,
            "timestamp": datetime.utcnow().isoformat(),
            "success": True,
            "confidence": 1,
            "filename": output_filename,
            "bucketName": bucket_name
        }
        
        ch.basic_publish(
            exchange="nldoc.topics",
            routing_key=f"specs.{doc_id}.tiptap",
            body=json.dumps(result),
            properties=pika.BasicProperties(
                content_type="application/json",
                headers=make_headers(trace_id or doc_id)
            )
        )
        print(f"[shim] Published result for {doc_id}")
        
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
    except Exception as e:
        print(f"[shim] Error processing job: {e}")
        import traceback
        traceback.print_exc()
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def main():
    print(f"[shim] Folio-spec shim worker starting as {INSTANCE_NAME}...")
    
    params = pika.URLParameters(AMQP_URL)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    
    # Declare queue and bind to folio-spec jobs
    queue_name = "worker-folio-spec-shim"
    channel.queue_declare(queue=queue_name, durable=True)
    channel.queue_bind(
        exchange="nldoc.topics",
        queue=queue_name,
        routing_key="worker.folio-spec.jobs"
    )
    
    print(f"[shim] Listening on {queue_name} for worker.folio-spec.jobs")
    
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=queue_name, on_message_callback=handle_job)
    
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    
    connection.close()

if __name__ == "__main__":
    main()

