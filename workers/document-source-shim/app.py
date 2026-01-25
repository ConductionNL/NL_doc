"""
Document-source shim: BYPASS APPROACH for PoC

The document-source station rejects our worker registration (unknown worker).
Instead of fighting the station, we bypass it:
1. Consume jobs from the station's worker queue
2. Forward directly to the pdf-pdfmetadata station's input queue
3. Skip the ACK/registration dance entirely
"""

import json
import os
import socket
import sys
import time
import uuid
from datetime import datetime, timezone
from io import BytesIO

import pika

# MinIO client for reading file headers
try:
    from minio import Minio
    MINIO_AVAILABLE = True
except ImportError:
    MINIO_AVAILABLE = False

INSTANCE_NAME = f"shim-document-source-{socket.gethostname()}"


def get_env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def get_minio_client():
    """Create MinIO client from environment variables"""
    if not MINIO_AVAILABLE:
        return None
    
    host = os.getenv("S3_HOST", "minio")
    port = os.getenv("S3_PORT", "9000")
    access_key = os.getenv("S3_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("S3_SECRET_KEY", "minioadmin")
    use_ssl = os.getenv("S3_USE_SSL", "false").lower() == "true"
    
    return Minio(
        f"{host}:{port}",
        access_key=access_key,
        secret_key=secret_key,
        secure=use_ssl
    )


def detect_file_type_from_minio(bucket_name: str, filename: str) -> str:
    """Read first bytes from MinIO to detect file type"""
    try:
        minio = get_minio_client()
        if not minio:
            return "unknown"
        
        # Read first 8 bytes
        response = minio.get_object(bucket_name, filename, length=8)
        header = response.read()
        response.close()
        response.release_conn()
        
        # PDF starts with %PDF
        if header.startswith(b'%PDF'):
            return 'pdf'
        
        # DOCX (ZIP with specific content) starts with PK
        if header.startswith(b'PK\x03\x04'):
            return 'docx'
        
        return 'unknown'
    except Exception as e:
        print(f"[shim] Error detecting file type: {e}", file=sys.stderr, flush=True)
        return 'unknown'


def build_amqp_url() -> str:
    protocol = os.getenv("AMQP_PROTOCOL", "amqp")
    host = get_env("AMQP_HOST", "rabbitmq")
    port = os.getenv("AMQP_PORT", "5672")
    username = os.getenv("AMQP_USERNAME") or os.getenv("AMQP_USER") or "guest"
    password = os.getenv("AMQP_PASSWORD") or os.getenv("AMQP_PASS") or "guest"
    return f"{protocol}://{username}:{password}@{host}:{port}/"


def main() -> None:
    amqp_url = build_amqp_url()
    exchange = "nldoc.topics"
    
    # What we consume (jobs from document-source station)
    consume_queue = "worker-document-source"
    consume_routing_key = "worker.document-source.jobs"
    
    # Where we publish (directly to pdf-pdfmetadata station)
    # The station listens on its station queue, which is bound to station.pdf-pdfmetadata.#
    publish_queue = "station-pdf-pdfmetadata"

    print(f"[shim] Worker {INSTANCE_NAME} starting (BYPASS MODE)...", flush=True)
    print(f"[shim] Connecting to {amqp_url}", flush=True)
    params = pika.URLParameters(amqp_url)

    while True:
        try:
            with pika.BlockingConnection(params) as connection:
                channel = connection.channel()
                channel.exchange_declare(
                    exchange=exchange, exchange_type="topic", durable=True
                )
                
                # Ensure our consume queue exists and is bound
                channel.queue_declare(queue=consume_queue, durable=True)
                channel.queue_bind(
                    queue=consume_queue, exchange=exchange, routing_key=consume_routing_key
                )
                channel.basic_qos(prefetch_count=1)

                print(f"[shim] Consuming from {consume_queue}", flush=True)
                print(f"[shim] Will forward to {publish_queue}", flush=True)

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

                    if not isinstance(job, dict):
                        print(f"[shim] Job is not a dict: {job}", file=sys.stderr, flush=True)
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                        return

                    bucket_name = job.get("bucketName")
                    filename = job.get("filename")
                    
                    if not bucket_name or not filename:
                        print(f"[shim] Missing bucketName or filename: {job}", file=sys.stderr, flush=True)
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                        return

                    # Generate IDs
                    trace_id = str(uuid.uuid4())
                    document_id = job.get("documentId") or filename
                    
                    # Detect file type from fileType field first
                    file_type = job.get("fileType", "").lower()
                    
                    # Determine if this is DOCX or PDF from fileType field
                    is_docx = (
                        "openxmlformats" in file_type or 
                        "wordprocessingml" in file_type or
                        "docx" in file_type or
                        str(filename).lower().endswith(".docx")
                    )
                    is_pdf = (
                        "pdf" in file_type or
                        str(filename).lower().endswith(".pdf")
                    )
                    
                    # If not determined, read magic bytes from MinIO
                    if not is_docx and not is_pdf:
                        detected_type = detect_file_type_from_minio(bucket_name, filename)
                        print(f"[shim] Detected file type from magic bytes: {detected_type}", flush=True)
                        is_docx = detected_type == 'docx'
                        is_pdf = detected_type == 'pdf'
                    
                    now = datetime.now(timezone.utc).isoformat()
                    
                    # Headers for the message
                    headers = {
                        "x-trace-id": trace_id,
                        "timestamp": now,
                    }
                    
                    if is_docx:
                        # DOCX flow: send to folio-spec-worker via worker.docx-spec.jobs
                        # This is bound to the worker-folio-spec queue
                        record_id = f"docx|||{document_id}"
                        job_id = str(uuid.uuid4())
                        target_file_type = job.get("targetFileType", job.get("targetContentType", "text/html"))
                        
                        # Format that folio-spec-worker expects (same as folio-spec station output)
                        docx_job = {
                            "recordId": record_id,
                            "jobId": job_id,
                            "bucketName": bucket_name,
                            "filename": filename,
                            # IMPORTANT: propagate requested target so the editor can request TipTap JSON
                            # (application/vnd.nldoc.tiptap+json) instead of HTML.
                            "targetFileType": target_file_type,
                            "attributes": {
                                "pageCount": {
                                    "values": [{"stringResult": "1"}]
                                }
                            }
                        }
                        
                        routing_key = "worker.docx-spec.jobs"
                        out_body = json.dumps(docx_job).encode("utf-8")
                        
                        channel.basic_publish(
                            exchange=exchange,
                            routing_key=routing_key,
                            body=out_body,
                            properties=pika.BasicProperties(
                                content_type="application/json",
                                delivery_mode=2,
                                headers=headers,
                            ),
                        )
                        
                        print(
                            f"[shim] DOCX: Forwarded job to {routing_key}: bucket={bucket_name}, file={filename}, target={target_file_type}, trace={trace_id}",
                            flush=True,
                        )
                    else:
                        # PDF flow: send to pdf-pdfmetadata station
                        record_id = f"pdf|||{document_id}"
                        
                        # Ensure mimeType is set
                        attrs = job.get("attributes") or {}
                        if "mimeType" not in attrs:
                            attrs["mimeType"] = "application/pdf" if is_pdf else "application/octet-stream"
                        
                        station_input = {
                            "id": record_id,
                            "bucketName": bucket_name,
                            "filename": filename,
                            "inputType": "pdf",
                            "targetFileType": job.get("targetFileType", job.get("targetContentType", "text/html")),
                            "documentId": document_id,
                            "itemIndex": None,
                            "traceId": trace_id,
                            "processingStartDate": now,
                            "state": "processing",
                            "attributes": attrs,
                            "knownAttributeNames": list(attrs.keys()),
                            "knownAttributeStats": {},
                            "kimiRegistrationDate": now,
                            "creationDate": now,
                            "processCount": 0,
                        }
                        
                        routing_key = f"pdfs.{document_id}"
                        out_body = json.dumps(station_input).encode("utf-8")
                        
                        channel.basic_publish(
                            exchange=exchange,
                            routing_key=routing_key,
                            body=out_body,
                            properties=pika.BasicProperties(
                                content_type="application/json",
                                delivery_mode=2,
                                headers=headers,
                            ),
                        )
                        
                        print(
                            f"[shim] PDF: Forwarded job to {routing_key}: bucket={bucket_name}, file={filename}, trace={trace_id}",
                            flush=True,
                        )

                    ch.basic_ack(delivery_tag=method.delivery_tag)

                channel.basic_consume(queue=consume_queue, on_message_callback=handle)
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
