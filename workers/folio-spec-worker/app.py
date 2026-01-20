#!/usr/bin/env python3
"""
Folio-Spec Worker - Converts folio content to NLdoc spec JSON

This worker consumes jobs from station-folio-spec and generates a spec document
that can be converted to HTML/TipTap by downstream workers.

Supports: PDF and DOCX files

Flow:
  folio-spec station → worker.folio-spec.jobs → THIS WORKER → specs.<docId> → station-spec-html → html-writer
"""

import os
import json
import uuid
import pika
import time
from datetime import datetime
from minio import Minio
from io import BytesIO

# PDF text extraction
import fitz  # PyMuPDF

# DOCX text extraction
from docx import Document as DocxDocument
from docx.shared import Pt

# Configuration
AMQP_URL = os.environ.get("AMQP_URL")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_USER = os.environ.get("MINIO_USER", "minio")
MINIO_PASS = os.environ.get("MINIO_PASS", "minio123")

INSTANCE_NAME = f"folio-spec-worker-{uuid.uuid4().hex[:8]}"
WORKER_NAME = "worker-folio-spec"

def get_minio():
    return Minio(MINIO_ENDPOINT, access_key=MINIO_USER, secret_key=MINIO_PASS, secure=False)

def extract_text_from_pdf(minio, bucket_name, filename):
    """Extract structured text from each page of a PDF file"""
    print(f"[worker] Extracting text from {bucket_name}/{filename}")
    
    try:
        # Download PDF from MinIO
        response = minio.get_object(bucket_name, filename)
        pdf_bytes = response.read()
        response.close()
        response.release_conn()
        
        # Open PDF with PyMuPDF
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        
        pages = []
        for page_num in range(len(doc)):
            page = doc[page_num]
            
            # Extract text blocks with position info for structure detection
            blocks = page.get_text("dict", flags=fitz.TEXT_PRESERVE_WHITESPACE)["blocks"]
            
            page_content = []
            for block in blocks:
                if block["type"] == 0:  # Text block
                    for line in block.get("lines", []):
                        line_text = ""
                        font_size = 0
                        is_bold = False
                        
                        for span in line.get("spans", []):
                            text = span.get("text", "")
                            font_size = max(font_size, span.get("size", 12))
                            font_name = span.get("font", "").lower()
                            is_bold = is_bold or "bold" in font_name
                            line_text += text
                        
                        line_text = line_text.strip()
                        if line_text:
                            # Fix encoding issues (Windows-1252 artifacts in UTF-8)
                            line_text = fix_encoding(line_text)
                            
                            # Detect structure based on font size and style
                            if font_size >= 16 or is_bold:
                                page_content.append({"type": "heading", "level": 1 if font_size >= 18 else 2, "text": line_text})
                            else:
                                page_content.append({"type": "paragraph", "text": line_text})
            
            pages.append({
                "page_number": page_num + 1,
                "content": page_content
            })
            print(f"[worker] Page {page_num + 1}: {len(page_content)} blocks")
        
        doc.close()
        return pages
        
    except Exception as e:
        print(f"[worker] Error extracting PDF text: {e}")
        import traceback
        traceback.print_exc()
        return None

def extract_text_from_docx(minio, bucket_name, filename):
    """Extract structured text from a DOCX file using python-docx"""
    print(f"[worker] Extracting text from DOCX: {bucket_name}/{filename}")
    
    try:
        # Download DOCX from MinIO
        response = minio.get_object(bucket_name, filename)
        docx_bytes = response.read()
        response.close()
        response.release_conn()
        
        # Open DOCX with python-docx
        doc = DocxDocument(BytesIO(docx_bytes))
        
        # Single page for DOCX (they don't have pages like PDF)
        content_blocks = []
        
        for para in doc.paragraphs:
            text = para.text.strip()
            if not text:
                continue
            
            # Detect headings based on style
            style_name = para.style.name.lower() if para.style else ""
            
            if "heading" in style_name or "title" in style_name or "kop" in style_name:
                # Extract heading level from style name (Heading 1, Heading 2, etc.)
                level = 1
                for char in style_name:
                    if char.isdigit():
                        level = int(char)
                        break
                content_blocks.append({"type": "heading", "level": level, "text": text})
            elif "list" in style_name or para.text.strip().startswith(('•', '-', '*', '1.', '2.', '3.')):
                # List items treated as paragraphs for now
                content_blocks.append({"type": "paragraph", "text": text})
            else:
                # Check if first run is bold (might be a heading)
                is_bold = False
                if para.runs:
                    first_run = para.runs[0]
                    is_bold = first_run.bold
                    # Check font size
                    if first_run.font.size and first_run.font.size >= Pt(14):
                        is_bold = True
                
                if is_bold and len(text.split()) < 15:
                    content_blocks.append({"type": "heading", "level": 2, "text": text})
                else:
                    content_blocks.append({"type": "paragraph", "text": text})
        
        print(f"[worker] DOCX: {len(content_blocks)} content blocks")
        
        # Return as single "page"
        return [{"page_number": 1, "content": content_blocks}]
        
    except Exception as e:
        print(f"[worker] Error extracting DOCX text: {e}")
        import traceback
        traceback.print_exc()
        return None

def detect_file_type(minio, bucket_name, filename):
    """Detect file type from content (magic bytes)"""
    try:
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
        print(f"[worker] Error detecting file type: {e}")
        return 'unknown'

def fix_encoding(text):
    """Fix common encoding issues from PDF extraction"""
    # Common Windows-1252 to UTF-8 misinterpretations
    replacements = {
        'â€"': '—',  # em-dash
        'â€"': '–',  # en-dash
        'â€™': "'",  # right single quote
        'â€œ': '"',  # left double quote
        'â€': '"',   # right double quote
        'â€¦': '…',  # ellipsis
        'Ã©': 'é',
        'Ã¨': 'è',
        'Ã«': 'ë',
        'Ã¯': 'ï',
        'Ã¶': 'ö',
        'Ã¼': 'ü',
        'Ã ': 'à',
        'ΓÇô': '–',  # en-dash variant
        'ΓÇö': '—',  # em-dash variant
        'ΓÇï': '',   # zero-width space
        'ΓÇ£': '"',  # quote
        'ΓÇª': '…',  # ellipsis
        '\u200b': '', # zero-width space
        '\ufeff': '', # BOM
    }
    
    for old, new in replacements.items():
        text = text.replace(old, new)
    
    return text

def make_headers(trace_id):
    return {
        "x-kimi-worker-instance-name": INSTANCE_NAME,
        "x-kimi-worker-name": WORKER_NAME,
        "x-trace-id": trace_id,
        "timestamp": datetime.utcnow().isoformat()
    }

def extract_page_content(attributes):
    """Extract page content from job attributes"""
    pages = []
    
    # Check if we have content attribute
    content_attr = attributes.get("content", {})
    if "values" in content_attr:
        for value in content_attr["values"]:
            if "stringResult" in value:
                try:
                    page_content = json.loads(value["stringResult"])
                    pages.append(page_content)
                except json.JSONDecodeError:
                    pages.append({"text": value["stringResult"]})
    
    return pages

def generate_spec_from_content(doc_id, page_count, page_contents):
    """Generate NLdoc spec JSON from structured page contents
    
    NLdoc spec format uses https://spec.nldoc.nl/Resource/* types:
    - Document: root element
    - Heading: heading with level (10=h1, 20=h2, 30=h3, etc)
    - Paragraph: paragraph container
    - Text: text content
    """
    
    children = []
    
    if page_contents:
        # Process structured content from PDF
        current_paragraphs = []
        
        for page in page_contents:
            page_num = page.get("page_number", 1)
            content_blocks = page.get("content", [])
            
            for block in content_blocks:
                block_type = block.get("type", "paragraph")
                text = block.get("text", "").strip()
                
                if not text:
                    continue
                
                if block_type == "heading":
                    # Flush any accumulated paragraphs
                    if current_paragraphs:
                        children.append({
                            "id": str(uuid.uuid4()),
                            "type": "https://spec.nldoc.nl/Resource/Paragraph",
                            "children": [{
                                "id": str(uuid.uuid4()),
                                "type": "https://spec.nldoc.nl/Resource/Text",
                                "text": " ".join(current_paragraphs)
                            }]
                        })
                        current_paragraphs = []
                    
                    # Add heading
                    level = block.get("level", 2)
                    heading_level = 10 if level == 1 else 20 if level == 2 else 30
                    children.append({
                        "id": str(uuid.uuid4()),
                        "type": "https://spec.nldoc.nl/Resource/Heading",
                        "level": heading_level,
                        "children": [{
                            "id": str(uuid.uuid4()),
                            "type": "https://spec.nldoc.nl/Resource/Text",
                            "text": text
                        }]
                    })
                else:
                    # Accumulate paragraph text
                    current_paragraphs.append(text)
                    
                    # Flush if we have a reasonable paragraph (ends with punctuation or is long)
                    if text.endswith(('.', '!', '?', ':')) or len(" ".join(current_paragraphs)) > 500:
                        children.append({
                            "id": str(uuid.uuid4()),
                            "type": "https://spec.nldoc.nl/Resource/Paragraph",
                            "children": [{
                                "id": str(uuid.uuid4()),
                                "type": "https://spec.nldoc.nl/Resource/Text",
                                "text": " ".join(current_paragraphs)
                            }]
                        })
                        current_paragraphs = []
        
        # Flush remaining paragraphs
        if current_paragraphs:
            children.append({
                "id": str(uuid.uuid4()),
                "type": "https://spec.nldoc.nl/Resource/Paragraph",
                "children": [{
                    "id": str(uuid.uuid4()),
                    "type": "https://spec.nldoc.nl/Resource/Text",
                    "text": " ".join(current_paragraphs)
                }]
            })
    
    if not children:
        # Fallback: no content available
        children.append({
            "id": str(uuid.uuid4()),
            "type": "https://spec.nldoc.nl/Resource/Paragraph",
            "children": [{
                "id": str(uuid.uuid4()),
                "type": "https://spec.nldoc.nl/Resource/Text",
                "text": f"Dit document bevat {page_count} pagina's maar de tekst kon niet worden geëxtraheerd."
            }]
        })
    
    return {
        "id": str(uuid.uuid4()),
        "type": "https://spec.nldoc.nl/Resource/Document",
        "children": children
    }

def send_worker_result(ch, doc_id, job_id, spec, trace_id, job_metadata):
    """Send worker result back to station-folio-spec"""
    
    now = datetime.utcnow().isoformat() + "Z"
    
    result = {
        "resultType": "fileWorkerResult",
        "traceId": trace_id,
        "recordId": f"folio|||{doc_id}",
        "jobId": job_id,
        "timestamp": now,
        "success": True,
        "bucketName": "files",
        "filename": f"{doc_id}.spec.json"
    }
    
    # Upload spec to MinIO
    minio = get_minio()
    spec_json = json.dumps(spec, ensure_ascii=False)
    spec_bytes = spec_json.encode("utf-8")
    
    # Ensure bucket exists
    if not minio.bucket_exists("files"):
        minio.make_bucket("files")
    
    minio.put_object("files", f"{doc_id}.spec.json", BytesIO(spec_bytes), len(spec_bytes), content_type="application/json")
    print(f"[worker] Uploaded spec to files/{doc_id}.spec.json")
    
    # Publish result to station-folio-spec
    headers = make_headers(trace_id)
    ch.basic_publish(
        exchange="nldoc.topics",
        routing_key=f"worker.folio-spec.results.{job_id}",
        body=json.dumps(result),
        properties=pika.BasicProperties(
            content_type="application/json",
            headers=headers
        )
    )
    print(f"[worker] Published result to worker.folio-spec.results.{job_id}")
    
    # Publish spec attribute message with full document metadata
    # station-spec-html expects these fields for DocumentInfoReport
    # IMPORTANT: filename must point to the spec.json file, NOT the original PDF!
    spec_message = {
        # Document metadata required by station-spec-html
        "documentId": doc_id,
        "bucketName": "files",
        "filename": f"{doc_id}.spec.json",  # Must be the spec file, not original!
        "kimiRegistrationDate": job_metadata.get("kimiRegistrationDate", now),
        "creationDate": job_metadata.get("creationDate", now),
        "processCount": job_metadata.get("processCount", 0),
        "targetFileType": job_metadata.get("targetFileType", "text/html"),
        "inputType": "spec",
        "traceId": trace_id,
        # Attribute info
        "attribute": "spec",
        "values": [{
            "resultType": "fileWorkerResult",
            "traceId": trace_id,
            "recordId": f"spec|||{doc_id}",
            "jobId": job_id,
            "timestamp": now,
            "success": True,
            "bucketName": "files",
            "filename": f"{doc_id}.spec.json"
        }],
        "expectedValues": 1,
        "isComplete": True,
        "bestJobId": job_id
    }
    
    ch.basic_publish(
        exchange="nldoc.topics",
        routing_key=f"specs.{doc_id}",
        body=json.dumps(spec_message),
        properties=pika.BasicProperties(
            content_type="application/json",
            content_encoding="utf8",
            headers=headers,
            delivery_mode=2
        )
    )
    print(f"[worker] Published spec attribute to specs.{doc_id}")

def handle_job(ch, method, properties, body):
    """Process a worker job from folio-spec station"""
    try:
        job = json.loads(body)
        routing_key = method.routing_key
        
        print(f"[worker] Received job on {routing_key}")
        print(f"[worker] Job keys: {list(job.keys())}")
        
        # Extract job info
        record_id = job.get("recordId", "")
        doc_id = record_id.split("|||")[-1] if "|||" in record_id else job.get("filename", "unknown")
        job_id = job.get("jobId", str(uuid.uuid4()))
        bucket_name = job.get("bucketName", "files")
        target_file_type = job.get("targetFileType", "text/html")
        
        # Get trace ID from headers or job
        trace_id = doc_id
        if properties.headers:
            trace_id = properties.headers.get("x-trace-id", doc_id)
        
        print(f"[worker] Processing doc: {doc_id}, target: {target_file_type}")
        
        # Extract attributes
        attributes = job.get("attributes", {})
        
        # Get page count
        page_count = 10  # default
        page_count_attr = attributes.get("pageCount", {})
        if "values" in page_count_attr and len(page_count_attr["values"]) > 0:
            page_count = int(page_count_attr["values"][0].get("stringResult", 10))
        
        print(f"[worker] Page count: {page_count}")
        
        # Extract text from document (PDF or DOCX)
        minio = get_minio()
        original_filename = job.get("filename", doc_id)
        
        # Detect file type
        file_type = detect_file_type(minio, bucket_name, original_filename)
        print(f"[worker] Detected file type: {file_type}")
        
        page_contents = None
        if file_type == 'pdf':
            page_contents = extract_text_from_pdf(minio, bucket_name, original_filename)
        elif file_type == 'docx':
            page_contents = extract_text_from_docx(minio, bucket_name, original_filename)
        else:
            print(f"[worker] Unknown file type, trying PDF first then DOCX")
            page_contents = extract_text_from_pdf(minio, bucket_name, original_filename)
            if not page_contents:
                page_contents = extract_text_from_docx(minio, bucket_name, original_filename)
        
        if page_contents:
            print(f"[worker] Extracted text from {len(page_contents)} pages/sections")
        else:
            print(f"[worker] Could not extract text, using fallback")
            page_contents = []
        
        # Generate spec
        spec = generate_spec_from_content(doc_id, page_count, page_contents)
        
        # Collect job metadata for downstream stations
        job_metadata = {
            "bucketName": bucket_name,
            "filename": job.get("filename", doc_id),
            "targetFileType": target_file_type,
            "kimiRegistrationDate": job.get("kimiRegistrationDate", datetime.utcnow().isoformat() + "Z"),
            "creationDate": job.get("creationDate", datetime.utcnow().isoformat() + "Z"),
            "processCount": job.get("processCount", 0),
        }
        
        # Send result
        send_worker_result(ch, doc_id, job_id, spec, trace_id, job_metadata)
        
        # ACK the message
        ch.basic_ack(delivery_tag=method.delivery_tag)
        print(f"[worker] Completed job for {doc_id}")
        
    except Exception as e:
        print(f"[worker] Error processing job: {e}")
        import traceback
        traceback.print_exc()
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def send_heartbeat(ch):
    """Send worker heartbeat"""
    heartbeat = {
        "workerName": WORKER_NAME,
        "instanceName": INSTANCE_NAME,
        "timestamp": datetime.utcnow().isoformat(),
        "status": "up"
    }
    ch.basic_publish(
        exchange="nldoc.topics",
        routing_key=f"worker.folio-spec.health.heartbeats.{INSTANCE_NAME}",
        body=json.dumps(heartbeat),
        properties=pika.BasicProperties(content_type="application/json")
    )

def main():
    print(f"[worker] Starting {INSTANCE_NAME}...")
    print(f"[worker] AMQP_URL: {AMQP_URL[:50] if AMQP_URL else 'None'}...")
    
    params = pika.URLParameters(AMQP_URL)
    params.heartbeat = 60
    params.blocked_connection_timeout = 300
    
    conn = pika.BlockingConnection(params)
    ch = conn.channel()
    
    # Declare our queue
    ch.queue_declare(queue="worker-folio-spec", durable=True)
    
    # Bind to worker.folio-spec.jobs
    ch.queue_bind(
        exchange="nldoc.topics",
        queue="worker-folio-spec",
        routing_key="worker.folio-spec.jobs"
    )
    
    print("[worker] Bound to worker.folio-spec.jobs")
    
    # Send initial heartbeat
    send_heartbeat(ch)
    
    # Set QoS
    ch.basic_qos(prefetch_count=1)
    
    # Start consuming
    ch.basic_consume(queue="worker-folio-spec", on_message_callback=handle_job)
    
    print("[worker] Waiting for jobs...")
    
    # Heartbeat thread
    import threading
    def heartbeat_loop():
        while True:
            time.sleep(30)
            try:
                send_heartbeat(ch)
            except:
                pass
    
    heartbeat_thread = threading.Thread(target=heartbeat_loop, daemon=True)
    heartbeat_thread.start()
    
    ch.start_consuming()

if __name__ == "__main__":
    main()

