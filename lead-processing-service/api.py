# api.py (top)

from fastapi import FastAPI, HTTPException, UploadFile, File, Body
from pydantic import BaseModel, EmailStr
from typing import List, Optional

import os
import json
import pika
import csv
import io

app = FastAPI(title="Lead Processing API")


# --- config
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_VHOST = os.getenv("RABBITMQ_VHOST", "/")
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE", "lead_queue")

# --- models
class LeadIn(BaseModel):
    SENDER_EMAIL: EmailStr
    SENDER_NAME: Optional[str] = ""
    SENDER_MOBILE: Optional[str] = ""
    SENDER_COMPANY: Optional[str] = ""
    UNIQUE_QUERY_ID: Optional[str] = ""
    CITY: Optional[str] = ""
    STATE: Optional[str] = ""

# --- rabbit helper
def _get_channel():
    cred = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST, port=RABBITMQ_PORT, virtual_host=RABBITMQ_VHOST, credentials=cred
    )
    conn = pika.BlockingConnection(params)
    ch = conn.channel()
    ch.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
    return conn, ch

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/leads/enqueue")
def enqueue_one(lead: LeadIn):  # single object in body
    conn, ch = _get_channel()
    try:
        ch.basic_publish(
            exchange="",
            routing_key=RABBITMQ_QUEUE,
            body=lead.model_dump_json(),
            properties=pika.BasicProperties(delivery_mode=2),
        )
        return {"enqueued": 1}
    finally:
        conn.close()

@app.post("/leads/batch")
def enqueue_batch(leads: List[LeadIn] = Body(...)):  # <-- mark as BODY (expects a JSON array)
    conn, ch = _get_channel()
    try:
        for item in leads:
            ch.basic_publish(
                exchange="",
                routing_key=RABBITMQ_QUEUE,
                body=item.model_dump_json(),
                properties=pika.BasicProperties(delivery_mode=2),
            )
        return {"enqueued": len(leads)}
    finally:
        conn.close()


@app.post("/leads/upload")
async def upload_csv(file: UploadFile = File(...)):
    # Basic content-type check (allow common CSV types)
    if file.content_type not in ("text/csv", "application/vnd.ms-excel", "application/csv", "text/plain"):
        raise HTTPException(status_code=415, detail="Unsupported file type")

    raw = await file.read()
    text = raw.decode("utf-8", errors="ignore")

    # Parse CSV with headers: name,email,phone,company
    try:
        reader = csv.DictReader(io.StringIO(text))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"CSV parse error: {e}")

    leads = []
    errors = []
    line_no = 1  # header line

    for row in reader:
        line_no += 1
        email = (row.get("email") or "").strip()
        name  = (row.get("name") or "").strip()
        phone = (row.get("phone") or "").strip()
        comp  = (row.get("company") or "").strip()
        uqid  = (row.get("unique_query_id") or row.get("UNIQUE_QUERY_ID") or "").strip()
        city  = (row.get("city") or row.get("CITY") or "").strip()
        state = (row.get("state") or row.get("STATE") or "").strip()

        if not email:
            errors.append({"line": line_no, "error": "Missing email"})
            continue

        leads.append({
            "SENDER_EMAIL":   email,
            "SENDER_NAME":    name,
            "SENDER_MOBILE":  phone,
            "SENDER_COMPANY": comp,
            "UNIQUE_QUERY_ID": uqid,
            "CITY":           city,
            "STATE":          state,
        })

    if not leads and errors:
        # All rows invalid
        raise HTTPException(status_code=400, detail={"message":"No valid rows", "errors": errors})

    # Call existing batch handler so logic stays centralized
    result = enqueue_batch([LeadIn(**x) for x in leads])

    return {
        "accepted": result.get("enqueued", len(leads)),
        "skipped": len(errors),
        "errors": errors
    }

