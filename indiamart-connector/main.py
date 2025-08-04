from __future__ import annotations
import os
import io
import json
import math
import re
import sys
import asyncio
from datetime import datetime
from typing import List, Optional, Any, Dict
from contextlib import asynccontextmanager

import aiohttp
import aio_pika
import pandas as pd
import structlog
from fastapi import FastAPI, HTTPException, BackgroundTasks, UploadFile, File, Body, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings
from prometheus_client import Counter, make_asgi_app

# ────────────────────────────────────────────────────────────────────────────────
# Configuration
# ────────────────────────────────────────────────────────────────────────────────
class Settings(BaseSettings):
    rabbitmq_url: str = Field(default="amqp://guest:guest@localhost/")
    INDIAMART_API_KEY: str = Field(default="")
    lead_queue: str = Field(default="lead_queue")
    cors_origins: str = Field(default="http://localhost:3000")  # comma-separated

    class Config:
        env_file = ".env"

settings = Settings()

# ────────────────────────────────────────────────────────────────────────────────
# Logging & Metrics
# ────────────────────────────────────────────────────────────────────────────────
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.stdlib.add_log_level,
        structlog.processors.JSONRenderer(),
    ]
)
log = structlog.get_logger()

# ← Now do your API-key sanity check exactly once
if not settings.INDIAMART_API_KEY:
    log.error("no_indiamart_api_key", msg="INDIAMART_API_KEY is not set or empty")
else:
    masked = settings.INDIAMART_API_KEY[:4] + "…"
    log.info("indiamart_api_key_loaded", masked=masked)

# ────────────────────────────────────────────────────────────────────────────────
# Counters, FastAPI app, etc.
# ────────────────────────────────────────────────────────────────────────────────
REQUEST_COUNTER = Counter(
    "connector_http_requests_total",
    "Total connector HTTP requests",
    ["endpoint", "method", "status"],
)
PUBLISHED_COUNTER = Counter(
    "connector_published_leads_total",
    "Total leads published to RabbitMQ",
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # RabbitMQ robust connection
    log.info("startup_connect_rabbitmq", url=settings.rabbitmq_url)
    connection: Optional[aio_pika.RobustConnection] = None
    try:
        connection = await aio_pika.connect_robust(settings.rabbitmq_url)
        app.state.rabbitmq_connection = connection
        log.info("rabbitmq_connected")
    except Exception as e:
        log.error("rabbitmq_connect_failed", error=str(e))
        app.state.rabbitmq_connection = None

    yield

    conn = app.state.rabbitmq_connection
    if conn and not conn.is_closed:
        await conn.close()
        log.info("rabbitmq_closed")

app = FastAPI(title="IndiaMART Connector", version="0.2.0", lifespan=lifespan)

# CORS
allowed_origins = [o.strip() for o in settings.cors_origins.split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins or ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Prometheus metrics endpoint
app.mount("/metrics", make_asgi_app())

# ────────────────────────────────────────────────────────────────────────────────
# Models
# ────────────────────────────────────────────────────────────────────────────────
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

class PullRequest(BaseModel):
    start_date: str  # YYYY-MM-DD
    end_date: str    # YYYY-MM-DD

class IndiaMartLead(BaseModel):
    UNIQUE_QUERY_ID: Optional[str] = None
    SENDER_NAME: Optional[str] = ""
    SENDER_MOBILE: Optional[str] = ""
    SENDER_EMAIL: Optional[str] = ""
    SENDER_COMPANY: Optional[str] = ""

class WebhookPayload(BaseModel):
    RESPONSE: IndiaMartLead

class LeadIn(BaseModel):
    # Generic enqueue schema (UI / API)
    SENDER_EMAIL: str
    SENDER_NAME: Optional[str] = ""
    SENDER_MOBILE: Optional[str] = ""
    SENDER_COMPANY: Optional[str] = ""

# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────
def norm(v: Any) -> str:
    """Convert any value to a clean string; handle None/NaN/numbers safely."""
    if v is None:
        return ""
    try:
        if isinstance(v, float) and math.isnan(v):
            return ""
    except Exception:
        pass
    # Handle pandas NA values
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return str(v).strip()

def to_indiamart_like(record: Dict[str, Any]) -> Dict[str, str]:
    """
    Accept both IndiaMART field names and CSV headers:
    - IndiaMART: SENDER_NAME, SENDER_EMAIL, SENDER_MOBILE, SENDER_COMPANY
    - CSV/UI:    name, email, phone, company
    Returns a normalized dict using SENDER_* keys.
    """
    name  = norm(record.get("SENDER_NAME")    or record.get("name"))
    email = norm(record.get("SENDER_EMAIL")   or record.get("email")).lower()
    phone = norm(record.get("SENDER_MOBILE")  or record.get("phone"))
    comp  = norm(record.get("SENDER_COMPANY") or record.get("company"))
    return {
        "SENDER_NAME": name,
        "SENDER_EMAIL": email,
        "SENDER_MOBILE": phone,
        "SENDER_COMPANY": comp,
    }

async def publish_leads(connection: Optional[aio_pika.RobustConnection], leads: List[Dict[str, Any]]) -> int:
    """
    Publish leads to RabbitMQ 'lead_queue' as JSON messages.
    Returns number of published messages.
    """
    if not leads:
        return 0

    if not connection or connection.is_closed:
        log.error("publish_no_connection")
        raise HTTPException(status_code=503, detail="Messaging unavailable")

    # Filter/validate emails; skip bad ones
    payloads: List[bytes] = []
    for lead in leads:
        email = norm(lead.get("SENDER_EMAIL", "")).lower()
        if not email or not EMAIL_RE.match(email):
            # skip silently but you can add to errors if desired
            continue
        payloads.append(json.dumps(lead).encode("utf-8"))

    if not payloads:
        return 0

    published = 0
    async with connection.channel() as channel:
        exchange = channel.default_exchange
        for body in payloads:
            await exchange.publish(
                aio_pika.Message(
                    body=body,
                    content_type="application/json",
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                ),
                routing_key=settings.lead_queue,
            )
            published += 1

    PUBLISHED_COUNTER.inc(published)
    log.info("published_batch", count=published)
    return published

async def fetch_indiamart_leads(start_dt: datetime, end_dt: datetime) -> List[dict]:
    s = start_dt.strftime("%d-%b-%Y")
    e = end_dt.strftime("%d-%b-%Y")
    log.info("indiamart_fetch", start=s, end=e)

    # Define the IndiaMART Pull API endpoint and query parameters
    url = "https://mapi.indiamart.com/wservce/crm/crmListing/v2/"
    params = {
        "glusr_crm_key": settings.INDIAMART_API_KEY,
        "start_time":    s,
        "end_time":      e,
    }

    try:
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, params=params) as resp:
                text = await resp.text()
                log.info(
                    "indiamart_raw_response",
                    status=resp.status,
                    body=(text[:1000] + "...") if len(text) > 1000 else text
                )
                resp.raise_for_status()

                data = await resp.json()
                raw = data.get("RESPONSE", [])
                if isinstance(raw, list):
                    return raw
                else:
                    log.warning("indiamart_invalid_response_shape", response=data)
                    return []
    except Exception as ex:
        log.error("indiamart_fetch_failed", error=str(ex))
        return []



async def process_batch_jobs(rmq_url: str, jobs: List[PullRequest]):
    """
    Background task: for each job (date range), call IndiaMART API and publish to queue.
    Spaced by 5 minutes to avoid rate limiting.
    """
    log.info("batch_worker_start", jobs=len(jobs))
    conn: Optional[aio_pika.RobustConnection] = None
    try:
        conn = await aio_pika.connect_robust(rmq_url)
        for idx, job in enumerate(jobs):
            if idx > 0:
                await asyncio.sleep(300)  # 5 minutes pause between pulls

            try:
                sd = datetime.fromisoformat(job.start_date)
                ed = datetime.fromisoformat(job.end_date)
            except Exception:
                log.error("batch_job_invalid_dates", start=job.start_date, end=job.end_date)
                continue

            raw = await fetch_indiamart_leads(sd, ed)
            if not raw:
                log.info("batch_no_leads", start=sd.strftime("%d-%b-%Y"), end=ed.strftime("%d-%b-%Y"))
                continue

            leads = [to_indiamart_like(item) for item in raw]
            # publish
            published = await publish_leads(conn, leads)
            log.info("batch_published", published=published, start=job.start_date, end=job.end_date)

    except Exception as e:
        log.error("batch_worker_failed", error=str(e))
    finally:
        if conn and not conn.is_closed:
            await conn.close()
        log.info("batch_worker_done")

# ────────────────────────────────────────────────────────────────────────────────
# Routes
# ────────────────────────────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    return {"status": "ok"}

@app.post("/leads/enqueue")
async def enqueue_one(lead: LeadIn, request: Request):
    REQUEST_COUNTER.labels(endpoint="/leads/enqueue", method="POST", status="200").inc()
    conn = request.app.state.rabbitmq_connection
    payload = to_indiamart_like(lead.model_dump())
    published = await publish_leads(conn, [payload])
    return {"enqueued": published}

@app.post("/leads/batch")
async def enqueue_batch(leads: List[LeadIn] = Body(...), request: Request = None):
    REQUEST_COUNTER.labels(endpoint="/leads/batch", method="POST", status="200").inc()
    conn = request.app.state.rabbitmq_connection if request else None
    items = [to_indiamart_like(l.model_dump()) for l in leads]
    published = await publish_leads(conn, items)
    return {"enqueued": published}

@app.post("/indiamart/push/")
async def webhook_push(payload: WebhookPayload, request: Request):
    REQUEST_COUNTER.labels(endpoint="/indiamart/push/", method="POST", status="200").inc()
    conn = request.app.state.rabbitmq_connection
    item = payload.RESPONSE.model_dump()
    lead = to_indiamart_like(item)
    published = await publish_leads(conn, [lead])
    return {"status": "success", "enqueued": published}

@app.post("/indiamart/pull/batch", status_code=202)
async def schedule_pull(jobs: List[PullRequest], background_tasks: BackgroundTasks):
    if not jobs:
        raise HTTPException(status_code=400, detail="No jobs provided")
    REQUEST_COUNTER.labels(endpoint="/indiamart/pull/batch", method="POST", status="202").inc()
    background_tasks.add_task(process_batch_jobs, settings.rabbitmq_url, jobs)
    return {"status": "scheduled", "jobs": len(jobs)}

@app.post("/leads/upload", status_code=202)
async def upload_files(
    request: Request,
    files: List[UploadFile] = File(...),
):
    """
    Accepts CSV/Excel/JSON(.txt carrying IndiaMART-like JSON) files and enqueues
    normalized leads into RabbitMQ.

    CSV headers supported: name,email,phone,company
    IndiaMART JSON supported: {"RESPONSE": [ {SENDER_* fields...}, ... ]}
    """
    conn = request.app.state.rabbitmq_connection
    endpoint = "/leads/upload"

    total_rows = 0
    accepted = 0
    errors: List[str] = []

    for file in files:
        try:
            content = await file.read()
            fname = (file.filename or "uploaded").strip()

            # Detect format by extension
            ext = fname.split(".")[-1].lower() if "." in fname else ""
            raw_records: List[Dict[str, Any]]

            if ext in ("txt", "json"):
                # Expecting IndiaMART-like JSON with key RESPONSE
                try:
                    data = json.loads(content)
                except Exception as je:
                    raise HTTPException(status_code=422, detail=f"{fname}: invalid JSON ({je})")
                raw = data.get("RESPONSE", [])
                if not isinstance(raw, list):
                    raise HTTPException(status_code=422, detail=f"{fname}: 'RESPONSE' must be a list")
                raw_records = raw
            elif ext == "csv":
                # Force string dtype and disable default NaN to keep empty strings
                df = await asyncio.to_thread(pd.read_csv, io.BytesIO(content), dtype=str, keep_default_na=False)
                raw_records = df.to_dict(orient="records")
            elif ext in ("xls", "xlsx"):
                df = await asyncio.to_thread(pd.read_excel, io.BytesIO(content), dtype=str, engine="openpyxl")
                df = df.fillna("")
                raw_records = df.to_dict(orient="records")
            else:
                raise HTTPException(status_code=415, detail=f"{fname}: unsupported file type")

            total_rows += len(raw_records)

            leads = []
            for rec in raw_records:
                lead = to_indiamart_like(rec)
                # minimal: require email
                if not lead["SENDER_EMAIL"]:
                    continue
                leads.append(lead)

            if leads:
                published = await publish_leads(conn, leads)
                accepted += published

        except HTTPException:
            raise
        except Exception as e:
            # Capture any parsing/IO errors for this file but continue others
            msg = f"{fname}: {e}"
            errors.append(msg)
            log.error("upload_file_failed", filename=fname, error=str(e))

    status = "success" if not errors and accepted == total_rows else ("partial" if accepted > 0 else "error")
    REQUEST_COUNTER.labels(endpoint=endpoint, method="POST", status=status).inc()
    return {
        "status": status,
        "total_files": len(files),
        "total_rows": total_rows,
        "accepted": accepted,
        "skipped": max(total_rows - accepted, 0),
        "errors": errors,
    }
