# email_service.py (enhanced, quota-aware, auto-refreshing)

import os
import asyncio
import base64
import re
import json
import time
import random
from collections import deque
from typing import Optional, List, Dict, Any, Tuple
from urllib.parse import quote
from email.mime.text import MIMEText

import hashlib
import sqlite3
import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr, Field
from pydantic_settings import BaseSettings
from google.auth.transport.requests import Request as GoogleRequest
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import structlog
from prometheus_client import Counter, Histogram, make_asgi_app
from contextlib import asynccontextmanager
from dotenv import load_dotenv

# ───────────────────────────────── Settings & Logging ─────────────────────────────────

load_dotenv()


class Settings(BaseSettings):
    credentials_file_path: str = os.getenv("GOOGLE_CREDS_PATH", "credentials.json")
    token_file_path: str = os.getenv("GOOGLE_TOKEN_PATH", "token.json")
    google_api_scopes: list[str] = ["https://www.googleapis.com/auth/gmail.send"]

    # Tracking redirects/pixel
    tracking_service_url: str = os.getenv("TRACKING_SERVICE_URL", "http://localhost:8007")

    # OAuth local server port (only used when a browser flow is needed)
    gmail_oauth_port: int = int(os.getenv("GMAIL_OAUTH_PORT", "8765"))

    # Quota/rate limits (tunable; conservative defaults)
    sends_per_second: int = int(os.getenv("EMAIL_RATE_PER_SECOND", "1"))
    sends_per_minute: int = int(os.getenv("EMAIL_RATE_PER_MINUTE", "60"))

    # Retry/backoff
    max_retries: int = int(os.getenv("EMAIL_SEND_MAX_RETRIES", "5"))
    base_backoff_seconds: float = float(os.getenv("EMAIL_SEND_BASE_BACKOFF", "1.5"))
    backoff_jitter_seconds: float = float(os.getenv("EMAIL_SEND_BACKOFF_JITTER", "0.4"))

    # Concurrency for batch endpoint (kept low by design)
    max_parallel_sends: int = int(os.getenv("EMAIL_MAX_PARALLEL_SENDS", "3"))

    # LLM copy generation
    llm_api_url: str = os.getenv("LLM_API_URL", "https://api.openai.com/v1/chat/completions")
    llm_api_key: str = os.getenv("LLM_API_KEY", "")
    llm_model: str = os.getenv("LLM_MODEL", "gpt-3.5-turbo")

    class Config:
        env_file = ".env"


settings = Settings()

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.stdlib.add_log_level,
        structlog.processors.JSONRenderer(),
    ]
)
logger = structlog.get_logger()

request_counter = Counter(
    "email_service_requests_total", "Total HTTP requests", ["endpoint", "method"]
)
send_counter = Counter(
    "email_service_send_total", "Total emails attempted to send", ["outcome"]
)
send_latency = Histogram(
    "email_service_send_seconds", "Latency for sending an email"
)

# Cache database for AI copy suggestions
DB_PATH = os.getenv("EMAIL_COPY_DB", "email_copy_cache.db")
copy_db = sqlite3.connect(DB_PATH, check_same_thread=False)
copy_db.execute(
    "CREATE TABLE IF NOT EXISTS email_copy_cache (key TEXT PRIMARY KEY, response TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
)
copy_db.commit()

# ───────────────────────────────── Helpers ─────────────────────────────────

def write_text_atomic(path: str, content: str) -> None:
    """Atomic file write: write to temp then replace."""
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(content)
    os.replace(tmp, path)

def now_monotonic() -> float:
    return time.monotonic()

class RateLimiter:
    """
    Simple in-memory limiter for two windows:
      - per-second (N)
      - per-minute (M)
    Uses timestamp deques; await until permits available.
    """
    def __init__(self, per_second: int, per_minute: int):
        self.per_second = max(1, per_second)
        self.per_minute = max(self.per_second, per_minute)
        self._sec = deque()
        self._min = deque()
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            while True:
                t = now_monotonic()
                # drop old entries
                while self._sec and t - self._sec[0] >= 1.0:
                    self._sec.popleft()
                while self._min and t - self._min[0] >= 60.0:
                    self._min.popleft()

                if len(self._sec) < self.per_second and len(self._min) < self.per_minute:
                    self._sec.append(t)
                    self._min.append(t)
                    return

                # compute sleep needed
                sec_wait = (1.0 - (t - self._sec[0])) if self._sec else 0
                min_wait = (60.0 - (t - self._min[0])) if self._min else 0
                to_sleep = max(0.01, min(sec_wait if sec_wait > 0 else 0.01,
                                         min_wait if min_wait > 0 else 0.01))
                await asyncio.sleep(to_sleep)

rate_limiter = RateLimiter(settings.sends_per_second, settings.sends_per_minute)

def is_retryable_http_error(err: HttpError) -> Tuple[bool, Optional[str]]:
    """
    Returns (retryable?, reason) based on Gmail API errors.
    Typical retryable: 429 Too Many Requests, 5xx, 403 rate/quota exceeded.
    """
    try:
        status = err.resp.status if hasattr(err, "resp") and err.resp else None
        reason = None
        if hasattr(err, "error_details") and err.error_details:
            reason = str(err.error_details)
        else:
            try:
                data = json.loads(err.content.decode("utf-8"))
                reason = data.get("error", {}).get("message")
            except Exception:
                reason = str(err)

        if status in (429, 500, 502, 503, 504):
            return True, reason
        if status == 403:
            # 403 can include rate/quota exceeded
            # We'll treat it as retryable unless message clearly says daily limit exceeded.
            msg = (reason or "").lower()
            if any(k in msg for k in ["ratelimit", "rate limit", "user rate limit", "quota", "try again later"]):
                return True, reason
            # daily cap: stop retrying
            if any(k in msg for k in ["daily limit exceeded", "quota exceeded", "dailyLimitExceeded"]):
                return False, reason
        return False, reason
    except Exception:
        return False, None

def backoff_sleep(attempt: int) -> float:
    base = settings.base_backoff_seconds
    jitter = random.uniform(0, settings.backoff_jitter_seconds)
    return (base ** attempt) + jitter

def to_local(dt) -> str:
    try:
        from datetime import datetime
        if isinstance(dt, (int, float)):
            return time.ctime(dt)
        return str(dt) if isinstance(dt, str) else datetime.fromisoformat(dt).isoformat()
    except Exception:
        return str(dt)

# ───────────────────────────────── Gmail Credential Manager ─────────────────────────────────

class GmailCredentialManager:
    """
    Thread-safe credential manager:
    - Loads token.json if present.
    - Refreshes if expired.
    - If refresh fails (invalid_grant), launches OAuth flow to mint a new token, then atomically rewrites token.json.
    """
    def __init__(self):
        self._creds: Optional[Credentials] = None
        self._lock = asyncio.Lock()

    async def get_credentials(self) -> Credentials:
        async with self._lock:
            if self._creds and self._creds.valid:
                return self._creds
            self._creds = await asyncio.to_thread(self._load_or_refresh_safe)
            return self._creds

    def _load_or_refresh_safe(self) -> Credentials:
        from google.auth.exceptions import RefreshError

        creds: Optional[Credentials] = None
        if os.path.exists(settings.token_file_path):
            try:
                creds = Credentials.from_authorized_user_file(
                    settings.token_file_path, settings.google_api_scopes
                )
            except Exception as e:
                logger.warning("Failed to parse token.json, will re-auth", error=str(e))
                creds = None

        try:
            if creds and creds.valid:
                return creds
            if creds and creds.expired and creds.refresh_token:
                logger.info("Refreshing Gmail API token")
                creds.refresh(GoogleRequest())
            else:
                logger.info("Starting new Gmail OAuth flow")
                flow = InstalledAppFlow.from_client_secrets_file(
                    settings.credentials_file_path, settings.google_api_scopes
                )
                creds = flow.run_local_server(port=settings.gmail_oauth_port)

            write_text_atomic(settings.token_file_path, creds.to_json())
            return creds

        except RefreshError as e:
            logger.warning("Token refresh failed; launching OAuth flow", error=str(e))
            flow = InstalledAppFlow.from_client_secrets_file(
                settings.credentials_file_path, settings.google_api_scopes
            )
            creds = flow.run_local_server(port=settings.gmail_oauth_port)
            write_text_atomic(settings.token_file_path, creds.to_json())
            return creds


credential_manager = GmailCredentialManager()

# ───────────────────────────────── Pydantic Models ─────────────────────────────────

class EmailSchema(BaseModel):
    to_email: EmailStr
    subject: str
    body: str
    campaign_id: int
    contact_id: int

class BatchRequest(BaseModel):
    emails: List[EmailSchema] = Field(default_factory=list)

class BatchResult(BaseModel):
    total: int
    succeeded: int
    failed: int
    results: List[Dict[str, Any]]


class CopyRequest(BaseModel):
    prompt: str
    contact: Dict[str, Any] = Field(default_factory=dict)


class CopyResponse(BaseModel):
    text: str

# ───────────────────────────────── LLM Copy Generation ─────────────────────────────────

def _cache_key(prompt: str, contact: Dict[str, Any]) -> str:
    payload = json.dumps({"p": prompt, "c": contact}, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


async def _get_cached_copy(key: str) -> Optional[str]:
    def _inner():
        cur = copy_db.execute("SELECT response FROM email_copy_cache WHERE key=?", (key,))
        row = cur.fetchone()
        return row[0] if row else None

    return await asyncio.to_thread(_inner)


async def _set_cached_copy(key: str, text: str) -> None:
    def _inner():
        copy_db.execute(
            "INSERT OR REPLACE INTO email_copy_cache(key, response) VALUES (?, ?)",
            (key, text),
        )
        copy_db.commit()

    await asyncio.to_thread(_inner)


async def call_llm(prompt: str, contact: Dict[str, Any]) -> str:
    if not settings.llm_api_key:
        raise HTTPException(status_code=500, detail="LLM API key not configured")

    payload = {
        "model": settings.llm_model,
        "messages": [
            {"role": "system", "content": "You are a helpful email copywriting assistant."},
            {"role": "user", "content": f"{prompt}\n\nContact: {json.dumps(contact)}"},
        ],
    }

    headers = {"Authorization": f"Bearer {settings.llm_api_key}"}
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(settings.llm_api_url, json=payload, headers=headers)
        resp.raise_for_status()
        data = resp.json()

    return data.get("choices", [{}])[0].get("message", {}).get("content", "").strip()

# ───────────────────────────────── Gmail Send Logic ─────────────────────────────────

def build_message_html(email_data: EmailSchema) -> str:
    # Basic HTML transform + link wrap + tracking pixel
    html_body = email_data.body.replace("\n", "<br>")
    def wrap_link(m):
        orig = m.group(1)
        enc = quote(orig, safe="")
        return (
            f'href="{settings.tracking_service_url}/track/click'
            f'?campaign_id={email_data.campaign_id}'
            f'&contact_id={email_data.contact_id}'
            f'&redirect_url={enc}"'
        )
    html_body = re.sub(r'href="([^"]+)"', wrap_link, html_body)

    pixel = (
        f'<img src="{settings.tracking_service_url}/track/open/'
        f'{email_data.campaign_id}/{email_data.contact_id}" '
        'width="1" height="1" style="display:none;">'
    )
    return html_body + pixel

def build_raw_message(email_data: EmailSchema) -> str:
    final_html = build_message_html(email_data)
    msg = MIMEText(final_html, "html", "utf-8")
    msg["To"] = email_data.to_email
    msg["From"] = "me"
    msg["Subject"] = email_data.subject
    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    return raw

async def gmail_send_with_retries(creds: Credentials, email_data: EmailSchema) -> Dict[str, Any]:
    """
    Sends a single email respecting:
      - rate limiter (awaits permit),
      - retry/backoff for retryable errors,
      - auto refresh if token expires mid-flight.
    Returns: {"status": "success", "message_id": "..."} or {"status": "error", "error": "..."}
    """
    await rate_limiter.acquire()
    attempt = 0

    while True:
        attempt += 1
        t0 = time.perf_counter()
        try:
            # Ensure creds are valid (refresh if needed)
            if not creds.valid and creds.refresh_token:
                logger.info("Refreshing token before send")
                creds.refresh(GoogleRequest())
                write_text_atomic(settings.token_file_path, creds.to_json())

            service = build("gmail", "v1", credentials=creds, cache_discovery=False)
            raw = build_raw_message(email_data)
            sent = service.users().messages().send(userId="me", body={"raw": raw}).execute()

            send_latency.observe(time.perf_counter() - t0)
            send_counter.labels(outcome="success").inc()
            return {"status": "success", "message_id": sent.get("id")}

        except HttpError as err:
            send_latency.observe(time.perf_counter() - t0)
            retryable, reason = is_retryable_http_error(err)
            msg = reason or str(err)
            logger.warning("Gmail API HttpError", attempt=attempt, retryable=retryable, reason=msg)

            # Token issues: try a fresh flow if needed
            try:
                # Frequently err.resp.status==401 indicates token invalid
                status = getattr(err.resp, "status", None)
                if status in (401,):
                    logger.info("Attempting credential refresh due to 401")
                    creds.refresh(GoogleRequest())
                    write_text_atomic(settings.token_file_path, creds.to_json())
                    # retry immediately (without counting as attempt backoff)
                    if attempt <= settings.max_retries + 1:
                        await asyncio.sleep(0.1)
                        continue
            except Exception as e:
                logger.warning("Refresh on 401 failed; will treat as retryable and backoff", error=str(e))

            # If daily limit exceeded, stop retrying and return error
            text = (msg or "").lower()
            if any(k in text for k in ["daily limit exceeded", "quota exceeded", "dailylimitexceeded"]):
                send_counter.labels(outcome="quota_exceeded").inc()
                return {"status": "error", "error": "Daily quota exceeded. Try later.", "detail": msg}

            if retryable and attempt <= settings.max_retries:
                sleep_s = backoff_sleep(attempt)
                await asyncio.sleep(sleep_s)
                continue

            send_counter.labels(outcome="error").inc()
            return {"status": "error", "error": msg}

        except Exception as e:
            send_latency.observe(time.perf_counter() - t0)
            logger.error("Unexpected send error", error=str(e), attempt=attempt)
            if attempt <= settings.max_retries:
                sleep_s = backoff_sleep(attempt)
                await asyncio.sleep(sleep_s)
                continue
            send_counter.labels(outcome="error").inc()
            return {"status": "error", "error": str(e)}

# ───────────────────────────────── FastAPI App ─────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Best-effort warm up: don’t crash if auth needs user interaction.
    try:
        await credential_manager.get_credentials()
        logger.info("Email service started with valid Gmail credentials")
    except Exception as e:
        logger.warning("Email service started WITHOUT Gmail credentials; call /auth/init", error=str(e))
    yield
    logger.info("Email service shutting down")

app = FastAPI(title="Gmail Email Service", version="2.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/metrics", make_asgi_app())

# ───────────────────────────────── Endpoints ─────────────────────────────────

@app.get("/health", tags=["Monitoring"])
async def health():
    request_counter.labels(endpoint="/health", method="GET").inc()
    return {"status": "ok"}

@app.get("/auth/status", tags=["Auth"])
async def auth_status():
    request_counter.labels(endpoint="/auth/status", method="GET").inc()
    try:
        creds = await credential_manager.get_credentials()
        return {"valid": bool(creds and creds.valid)}
    except Exception as e:
        return {"valid": False, "error": str(e)}

@app.post("/auth/init", tags=["Auth"])
async def auth_init():
    """
    Forces credential availability now (will open browser once).
    """
    request_counter.labels(endpoint="/auth/init", method="POST").inc()
    try:
        creds = await credential_manager.get_credentials()
        return {"status": "ok", "valid": bool(creds and creds.valid)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Auth init failed: {e}")

@app.post("/auth/reset", tags=["Auth"])
async def auth_reset():
    """
    Deletes token.json so next auth will re-prompt (useful if changing accounts).
    """
    request_counter.labels(endpoint="/auth/reset", method="POST").inc()
    try:
        if os.path.exists(settings.token_file_path):
            os.remove(settings.token_file_path)
        # Next get_credentials() will run the flow
        return {"status": "ok", "message": "Token cleared"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Reset failed: {e}")


@app.post("/generate-copy", response_model=CopyResponse, tags=["AI"])
async def generate_copy(req: CopyRequest):
    """Return suggested email text from an LLM, caching responses."""
    request_counter.labels(endpoint="/generate-copy", method="POST").inc()
    key = _cache_key(req.prompt, req.contact)
    cached = await _get_cached_copy(key)
    if cached:
        return {"text": cached}

    suggestion = await call_llm(req.prompt, req.contact)
    await _set_cached_copy(key, suggestion)
    return {"text": suggestion}

@app.post("/send-email", tags=["Email"])
async def send_email(email: EmailSchema):
    """
    Sends a single email with tracking + link wrapping.
    Rate limited + retry/backoff under the hood.
    """
    request_counter.labels(endpoint="/send-email", method="POST").inc()
    try:
        creds = await credential_manager.get_credentials()
        result = await gmail_send_with_retries(creds, email)
        if result.get("status") == "success":
            logger.info("Email sent", message_id=result.get("message_id"), recipient=email.to_email)
            return result
        else:
            raise HTTPException(status_code=429, detail=result)
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Email send failed", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to send email: {e}")

@app.post("/send-batch", response_model=BatchResult, tags=["Email"])
async def send_batch(payload: BatchRequest):
    """
    Sends a list of emails, gently respecting quotas:
    - global rate limiter (1/s, 60/min by default),
    - capped parallelism,
    - retries with backoff for transient errors.
    Returns per-item results; does not hard-fail the whole batch.
    """
    request_counter.labels(endpoint="/send-batch", method="POST").inc()
    emails = payload.emails or []
    if not emails:
        return BatchResult(total=0, succeeded=0, failed=0, results=[])

    try:
        creds = await credential_manager.get_credentials()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Auth not available: {e}")

    sem = asyncio.Semaphore(settings.max_parallel_sends)

    async def _send_one(e: EmailSchema):
        async with sem:
            return await gmail_send_with_retries(creds, e)

    results = []
    # Run in small waves to avoid spikes
    chunk_size = max(1, settings.max_parallel_sends * 2)
    for i in range(0, len(emails), chunk_size):
        chunk = emails[i : i + chunk_size]
        results.extend(await asyncio.gather(*[ _send_one(e) for e in chunk ]))

    succeeded = sum(1 for r in results if r.get("status") == "success")
    failed = len(results) - succeeded
    return BatchResult(total=len(emails), succeeded=succeeded, failed=failed, results=results)
