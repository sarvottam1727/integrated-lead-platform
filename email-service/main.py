# email_service.py

import os
import asyncio
import base64
import re
from urllib.parse import quote
from email.mime.text import MIMEText

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr
from pydantic_settings import BaseSettings
from google.auth.transport.requests import Request as GoogleRequest
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import structlog
from prometheus_client import Counter, make_asgi_app
from contextlib import asynccontextmanager
from dotenv import load_dotenv

# --- 1. Settings & Logging ---
load_dotenv()
class Settings(BaseSettings):
    credentials_file_path: str = os.getenv("GOOGLE_CREDS_PATH", "credentials.json")
    token_file_path: str       = os.getenv("GOOGLE_TOKEN_PATH",  "token.json")
    google_api_scopes: list[str] = ["https://www.googleapis.com/auth/gmail.send"]
    tracking_service_url: str  = os.getenv("TRACKING_SERVICE_URL", "http://localhost:8007")
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
request_counter = Counter("email_service_requests_total", "Total HTTP requests", ["endpoint", "method"])

# --- 2. Gmail Credential Manager ---
class GmailCredentialManager:
    def __init__(self):
        self._creds: Credentials | None = None
        self._lock = asyncio.Lock()

    async def get_credentials(self) -> Credentials:
        async with self._lock:
            if self._creds and self._creds.valid:
                return self._creds
            self._creds = await asyncio.to_thread(self._load_or_refresh)
            return self._creds

    def _load_or_refresh(self) -> Credentials:
        creds = None
        if os.path.exists(settings.token_file_path):
            creds = Credentials.from_authorized_user_file(
                settings.token_file_path, settings.google_api_scopes
            )
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                logger.info("Refreshing Gmail API token")
                creds.refresh(GoogleRequest())
            else:
                logger.info("Starting new Gmail OAuth flow")
                flow = InstalledAppFlow.from_client_secrets_file(
                    settings.credentials_file_path, settings.google_api_scopes
                )
                creds = flow.run_local_server(port=0)
            with open(settings.token_file_path, "w") as f:
                f.write(creds.to_json())
        return creds

credential_manager = GmailCredentialManager()

# --- 3. send_email_sync with tracking pixel & click wrapping ---
def send_email_sync(creds: Credentials, email_data: "EmailSchema"):
    try:
        service = build("gmail", "v1", credentials=creds)
        # Build HTML body + tracking
        html_body = email_data.body.replace("\n", "<br>")
        # Wrap all href links
        def wrap_link(m):
            orig = m.group(1)
            enc  = quote(orig, safe="")
            return f'href="{settings.tracking_service_url}/track/click?campaign_id={email_data.campaign_id}&contact_id={email_data.contact_id}&redirect_url={enc}"'
        html_body = re.sub(r'href="([^"]+)"', wrap_link, html_body)
        # Append tracking pixel
        pixel = (
            f'<img src="{settings.tracking_service_url}/track/open/'
            f'{email_data.campaign_id}/{email_data.contact_id}" '
            'width="1" height="1" style="display:none;">'
        )
        final_html = html_body + pixel

        # Create message
        msg = MIMEText(final_html, "html", "utf-8")
        msg["To"]      = email_data.to_email
        msg["From"]    = "me"
        msg["Subject"] = email_data.subject
        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()

        sent = service.users().messages().send(userId="me", body={"raw": raw}).execute()
        return sent
    except HttpError as err:
        logger.error("Gmail API error", error=str(err))
        raise

# --- 4. FastAPI app & lifespan ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Preload credentials
    await credential_manager.get_credentials()
    logger.info("Email service started with valid Gmail credentials")
    yield
    logger.info("Email service shutting down")

app = FastAPI(title="Gmail Email Service", version="1.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.mount("/metrics", make_asgi_app())

# --- 5. Pydantic & Endpoints ---
class EmailSchema(BaseModel):
    to_email: EmailStr
    subject: str
    body: str
    campaign_id: int
    contact_id: int

@app.get("/health", tags=["Monitoring"])
async def health():
    request_counter.labels(endpoint="/health", method="GET").inc()
    return {"status": "ok"}

@app.post("/send-email", tags=["Email"])
async def send_email(email: EmailSchema):
    request_counter.labels(endpoint="/send-email", method="POST").inc()
    try:
        creds = await credential_manager.get_credentials()
        result = await asyncio.to_thread(send_email_sync, creds, email)
        logger.info("Email sent", message_id=result.get("id"), recipient=email.to_email)
        return {"status": "success", "message_id": result.get("id")}
    except Exception as e:
        logger.error("Email send failed", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to send email: {e}")
