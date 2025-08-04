# tracking_service.py

import os
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import FileResponse, RedirectResponse
from contextlib import asynccontextmanager
import asyncpg
from dotenv import load_dotenv

# --- Load environment and DB ---
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@localhost:5432/campaign_db")
PIXEL_PATH = "pixel.gif"

# Create a 1×1 transparent GIF if it doesn't exist
if not os.path.exists(PIXEL_PATH):
    with open(PIXEL_PATH, "wb") as f:
        f.write(
            b"\x47\x49\x46\x38\x39\x61"  # GIF89a
            b"\x01\x00\x01\x00"          # 1×1 pixel
            b"\x80\x00\x00"              # palette follows
            b"\xff\xff\xff\x00\x00\x00"  # white & black
            b"\x21\xf9\x04\x01\x00\x00\x00\x00"  # Graphics Control
            b"\x2c\x00\x00\x00\x00\x01\x00\x01\x00\x00"  # Image Descriptor
            b"\x02\x02\x44\x01\x00\x3b"  # image data + trailer
        )

# --- FastAPI with lifespan for DB pool ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db_pool = await asyncpg.create_pool(dsn=DATABASE_URL, max_size=10)
    yield
    await app.state.db_pool.close()

app = FastAPI(title="Email Tracking Service", lifespan=lifespan)

# --- 1×1 pixel for opens ---
@app.get("/track/open/{campaign_id}/{contact_id}")
async def track_open(campaign_id: int, contact_id: int, request: Request):
    pool = request.app.state.db_pool
    async with pool.acquire() as conn:
        async with conn.transaction():
            # increment opens count
            await conn.execute(
                "UPDATE campaigns SET opens = opens + 1 WHERE id = $1",
                campaign_id
            )
            # log the open event
            await conn.execute(
                """
                INSERT INTO email_send_logs(campaign_id, contact_id, email, status, error_message)
                SELECT $1, $2, email, 'opened', NULL
                FROM contacts
                WHERE id = $2
                """,
                campaign_id,
                contact_id
            )
    return FileResponse(PIXEL_PATH, media_type="image/gif")

# --- redirect for clicks ---
@app.get("/track/click")
async def track_click(
    request: Request,
    campaign_id: int,
    contact_id: int,
    redirect_url: str
):
    pool = request.app.state.db_pool
    async with pool.acquire() as conn:
        async with conn.transaction():
            # increment click count
            await conn.execute(
                "UPDATE campaigns SET clicks = clicks + 1 WHERE id = $1",
                campaign_id
            )
            # log the click event
            await conn.execute(
                """
                INSERT INTO email_send_logs(campaign_id, contact_id, email, status, error_message)
                SELECT $1, $2, email, 'clicked', NULL
                FROM contacts
                WHERE id = $2
                """,
                campaign_id,
                contact_id
            )
    # redirect to the original URL
    return RedirectResponse(url=redirect_url)

# --- health endpoint ---
@app.get("/health")
async def health():
    return {"status": "healthy"}
