# campaigns_service.py
# main.py (very top)
import sys, asyncio
if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
import os
import aiohttp
from contextlib import asynccontextmanager
from typing import List, Optional
from datetime import datetime, timezone, timedelta


from fastapi import FastAPI, HTTPException, BackgroundTasks, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, conlist
from psycopg_pool import AsyncConnectionPool
from psycopg.rows import dict_row
from jinja2 import Environment
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv



load_dotenv()

DATABASE_URL     = os.getenv("DATABASE_URL",     "postgresql://user:password@localhost:5432/campaign_db")
EMAIL_SERVICE    = os.getenv("EMAIL_SERVICE_URL","http://localhost:8003")
jinja_env        = Environment()
scheduler        = AsyncIOScheduler()

# --- Models ---
class CampaignCreate(BaseModel):
    name: str
    subject: str
    body_template: str
    contact_ids: conlist(int, min_length=1)

class CampaignSchedule(CampaignCreate):
    scheduled_at: datetime

class CampaignSummary(BaseModel):
    id: int
    name: str
    subject: Optional[str]
    status: str
    successful_sends: int
    failed_sends: int
    created_at: datetime
    opens: int
    clicks: int

class SendLog(BaseModel):
    email: str
    status: str
    error_message: Optional[str]
    timestamp: datetime

class CampaignDetail(BaseModel):
    id: int
    name: str
    subject: Optional[str]
    body_template: Optional[str]
    contact_ids: List[int]
    status: str
    successful_sends: int
    failed_sends: int
    opens: int
    clicks: int
    scheduled_at: Optional[datetime]
    sent_at: Optional[datetime]
    created_at: datetime
    send_logs: List[SendLog]

# --- DB bootstrap ---
CREATE_SQL = """
CREATE TABLE IF NOT EXISTS contacts (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL DEFAULT '',
    email TEXT NOT NULL UNIQUE,
    phone_number TEXT NOT NULL DEFAULT '',
    company_name TEXT NOT NULL DEFAULT '',
    subscribed BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS campaigns (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    subject TEXT,
    body_template TEXT,
    contact_ids INT[] NOT NULL,
    status TEXT NOT NULL DEFAULT 'queued',
    successful_sends INT NOT NULL DEFAULT 0,
    failed_sends INT NOT NULL DEFAULT 0,
    opens INT NOT NULL DEFAULT 0,
    clicks INT NOT NULL DEFAULT 0,
    scheduled_at TIMESTAMPTZ,
    sent_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS email_send_logs (
    id BIGSERIAL PRIMARY KEY,
    campaign_id INT NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
    contact_id INT,
    email TEXT,
    status TEXT NOT NULL,
    error_message TEXT,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

async def init_db(pool: AsyncConnectionPool):
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(CREATE_SQL)
            await conn.commit()

# --- Lifespan ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db_pool = AsyncConnectionPool(conninfo=DATABASE_URL, max_size=10, open=False)
    await app.state.db_pool.open()              # explicit open (avoids deprecation warning)
    await init_db(app.state.db_pool)            # ensure tables exist
    scheduler.start()
    print("▶️ Campaigns Service up: DB pool & scheduler started")
    try:
        yield
    finally:
        scheduler.shutdown()
        await app.state.db_pool.close()
        print("⏹ Campaigns Service down: DB pool & scheduler shut down")

app = FastAPI(title="Enhanced Campaigns Service", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Helpers ---


async def get_contacts_by_ids(pool, ids):
    if not ids:
        return []
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            # Only subscribed contacts are eligible
            await cur.execute(
                "SELECT id,name,email,phone_number,company_name "
                "FROM contacts WHERE subscribed = TRUE AND id = ANY(%s)",
                (ids,)
            )
            return await cur.fetchall()


# --- Background worker ---
async def process_campaign_sending(db_url: str, campaign_id: int):
    print(f"🔄 Worker: start campaign {campaign_id}")
    pool = AsyncConnectionPool(conninfo=db_url, open=False)
    await pool.open()
    try:
        async with pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("SELECT * FROM campaigns WHERE id = %s", (campaign_id,))
                camp = await cur.fetchone()
                if not camp:
                    return

                contacts = await get_contacts_by_ids(pool, camp["contact_ids"])
                await cur.execute("UPDATE campaigns SET status = 'sending' WHERE id = %s", (campaign_id,))
                await conn.commit()

        async with aiohttp.ClientSession() as session:
            tasks = [
                send_and_log_email(pool, session, campaign_id, c, camp["subject"], camp["body_template"])
                for c in contacts
            ]
            results = await asyncio.gather(*tasks)
        success = sum(1 for ok in results if ok)
        failed  = len(results) - success

        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "UPDATE campaigns SET status='sent', sent_at=NOW(), successful_sends=%s, failed_sends=%s WHERE id=%s",
                    (success, failed, campaign_id)
                )
                await conn.commit()
        print(f"✅ Campaign {campaign_id} done: {success} ok, {failed} failed")
    finally:
        await pool.close()

async def send_and_log_email(pool, session, campaign_id, contact, subj, body_tmpl):
    status = "failed"
    err    = None
    try:
        template = jinja_env.from_string(body_tmpl or "")
        body = template.render(contact=contact)
        payload = {
            "to_email": contact["email"],
            "subject":  subj,
            "body":     body,
            "campaign_id": campaign_id,
            "contact_id":  contact["id"],
        }
        async with session.post(f"{EMAIL_SERVICE}/send-email", json=payload, timeout=15) as resp:
            if resp.status == 200:
                status = "success"
            else:
                err = await resp.text()
    except Exception as e:
        err = str(e)

    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO email_send_logs(campaign_id,contact_id,email,status,error_message) "
                "VALUES(%s,%s,%s,%s,%s)",
                (campaign_id, contact["id"], contact["email"], status, err)
            )
            await conn.commit()
    return status == "success"

# --- Endpoints ---
@app.get("/contacts")
async def get_contacts(request: Request, page: int = Query(1, ge=1), per_page: int = Query(50, ge=1, le=100)):
    pool = request.app.state.db_pool
    offset = (page - 1) * per_page
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT id,name,email,phone_number,company_name,subscribed,created_at "
                "FROM contacts ORDER BY id LIMIT %s OFFSET %s",
                (per_page, offset)
            )
            contacts = await cur.fetchall()
            await cur.execute("SELECT COUNT(*) AS count FROM contacts")
            total = (await cur.fetchone())["count"]
    return {"contacts": contacts, "total": total}

@app.post("/campaigns/send-now", status_code=202)
async def send_now(c: CampaignCreate, bg: BackgroundTasks, req: Request):
    pool = req.app.state.db_pool
    contacts = await get_contacts_by_ids(pool, c.contact_ids)
    if not contacts:
        raise HTTPException(404, "No subscribed contacts found.")

    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "INSERT INTO campaigns(name,subject,body_template,contact_ids,status) "
                "VALUES(%s,%s,%s,%s,'queued') RETURNING id",
                (c.name, c.subject, c.body_template, c.contact_ids)
            )
            row = await cur.fetchone()
            await conn.commit()
    camp_id = row["id"]
    bg.add_task(process_campaign_sending, DATABASE_URL, camp_id)
    return {"status": "queued", "campaign_id": camp_id}

@app.post("/campaigns/schedule", status_code=202)
async def schedule(c: CampaignSchedule, req: Request):
    # normalize to UTC and validate
    if c.scheduled_at.tzinfo is None:
        scheduled_utc = c.scheduled_at.replace(tzinfo=timezone.utc)
    else:
        scheduled_utc = c.scheduled_at.astimezone(timezone.utc)

    now_utc = datetime.now(timezone.utc)
    if scheduled_utc <= now_utc:
        raise HTTPException(400, "Scheduled time cannot be in the past.")

    pool = req.app.state.db_pool
    ...
    # when inserting, use scheduled_utc
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "INSERT INTO campaigns(name,subject,body_template,contact_ids,status,scheduled_at) "
                "VALUES(%s,%s,%s,%s,'scheduled',%s) RETURNING id",
                (c.name, c.subject, c.body_template, c.contact_ids, scheduled_utc)
            )
            row = await cur.fetchone()
            camp_id = row["id"]
            await conn.commit()

    # schedule the job with a timezone-aware datetime
    scheduler.add_job(
        process_campaign_sending,
        trigger="date",
        run_date=scheduled_utc,
        args=[DATABASE_URL, camp_id]
    )
    return {"status": "scheduled", "campaign_id": camp_id, "run_at": scheduled_utc}


@app.get("/campaigns", response_model=List[CampaignSummary])
async def list_campaigns(req: Request):
    pool = req.app.state.db_pool
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT id,name,subject,status,successful_sends,failed_sends,created_at,opens,clicks "
                "FROM campaigns ORDER BY created_at DESC"
            )
            return await cur.fetchall()

@app.get("/campaigns/{campaign_id}", response_model=CampaignDetail)
async def get_campaign(campaign_id: int, req: Request):
    pool = req.app.state.db_pool
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("SELECT * FROM campaigns WHERE id = %s", (campaign_id,))
            camp = await cur.fetchone()
            if not camp:
                raise HTTPException(404, "Campaign not found")
            await cur.execute(
                "SELECT email,status,error_message,timestamp FROM email_send_logs WHERE campaign_id=%s ORDER BY timestamp",
                (campaign_id,)
            )
            logs = await cur.fetchall()
    detail = CampaignDetail(**camp, send_logs=logs)
    return detail
