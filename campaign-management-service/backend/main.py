"""
Campaign-Management Service
Run: uvicorn main:create_app --host 0.0.0.0 --port 8005 --factory
Env:
  DATABASE_URL=postgresql://campaign_user:securepassword@localhost:5432/campaign_db
  CONTACTS_DATABASE_URL=postgresql://sai_user:***@localhost/sai_unisonic_db   (optional)
  EMAIL_SERVICE_URL=http://localhost:8003
"""

# 1) Windows-safe policy BEFORE anything touches asyncio
import os, sys, asyncio
if os.name == "nt":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# 2) Imports
import aiohttp
from contextlib import asynccontextmanager
from typing import List, Optional, Dict
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, BackgroundTasks, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, conlist
from psycopg_pool import AsyncConnectionPool
from psycopg.rows import dict_row
from jinja2 import Environment
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv

# 3) Environment
load_dotenv()
DATABASE_URL            = os.getenv("DATABASE_URL", "postgresql://campaign_user:securepassword@localhost:5432/campaign_db")
CONTACTS_DATABASE_URL   = os.getenv("CONTACTS_DATABASE_URL", "").strip() or None   # <- NEW
EMAIL_SERVICE           = os.getenv("EMAIL_SERVICE_URL", "http://localhost:8003")

# 4) Globals
jinja_env = Environment()
scheduler = AsyncIOScheduler()

# 5) Models
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

# 6) Bootstrap SQL (safe)
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

# 7) Detect subscription mode for a contacts table in a given pool
async def detect_subscription_mode(pool: AsyncConnectionPool) -> str:
    """
    Returns 'subscribed' (eligible when TRUE) or 'dnc' (eligible when FALSE).
    Raises if neither column exists.
    """
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'contacts'
            """)
            cols = {row["column_name"] for row in await cur.fetchall()}
    if "subscribed" in cols:
        return "subscribed"
    if "dnc" in cols:
        return "dnc"
    raise RuntimeError(
        "contacts table has neither 'subscribed' nor 'dnc' column in this database."
    )

# 8) Fetch contacts by ids from a single pool with a given mode
async def fetch_contacts_from_pool(pool: AsyncConnectionPool, ids: List[int], mode: str) -> List[dict]:
    if not ids:
        return []
    where_clause = "subscribed = TRUE" if mode == "subscribed" else "dnc = FALSE"
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                SELECT id, name, email, phone_number, company_name
                FROM contacts
                WHERE {where_clause} AND id = ANY(%s)
                """,
                (ids,),
            )
            return await cur.fetchall()

# 9) Combine across primary + optional secondary contacts DB (dedupe by id)
async def get_contacts_across_sources(app, ids: List[int]) -> List[dict]:
    """
    Tries primary (campaign DB) contacts table, then optional secondary
    contacts DB if configured. Returns unique contacts by id.
    """
    result_by_id: Dict[int, dict] = {}

    # primary
    prim_pool = app.state.db_pool
    prim_mode = app.state.subscription_mode_primary
    primary_contacts = await fetch_contacts_from_pool(prim_pool, ids, prim_mode)
    for c in primary_contacts:
        result_by_id[c["id"]] = c

    # secondary (optional)
    sec_pool = getattr(app.state, "contacts_pool_secondary", None)
    if sec_pool:
        sec_mode = app.state.subscription_mode_secondary
        remaining = [i for i in ids if i not in result_by_id]
        if remaining:
            secondary_contacts = await fetch_contacts_from_pool(sec_pool, remaining, sec_mode)
            for c in secondary_contacts:
                result_by_id[c["id"]] = c

    # preserve input order
    return [result_by_id[i] for i in ids if i in result_by_id]

# 10) Email worker helpers
async def send_and_log_email(pool, session, campaign_id, contact, subj, body_tmpl):
    status, err = "failed", None
    try:
        body = jinja_env.from_string(body_tmpl or "").render(contact=contact)
        payload = {
            "to_email":   contact["email"],
            "subject":    subj,
            "body":       body,
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

async def process_campaign_sending(campaign_db_url: str, contacts_db_url: Optional[str], campaign_id: int):
    print(f"🔄  Worker: start campaign {campaign_id}")
    camp_pool = AsyncConnectionPool(conninfo=campaign_db_url, open=False)
    await camp_pool.open()
    sec_pool = None
    try:
        # detect modes
        mode_primary = await detect_subscription_mode(camp_pool)
        if contacts_db_url and contacts_db_url != campaign_db_url:
            sec_pool = AsyncConnectionPool(conninfo=contacts_db_url, open=False)
            await sec_pool.open()
            mode_secondary = await detect_subscription_mode(sec_pool)
        else:
            mode_secondary = None

        async with camp_pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("SELECT * FROM campaigns WHERE id = %s", (campaign_id,))
                camp = await cur.fetchone()
                if not camp:
                    return
                # gather contacts from both sources
                ids = camp["contact_ids"]
                contacts_all: Dict[int, dict] = {}

                # primary
                for c in await fetch_contacts_from_pool(camp_pool, ids, mode_primary):
                    contacts_all[c["id"]] = c
                # secondary
                if sec_pool and mode_secondary:
                    remaining = [i for i in ids if i not in contacts_all]
                    if remaining:
                        for c in await fetch_contacts_from_pool(sec_pool, remaining, mode_secondary):
                            contacts_all[c["id"]] = c

                contacts = [contacts_all[i] for i in ids if i in contacts_all]

                await cur.execute("UPDATE campaigns SET status = 'sending' WHERE id = %s", (campaign_id,))
                await conn.commit()

        if not contacts:
            async with camp_pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "UPDATE campaigns SET status='sent', sent_at=NOW(), successful_sends=0, failed_sends=0 WHERE id=%s",
                        (campaign_id,)
                    )
                    await conn.commit()
            print(f"ℹ️  Campaign {campaign_id}: no eligible contacts across sources.")
            return

        async with aiohttp.ClientSession() as session:
            results = await asyncio.gather(
                *[send_and_log_email(camp_pool, session, campaign_id, c, camp["subject"], camp["body_template"])
                  for c in contacts]
            )

        success = sum(1 for r in results if r)
        failed  = len(results) - success

        async with camp_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "UPDATE campaigns SET status='sent', sent_at=NOW(), successful_sends=%s, failed_sends=%s WHERE id=%s",
                    (success, failed, campaign_id)
                )
                await conn.commit()
        print(f"✅  Campaign {campaign_id} done: {success} ok, {failed} failed")
    finally:
        if sec_pool:
            await sec_pool.close()
        await camp_pool.close()

# 11) App factory & lifespan
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Campaign DB pool (also primary contacts if you want)
    app.state.db_pool = AsyncConnectionPool(conninfo=DATABASE_URL, max_size=10, open=False)
    await app.state.db_pool.open()
    await init_db(app.state.db_pool)

    # Detect primary mode
    app.state.subscription_mode_primary = await detect_subscription_mode(app.state.db_pool)

    # Optional secondary contacts DB
    if CONTACTS_DATABASE_URL and CONTACTS_DATABASE_URL != DATABASE_URL:
        app.state.contacts_pool_secondary = AsyncConnectionPool(conninfo=CONTACTS_DATABASE_URL, max_size=10, open=False)
        await app.state.contacts_pool_secondary.open()
        app.state.subscription_mode_secondary = await detect_subscription_mode(app.state.contacts_pool_secondary)
        print(f"ℹ️  Eligibility primary: {app.state.subscription_mode_primary} (campaign DB)")
        print(f"ℹ️  Eligibility secondary: {app.state.subscription_mode_secondary} (contacts DB)")
    else:
        app.state.contacts_pool_secondary = None
        app.state.subscription_mode_secondary = None
        print(f"ℹ️  Eligibility mode: {app.state.subscription_mode_primary} (campaign DB only)")

    scheduler.start()
    print("▶️  Campaigns Service up: DB pool & scheduler started")
    try:
        yield
    finally:
        scheduler.shutdown()
        if app.state.contacts_pool_secondary:
            await app.state.contacts_pool_secondary.close()
        await app.state.db_pool.close()
        print("⏹  Campaigns Service down: DB pool & scheduler shut down")

def create_app() -> FastAPI:
    app = FastAPI(title="Enhanced Campaigns Service", lifespan=lifespan)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:3000"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/contacts", summary="Get Contacts (from primary DB)")
    async def get_contacts(request: Request,
                           page: int = Query(1, ge=1),
                           per_page: int = Query(50, ge=1, le=100)):
        pool = request.app.state.db_pool
        mode = request.app.state.subscription_mode_primary
        offset = (page - 1) * per_page
        eligibility_expr = "subscribed" if mode == "subscribed" else "NOT dnc"
        async with pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    f"""
                    SELECT id, name, email, phone_number, company_name,
                           {eligibility_expr} AS subscribed,
                           created_at
                    FROM contacts
                    ORDER BY id
                    LIMIT %s OFFSET %s
                    """,
                    (per_page, offset)
                )
                contacts = await cur.fetchall()
                await cur.execute("SELECT COUNT(*) AS count FROM contacts")
                total = (await cur.fetchone())["count"]
        return {"contacts": contacts, "total": total}

    @app.post("/campaigns/send-now", status_code=202, summary="Send Now")
    async def send_now(c: CampaignCreate, bg: BackgroundTasks, req: Request):
        ids = c.contact_ids
        print(f"[send-now] ids={ids}")

        contacts = await get_contacts_across_sources(req.app, ids)
        if not contacts:
            raise HTTPException(
                status_code=404,
                detail="No eligible contacts across configured sources. "
                       "IDs may not exist or are unsubscribed/DNC."
            )

        pool = req.app.state.db_pool
        async with pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    "INSERT INTO campaigns(name,subject,body_template,contact_ids,status) "
                    "VALUES(%s,%s,%s,%s,'queued') RETURNING id",
                    (c.name, c.subject, c.body_template, c.contact_ids)
                )
                camp_id = (await cur.fetchone())["id"]
                await conn.commit()

        print(f"[send-now] matched={len(contacts)} campaign_id={camp_id}")
        bg.add_task(process_campaign_sending, DATABASE_URL, CONTACTS_DATABASE_URL, camp_id)
        return {"status": "queued", "campaign_id": camp_id}

    # Alias for older UIs
    @app.post("/campaigns/send", status_code=202, summary="Send Now (alias)")
    async def send_alias(payload: CampaignCreate, bg: BackgroundTasks, req: Request):
        return await send_now(payload, bg, req)

    @app.post("/campaigns/schedule", status_code=202, summary="Schedule")
    async def schedule(c: CampaignSchedule, req: Request):
        sched_utc = c.scheduled_at.replace(tzinfo=timezone.utc) if c.scheduled_at.tzinfo is None else c.scheduled_at.astimezone(timezone.utc)
        if sched_utc <= datetime.now(timezone.utc):
            raise HTTPException(400, "Scheduled time cannot be in the past.")

        pool = req.app.state.db_pool
        async with pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    "INSERT INTO campaigns(name,subject,body_template,contact_ids,status,scheduled_at) "
                    "VALUES(%s,%s,%s,%s,'scheduled',%s) RETURNING id",
                    (c.name, c.subject, c.body_template, c.contact_ids, sched_utc)
                )
                camp_id = (await cur.fetchone())["id"]
                await conn.commit()

        scheduler.add_job(
            process_campaign_sending,
            trigger="date",
            run_date=sched_utc,
            args=[DATABASE_URL, CONTACTS_DATABASE_URL, camp_id]
        )
        return {"status": "scheduled", "campaign_id": camp_id, "run_at": sched_utc}

    @app.get("/campaigns", response_model=List[CampaignSummary], summary="List Campaigns")
    async def list_campaigns(req: Request):
        pool = req.app.state.db_pool
        async with pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    "SELECT id,name,subject,status,successful_sends,failed_sends,created_at,opens,clicks "
                    "FROM campaigns ORDER BY created_at DESC"
                )
                return await cur.fetchall()

    @app.get("/campaigns/{campaign_id}", response_model=CampaignDetail, summary="Get Campaign")
    async def get_campaign(campaign_id: int, req: Request):
        pool = req.app.state.db_pool
        async with pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("SELECT * FROM campaigns WHERE id = %s", (campaign_id,))
                camp = await cur.fetchone()
                if not camp:
                    raise HTTPException(404, "Campaign not found")
                await cur.execute(
                    "SELECT email,status,error_message,timestamp "
                    "FROM email_send_logs WHERE campaign_id=%s ORDER BY timestamp",
                    (campaign_id,)
                )
                logs = await cur.fetchall()
        return CampaignDetail(**camp, send_logs=logs)

    @app.get("/health", summary="Health")
    async def health():
        return {"status": "ok"}

    return app

# 12) Dev entry-point (Windows-friendly with reload)
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:create_app", host="0.0.0.0", port=8005, reload=True, factory=True)
