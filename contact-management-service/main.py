import os
import asyncio
from pathlib import Path
from typing import List, Optional, Set
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr, field_validator
from psycopg_pool import AsyncConnectionPool
from psycopg.rows import dict_row
import structlog
from prometheus_client import Counter, Histogram, make_asgi_app, CollectorRegistry
from dotenv import load_dotenv

# ── FORCE Windows to use the SelectorEventLoop for psycopg  ────────────────
if os.name == "nt":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from dotenv import load_dotenv

# ── Load environment from .env (and override any existing vars) ───
env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=env_path, override=True)
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set—did you forget to create .env?")


# ── Structured logging setup ───────────────────────────────────────────────
tuple_processors = [
    structlog.processors.TimeStamper(fmt="iso"),
    structlog.stdlib.add_log_level,
    structlog.processors.JSONRenderer(),
]
structlog.configure(processors=tuple_processors)
logger = structlog.get_logger()

# ── FastAPI app ───────────────────────────────────────────────────────────
app = FastAPI(title="Contact Management Service")

# ── CORS ──────────────────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1) Create and open the pool
    pool = AsyncConnectionPool(conninfo=DATABASE_URL, max_size=10)
    await pool.open()
    app.state.db_pool = pool

    # 2) Initialize WebSockets set
    app.state.clients: Set[WebSocket] = set()
    logger.info("Database pool initialized and opened")

    # 3) Metrics
    registry = CollectorRegistry()
    app.state.request_counter = Counter(
        "contact_service_requests_total",
        "Total requests to Contact Service",
        ["endpoint"],
        registry=registry,
    )
    app.state.request_latency = Histogram(
        "contact_service_request_latency_seconds",
        "Request latency",
        ["endpoint"],
        registry=registry,
    )
    app.mount("/metrics", make_asgi_app(registry=registry))

    yield

    # Teardown
    for ws in list(app.state.clients):
        await ws.close()
    await pool.close()
    logger.info("Database pool closed")



    # 2) Metrics
    registry = CollectorRegistry()
    app.state.request_counter = Counter(
        "contact_service_requests_total",
        "Total requests to Contact Service",
        ["endpoint"],
        registry=registry,
    )
    app.state.request_latency = Histogram(
        "contact_service_request_latency_seconds",
        "Request latency",
        ["endpoint"],
        registry=registry,
    )
    # mount metrics
    app.mount("/metrics", make_asgi_app(registry=registry))

    yield

    # teardown
    for ws in list(app.state.clients):
        await ws.close()
    await app.state.db_pool.close()
    logger.info("Database pool closed")

app.router.lifespan_context = lifespan

# ── Pydantic models ───────────────────────────────────────────────────────
class ContactCreate(BaseModel):
    name: str
    email: EmailStr
    phone_number: Optional[str] = None
    company_name: Optional[str] = None

    @field_validator("name")
    def validate_name(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("Name cannot be empty")
        return v.strip()

class ContactUpdate(BaseModel):
    name: Optional[str] = None
    email: Optional[EmailStr] = None
    phone_number: Optional[str] = None
    company_name: Optional[str] = None
    dnc: Optional[bool] = None

    @field_validator("name")
    def validate_name(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not v.strip():
            raise ValueError("Name cannot be empty")
        return v.strip() if v else None

class Contact(BaseModel):
    id: int
    name: str
    email: EmailStr
    phone_number: Optional[str]
    company_name: Optional[str]
    dnc: bool
    created_at: datetime
    updated_at: datetime

class BatchResponse(BaseModel):
    success: int
    failures: List[dict]

# ── Helpers ────────────────────────────────────────────────────────────────
async def validate_contact_exists(pool: AsyncConnectionPool, contact_id: int) -> bool:
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT 1 FROM contacts WHERE id = %s", (contact_id,))
            return bool(await cur.fetchone())

async def broadcast(app: FastAPI, event: str, data: dict):
    dead = []
    for ws in app.state.clients:
        try:
            await ws.send_json({"event": event, "data": data})
        except:
            dead.append(ws)
    for ws in dead:
        app.state.clients.discard(ws)

# ── WebSocket endpoint ────────────────────────────────────────────────────
@app.websocket("/ws/contacts")
async def ws_contacts(websocket: WebSocket):
    await websocket.accept()
    app.state.clients.add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        app.state.clients.discard(websocket)

@app.get("/whoami")
async def whoami(request: Request):
    try:
        # grab a raw AsyncConnection from the pool
        async with request.app.state.db_pool.connection() as conn:
            # open a cursor on it
            async with conn.cursor() as cur:
                await cur.execute("SELECT current_user;")
                row = await cur.fetchone()
        return {"current_user": row[0]}
    except Exception as e:
        logger.error("whoami failed", error=str(e), exc_info=True)
        raise HTTPException(status_code=500, detail="Internal error in whoami")




# ── Health check ──────────────────────────────────────────────────────────
@app.get("/health")
async def health_check(request: Request):
    start = datetime.now()
    try:
        async with request.app.state.db_pool.connection() as conn:
            await conn.execute("SELECT 1")
        labels = {"endpoint": "/health"}
        request.app.state.request_counter.labels(**labels).inc()
        request.app.state.request_latency.labels(**labels).observe(
            (datetime.now() - start).total_seconds()
        )
        return {"status": "healthy"}
    except Exception as e:
        logger.error("Health check failed", error=str(e))
        raise HTTPException(status_code=500, detail="Service unhealthy")

# ── CRUD: create ──────────────────────────────────────────────────────────
@app.post("/contacts", response_model=Contact)
async def create_contact(contact: ContactCreate, request: Request):
    start = datetime.now()
    async with request.app.state.db_pool.connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                try:
                    await cur.execute(
                        "INSERT INTO contacts (name,email,phone_number,company_name) "
                        "VALUES (%s,%s,%s,%s) RETURNING *",
                        (contact.name, contact.email, contact.phone_number, contact.company_name),
                    )
                    new = await cur.fetchone()
                    await broadcast(request.app, "contacts_added", {"created_count": 1})
                    labels = {"endpoint": "/contacts"}
                    request.app.state.request_counter.labels(**labels).inc()
                    request.app.state.request_latency.labels(**labels).observe(
                        (datetime.now() - start).total_seconds()
                    )
                    logger.info("Contact created", contact_id=new["id"], email=contact.email)
                    return new
                except Exception as e:
                    logger.error("Failed to create contact", error=str(e))
                    if "unique constraint" in str(e).lower():
                        raise HTTPException(status_code=400, detail="Email already exists")
                    raise HTTPException(status_code=500, detail="Internal server error")

# ── CRUD: list ────────────────────────────────────────────────────────────
@app.get("/contacts")
async def list_contacts(
    request: Request,
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=500),
):
    start = datetime.now()
    async with request.app.state.db_pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("SELECT COUNT(*) AS c FROM contacts")
            total = (await cur.fetchone())["c"]
            offset = (page - 1) * per_page
            await cur.execute(
                "SELECT * FROM contacts ORDER BY created_at DESC LIMIT %s OFFSET %s",
                (per_page, offset),
            )
            data = await cur.fetchall()
    labels = {"endpoint": "/contacts"}
    request.app.state.request_counter.labels(**labels).inc()
    request.app.state.request_latency.labels(**labels).observe(
        (datetime.now() - start).total_seconds()
    )
    logger.info("Contacts retrieved", count=len(data))
    return {"contacts": data, "total": total}

# ── CRUD: get single ──────────────────────────────────────────────────────
@app.get("/contacts/{contact_id}", response_model=Contact)
async def get_contact(contact_id: int, request: Request):
    start = datetime.now()
    if not await validate_contact_exists(request.app.state.db_pool, contact_id):
        raise HTTPException(status_code=404, detail="Contact not found")
    async with request.app.state.db_pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("SELECT * FROM contacts WHERE id = %s", (contact_id,))
            rec = await cur.fetchone()
    labels = {"endpoint": "/contacts/{contact_id}"}
    request.app.state.request_counter.labels(**labels).inc()
    request.app.state.request_latency.labels(**labels).observe(
        (datetime.now() - start).total_seconds()
    )
    logger.info("Contact retrieved", contact_id=contact_id)
    return rec

# ── CRUD: update ──────────────────────────────────────────────────────────
@app.put("/contacts/{contact_id}", response_model=Contact)
async def update_contact(contact_id: int, contact: ContactUpdate, request: Request):
    start = datetime.now()
    if not await validate_contact_exists(request.app.state.db_pool, contact_id):
        raise HTTPException(status_code=404, detail="Contact not found")
    fields = {k: v for k, v in contact.dict(exclude_unset=True).items() if v is not None}
    if not fields:
        raise HTTPException(status_code=400, detail="No fields to update")
    fields["updated_at"] = datetime.now()
    set_clause = ", ".join(f"{k} = %s" for k in fields)
    values = list(fields.values()) + [contact_id]
    async with request.app.state.db_pool.connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                try:
                    await cur.execute(f"UPDATE contacts SET {set_clause} WHERE id = %s RETURNING *", values)
                    updated = await cur.fetchone()
                    await broadcast(request.app, "contact_updated", updated)
                    labels = {"endpoint": "/contacts/{contact_id}"}
                    request.app.state.request_counter.labels(**labels).inc()
                    request.app.state.request_latency.labels(**labels).observe(
                        (datetime.now() - start).total_seconds()
                    )
                    logger.info("Contact updated", contact_id=contact_id)
                    return updated
                except Exception as e:
                    logger.error("Failed to update contact", error=str(e))
                    if "unique constraint" in str(e).lower():
                        raise HTTPException(status_code=400, detail="Email already exists")
                    raise HTTPException(status_code=500, detail="Internal server error")

# ── CRUD: delete ──────────────────────────────────────────────────────────
@app.delete("/contacts/{contact_id}")
async def delete_contact(contact_id: int, request: Request):
    start = datetime.now()
    if not await validate_contact_exists(request.app.state.db_pool, contact_id):
        raise HTTPException(status_code=404, detail="Contact not found")
    async with request.app.state.db_pool.connection() as conn:
        async with conn.transaction():
            async with conn.cursor() as cur:
                await cur.execute("DELETE FROM contacts WHERE id = %s", (contact_id,))
                await broadcast(request.app, "contact_deleted", {"id": contact_id})
    labels = {"endpoint": "/contacts/{contact_id}"}
    request.app.state.request_counter.labels(**labels).inc()
    request.app.state.request_latency.labels(**labels).observe(
        (datetime.now() - start).total_seconds()
    )
    logger.info("Contact deleted", contact_id=contact_id)
    return {"message": f"Contact {contact_id} deleted"}

# ── Batch create ──────────────────────────────────────────────────────────
@app.post("/contacts/batch", response_model=BatchResponse)
async def batch_create_contacts(contacts: List[ContactCreate], request: Request):
    start = datetime.now()
    success = 0
    failures = []
    async with request.app.state.db_pool.connection() as conn:
        async with conn.transaction():
            async with conn.cursor() as cur:
                for c in contacts:
                    try:
                        await cur.execute(
                            "INSERT INTO contacts (name,email,phone_number,company_name) "
                            "VALUES (%s,%s,%s,%s)",
                            (c.name, c.email, c.phone_number, c.company_name),
                        )
                        success += 1
                    except Exception as e:
                        failures.append({"email": c.email, "error": str(e)})
            if success:
                await broadcast(request.app, "contacts_added", {"created_count": success})
    labels = {"endpoint": "/contacts/batch"}
    request.app.state.request_counter.labels(**labels).inc()
    request.app.state.request_latency.labels(**labels).observe(
        (datetime.now() - start).total_seconds()
    )
    logger.info("Batch create completed", success=success, failures=len(failures))
    return {"success": success, "failures": failures}

# ── Toggle DNC ────────────────────────────────────────────────────────────
@app.put("/contacts/{contact_id}/dnc", response_model=Contact)
async def toggle_dnc(contact_id: int, request: Request):
    start = datetime.now()
    if not await validate_contact_exists(request.app.state.db_pool, contact_id):
        raise HTTPException(status_code=404, detail="Contact not found")
    async with request.app.state.db_pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "UPDATE contacts SET dnc = NOT dnc, updated_at = NOW() WHERE id = %s RETURNING *",
                (contact_id,),
            )
            updated = await cur.fetchone()
    await broadcast(request.app, "contact_updated", updated)
    labels = {"endpoint": "/contacts/{contact_id}/dnc"}
    request.app.state.request_counter.labels(**labels).inc()
    request.app.state.request_latency.labels(**labels).observe(
        (datetime.now() - start).total_seconds()
    )
    logger.info("DNC toggled", contact_id=contact_id, new_status=updated["dnc"])
    return updated

# ── Entrypoint ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8002, reload=True)
