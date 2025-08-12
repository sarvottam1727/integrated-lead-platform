"""
Lead Processing Worker (robust, production-hardened single file)

- Consumes lead messages from RabbitMQ.
- Batches to POST /contacts/batch on the Contacts service.
- Validates & deduplicates (in-batch) by email.
- Automatic retries (HTTP + RabbitMQ reconnect).
- Graceful shutdown on Ctrl+C / SIGTERM (Windows-safe).
- Falls back from batch -> per-item on conflicts/validation errors.
- Optional Dead-Letter Queue (DLQ) for permanently failed items.
- Prevents hot loops; supports backoff; has periodic metrics.

ENV (with defaults):
  CONTACT_SERVICE_URL=http://localhost:8002/contacts/batch
  RABBITMQ_HOST=localhost
  RABBITMQ_PORT=5672
  RABBITMQ_USER=guest
  RABBITMQ_PASS=guest
  RABBITMQ_VHOST=/
  RABBITMQ_QUEUE=lead_queue
  RABBITMQ_DLX=lead_dlx                # optional exchange for DLQ
  RABBITMQ_DLQ=lead_dlq                # optional DLQ queue name
  RABBITMQ_PREFETCH=200

  BATCH_SIZE=100
  BATCH_TIMEOUT=5.0
  MAX_RETRIES=3
  RETRY_BACKOFF_FACTOR=2.0
  HTTP_TIMEOUT=15
  PER_ITEM_TIMEOUT=10

  # When Contacts service is down for long, pause consuming (seconds)
  BACKPRESSURE_SLEEP=10

Requires: pika, requests, urllib3
"""

from __future__ import annotations
import os
import sys
import time
import json
import re
import logging
import signal
from typing import Dict, Any, List, Tuple
from threading import Thread, Lock, Event

import pika
from pika.adapters.blocking_connection import BlockingChannel
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ------------------------- Config -------------------------
CONTACT_SERVICE_URL   = os.getenv("CONTACT_SERVICE_URL", "http://localhost:8002/contacts/batch")

RABBITMQ_HOST         = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT         = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER         = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS         = os.getenv("RABBITMQ_PASS", "guest")
RABBITMQ_VHOST        = os.getenv("RABBITMQ_VHOST", "/")
RABBITMQ_QUEUE        = os.getenv("RABBITMQ_QUEUE", "lead_queue")
RABBITMQ_DLX          = os.getenv("RABBITMQ_DLX")  # optional
RABBITMQ_DLQ          = os.getenv("RABBITMQ_DLQ")  # optional
RABBITMQ_PREFETCH     = int(os.getenv("RABBITMQ_PREFETCH", "200"))

BATCH_SIZE            = int(os.getenv("BATCH_SIZE", "100"))
BATCH_TIMEOUT         = float(os.getenv("BATCH_TIMEOUT", "5.0"))
MAX_RETRIES           = int(os.getenv("MAX_RETRIES", "3"))
RETRY_BACKOFF_FACTOR  = float(os.getenv("RETRY_BACKOFF_FACTOR", "2.0"))
HTTP_TIMEOUT          = float(os.getenv("HTTP_TIMEOUT", "15"))
PER_ITEM_TIMEOUT      = float(os.getenv("PER_ITEM_TIMEOUT", "10"))
BACKPRESSURE_SLEEP    = float(os.getenv("BACKPRESSURE_SLEEP", "10"))

# ------------------------- Logging ------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03dZ [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("lead-worker")

# ------------------------- Globals ------------------------
EMAIL_REGEX = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

shutdown_event = Event()

class Metrics:
    def __init__(self) -> None:
        self.lock = Lock()
        self.processed_msgs = 0
        self.invalid_msgs = 0
        self.batch_ok = 0
        self.batch_fail = 0
        self.item_ok = 0
        self.item_fail = 0
        self.requeued = 0
        self.deadlettered = 0

    def inc(self, attr: str, n: int = 1) -> None:
        with self.lock:
            setattr(self, attr, getattr(self, attr) + n)

    def snapshot(self) -> Dict[str, int]:
        with self.lock:
            return {
                "processed_msgs": self.processed_msgs,
                "invalid_msgs": self.invalid_msgs,
                "batch_ok": self.batch_ok,
                "batch_fail": self.batch_fail,
                "item_ok": self.item_ok,
                "item_fail": self.item_fail,
                "requeued": self.requeued,
                "deadlettered": self.deadlettered,
            }

    def log(self, prefix: str = "Metrics") -> None:
        s = self.snapshot()
        logger.info(
            f"{prefix}: processed={s['processed_msgs']} invalid={s['invalid_msgs']} "
            f"batch_ok={s['batch_ok']} batch_fail={s['batch_fail']} "
            f"item_ok={s['item_ok']} item_fail={s['item_fail']} "
            f"requeued={s['requeued']} dlq={s['deadlettered']}"
        )

metrics = Metrics()

def build_session() -> requests.Session:
    # Retry on specific status codes; backoff across attempts
    retry_kwargs = dict(
        total=MAX_RETRIES,
        backoff_factor=RETRY_BACKOFF_FACTOR,
        status_forcelist=[409, 413, 422, 429, 500, 502, 503, 504],
        respect_retry_after_header=True,
    )
    try:
        retry = Retry(allowed_methods=frozenset(["POST"]), **retry_kwargs)
    except TypeError:
        # urllib3 < 2.0
        retry = Retry(method_whitelist=frozenset(["POST"]), **retry_kwargs)

    adapter = HTTPAdapter(max_retries=retry)
    s = requests.Session()
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

session = build_session()

# ---------------------- RabbitMQ utils --------------------
def declare_queues(channel: BlockingChannel) -> None:
    args = {}
    # Optionally bind our main queue to a DLX so server can DLQ nacked/expired messages
    if RABBITMQ_DLX:
        args["x-dead-letter-exchange"] = RABBITMQ_DLX

    channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True, arguments=args)

    # If DLQ requested, ensure DLX exchange and DLQ exist/bound
    if RABBITMQ_DLX and RABBITMQ_DLQ:
        channel.exchange_declare(exchange=RABBITMQ_DLX, exchange_type="fanout", durable=True)
        channel.queue_declare(queue=RABBITMQ_DLQ, durable=True)
        channel.queue_bind(queue=RABBITMQ_DLQ, exchange=RABBITMQ_DLX)

def connect_rabbit() -> Tuple[pika.BlockingConnection, BlockingChannel]:
    attempt = 0
    while not shutdown_event.is_set():
        try:
            creds = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
            params = pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                port=RABBITMQ_PORT,
                virtual_host=RABBITMQ_VHOST,
                credentials=creds,
                heartbeat=600,
                blocked_connection_timeout=300,
                client_properties={"connection_name": "lead-worker"},
            )
            conn = pika.BlockingConnection(params)
            ch = conn.channel()
            declare_queues(ch)
            ch.basic_qos(prefetch_count=RABBITMQ_PREFETCH)
            logger.info("Connected to RabbitMQ.")
            return conn, ch
        except pika.exceptions.AMQPConnectionError as e:
            attempt += 1
            wait = min(60.0, (RETRY_BACKOFF_FACTOR ** attempt))
            logger.warning(f"RabbitMQ connect failed, retrying in {wait:.1f}s: {e}")
            time.sleep(wait)
    raise RuntimeError("Shutdown requested before RabbitMQ connection was established.")

def dlq_publish(channel: BlockingChannel, body: bytes) -> None:
    """Publish a single message to DLQ if configured; else drop."""
    if not (RABBITMQ_DLX and RABBITMQ_DLQ):
        return
    try:
        channel.basic_publish(
            exchange=RABBITMQ_DLX,
            routing_key="",  # fanout
            body=body,
            properties=pika.BasicProperties(content_type="application/json", delivery_mode=2),
        )
        metrics.inc("deadlettered")
    except Exception as e:
        logger.error(f"Failed to publish to DLQ: {e}")

# ---------------------- Batching buffer -------------------
class BatchBuffer:
    """Thread-safe batch buffer with timeout flushing."""
    def __init__(self, batch_size: int, timeout_s: float) -> None:
        self.batch_size = batch_size
        self.timeout_s = timeout_s
        self._items: List[Dict[str, Any]] = []
        self._emails_in_batch: set[str] = set()  # dedupe by email in-batch
        self._lock = Lock()
        self._last_flush = time.time()

    def add(self, entry: Dict[str, Any]) -> bool:
        """Add an entry. Returns True if size threshold reached and a flush is recommended."""
        with self._lock:
            email = entry["data"]["email"]
            if email in self._emails_in_batch:
                # Duplicate in current batch — ack immediately to avoid double import
                entry["channel"].basic_ack(delivery_tag=entry["tag"])
                metrics.inc("processed_msgs")  # we *did* process the message logically
                return False
            self._items.append(entry)
            self._emails_in_batch.add(email)
            return len(self._items) >= self.batch_size

    def due_to_timeout(self) -> bool:
        with self._lock:
            return bool(self._items) and (time.time() - self._last_flush) >= self.timeout_s

    def drain(self) -> List[Dict[str, Any]]:
        with self._lock:
            items = self._items
            self._items = []
            self._emails_in_batch.clear()
            self._last_flush = time.time()
        return items

batch_buffer = BatchBuffer(BATCH_SIZE, BATCH_TIMEOUT)

def timeout_flusher() -> None:
    """Background thread to flush batch on timeout."""
    while not shutdown_event.is_set():
        time.sleep(1)
        if batch_buffer.due_to_timeout():
            logger.info("Batch timeout reached → flushing")
            flush_batch("timeout")
            metrics.log("Post-timeout flush")

# ---------------------- HTTP helpers ----------------------
def post_contacts_batch(payload: List[Dict[str, Any]]) -> requests.Response:
    return session.post(CONTACT_SERVICE_URL, json=payload, timeout=HTTP_TIMEOUT)

def post_contacts_single(item: Dict[str, Any]) -> requests.Response:
    return session.post(CONTACT_SERVICE_URL, json=[item], timeout=PER_ITEM_TIMEOUT)

def is_transient_http(code: int) -> bool:
    # Consider transient if 5xx, 429, gateway issues
    return code in (429, 500, 502, 503, 504)

def classify_batch_error(resp: requests.Response) -> str:
    """
    Classify known batch error types:
    - 409: conflict (duplicates) → fallback per-item
    - 413: payload too large → split & retry (handled by caller)
    - 422: validation error → likely some bad items → per-item
    """
    if resp.status_code == 409:
        return "conflict"
    if resp.status_code == 413:
        return "too_large"
    if resp.status_code == 422:
        return "validation"
    if is_transient_http(resp.status_code):
        return "transient"
    return "fatal"

# ---------------------- Flush logic -----------------------
def split_and_retry(items: List[Dict[str, Any]]) -> None:
    """Binary split for large payload (413) until minimal size or success/fallback-hit."""
    if not items:
        return
    if len(items) == 1:
        # Single item too large / failing — try per-item path (same end result)
        fallback_per_item(items)
        return
    mid = len(items) // 2
    left, right = items[:mid], items[mid:]
    logger.info(f"Splitting batch {len(items)} → {len(left)} + {len(right)}")
    flush_specific_batch(left)
    flush_specific_batch(right)

def fallback_per_item(items: List[Dict[str, Any]]) -> None:
    """Try to import each item individually; DLQ on hard failure."""
    for e in items:
        data = e["data"]
        try:
            r = post_contacts_single(data)
            if r.ok:
                e["channel"].basic_ack(delivery_tag=e["tag"])
                metrics.inc("item_ok")
                logger.info(f"Item OK: {data.get('email')}")
            else:
                if is_transient_http(r.status_code):
                    # Requeue to try again later (backpressure)
                    e["channel"].basic_nack(delivery_tag=e["tag"], requeue=True)
                    metrics.inc("requeued")
                    logger.warning(f"Item transient {r.status_code}: requeue {data.get('email')}")
                else:
                    # Send to DLQ or drop
                    dlq_publish(e["channel"], json.dumps(data).encode("utf-8"))
                    e["channel"].basic_nack(delivery_tag=e["tag"], requeue=False)
                    metrics.inc("item_fail")
                    logger.error(f"Item fatal {r.status_code}: DLQ {data.get('email')}  body={r.text[:200]}")
        except requests.RequestException as ex:
            # Network error – requeue for later
            e["channel"].basic_nack(delivery_tag=e["tag"], requeue=True)
            metrics.inc("requeued")
            logger.warning(f"Item network error, requeued {data.get('email')}: {ex}")

def flush_specific_batch(items: List[Dict[str, Any]]) -> None:
    """Flush a specific list of items with robust fallbacks."""
    if not items:
        return
    payload = [e["data"] for e in items]

    try:
        r = post_contacts_batch(payload)
        if r.ok:
            for e in items:
                e["channel"].basic_ack(delivery_tag=e["tag"])
            metrics.inc("batch_ok")
            logger.info(f"Batch OK: n={len(items)} resp={safe_json(r)}")
            return

        # Non-2xx
        err_type = classify_batch_error(r)
        logger.warning(f"Batch HTTP {r.status_code} ({err_type}) for n={len(items)}; body={r.text[:300]}")

        if err_type == "conflict" or err_type == "validation":
            # Some (or all) items already exist / invalid → per-item path
            fallback_per_item(items)
            return

        if err_type == "too_large":
            # Split into smaller chunks recursively
            split_and_retry(items)
            return

        if err_type == "transient":
            # Requeue all items; avoid hot loop with brief sleep
            for e in items:
                e["channel"].basic_nack(delivery_tag=e["tag"], requeue=True)
            metrics.inc("batch_fail")
            logger.warning(f"Batch transient error, requeued n={len(items)}; sleeping {BACKPRESSURE_SLEEP}s")
            time.sleep(BACKPRESSURE_SLEEP)
            return

        # Fatal batch error → DLQ all items (or drop if no DLQ configured)
        for e in items:
            dlq_publish(e["channel"], json.dumps(e["data"]).encode("utf-8"))
            e["channel"].basic_nack(delivery_tag=e["tag"], requeue=False)
        metrics.inc("batch_fail")
        logger.error(f"Batch fatal error {r.status_code}, sent to DLQ n={len(items)}")

    except requests.RequestException as ex:
        # Network errors → requeue; apply backpressure sleep
        for e in items:
            e["channel"].basic_nack(delivery_tag=e["tag"], requeue=True)
        metrics.inc("batch_fail")
        logger.warning(f"Batch network error, requeued n={len(items)}: {ex}; sleeping {BACKPRESSURE_SLEEP}s")
        time.sleep(BACKPRESSURE_SLEEP)

def flush_batch(reason: str) -> None:
    items = batch_buffer.drain()
    if not items:
        return
    logger.info(f"Flushing batch (reason={reason}) size={len(items)} → {CONTACT_SERVICE_URL}")
    flush_specific_batch(items)

def safe_json(resp: requests.Response) -> Any:
    try:
        return resp.json()
    except Exception:
        return f"HTTP {resp.status_code}"

# ---------------------- Message handling ------------------
def parse_lead(body: bytes) -> Dict[str, Any] | None:
    try:
        record = json.loads(body)
    except json.JSONDecodeError:
        metrics.inc("invalid_msgs")
        return None

    email = (record.get("SENDER_EMAIL") or "").strip().lower()
    if not EMAIL_REGEX.match(email):
        metrics.inc("invalid_msgs")
        return None

    lead = {
        "unique_query_id": (record.get("UNIQUE_QUERY_ID") or "").strip(),
        "name":           (record.get("SENDER_NAME")    or "").strip(),
        "email":          email,
        "phone_number":   (record.get("SENDER_MOBILE")  or "").strip(),
        "company_name":   (record.get("SENDER_COMPANY") or "").strip(),
        "city":           (record.get("CITY")          or "").strip(),
        "state":          (record.get("STATE")         or "").strip(),
    }
    return lead

# ---------------------- Main consume loop -----------------
def consume_forever() -> None:
    """
    Outer loop: ensure we stay connected; reconnect on drops.
    """
    Thread(target=timeout_flusher, daemon=True).start()

    while not shutdown_event.is_set():
        try:
            conn, ch = connect_rabbit()

            # Iterate using basic_consume-like generator with inactivity_timeout
            for method, props, body in ch.consume(RABBITMQ_QUEUE, inactivity_timeout=1):
                if shutdown_event.is_set():
                    break

                if method is None:
                    # No message this second
                    continue

                lead = parse_lead(body)
                if lead is None:
                    # Malformed/invalid → ACK (discard)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    continue

                entry = {
                    "data": lead,
                    "tag": method.delivery_tag,
                    "channel": ch,
                }

                metrics.inc("processed_msgs")

                flush_now = batch_buffer.add(entry)
                if flush_now:
                    flush_batch("size")
                    metrics.log("Post-size flush")

            try:
                ch.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

        except pika.exceptions.AMQPConnectionError as e:
            logger.warning(f"RabbitMQ connection dropped: {e}. Reconnecting soon...")
            time.sleep(min(60.0, RETRY_BACKOFF_FACTOR))
        except Exception as e:
            logger.error(f"Unexpected consume error: {e}", exc_info=True)
            time.sleep(min(60.0, RETRY_BACKOFF_FACTOR))

    # Final flush on shutdown
    flush_batch("shutdown")
    metrics.log("Final")

# ---------------------- Signals / Entrypoint --------------
def install_signal_handlers() -> None:
    def _stop(signum, _frame):
        logger.info(f"Signal {signum} received: shutting down...")
        shutdown_event.set()

    # Windows has limited signals; SIGINT is enough for Ctrl+C.
    for sig in (getattr(signal, "SIGINT", None), getattr(signal, "SIGTERM", None)):
        if sig is not None:
            try:
                signal.signal(sig, _stop)
            except Exception:
                pass  # some platforms may reject setting handlers

def main() -> None:
    logger.info("Lead Processor starting")
    logger.info(f"Contacts endpoint: {CONTACT_SERVICE_URL}")
    logger.info(
        f"RabbitMQ: host={RABBITMQ_HOST}:{RABBITMQ_PORT} vhost={RABBITMQ_VHOST} "
        f"queue={RABBITMQ_QUEUE} dlx={RABBITMQ_DLX or '-'} dlq={RABBITMQ_DLQ or '-'}"
    )
    install_signal_handlers()
    consume_forever()
    logger.info("Lead Processor exited cleanly.")

if __name__ == "__main__":
    main()
