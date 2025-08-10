# orchestrator_service.py
import os
import sys
import asyncio
import platform
import yaml
import shutil
import structlog

from contextlib import asynccontextmanager
from typing import Dict

import httpx
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Depends, Security
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import APIKeyHeader
from prometheus_client import Counter, Gauge, make_asgi_app
from pydantic_settings import BaseSettings

# ───────────────────────── Logging ─────────────────────────
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.stdlib.add_log_level,
        structlog.processors.JSONRenderer(),
    ]
)
log = structlog.get_logger()

# ───────────────────────── Settings ────────────────────────
class Settings(BaseSettings):
    PROJECT_BASE_PATH: str
    ORCHESTRATOR_API_KEY: str
    CORS_ORIGINS: str = "http://localhost:3000"  # comma-separated

    class Config:
        env_file = ".env"

settings = Settings()
API_KEY = settings.ORCHESTRATOR_API_KEY
api_key_header = APIKeyHeader(name="X-API-Key")

# ───────────────────────── Metrics ─────────────────────────
start_counter = Counter("orchestrator_service_starts_total", "Total service start attempts", ["service"])
stop_counter  = Counter("orchestrator_service_stops_total", "Total service stop attempts", ["service"])
status_gauge  = Gauge("orchestrator_service_status", "Service status (1=running,0=stopped)", ["service"])

# ────────────────── Service config loader ──────────────────
def _find_services_yaml() -> str:
    base = settings.PROJECT_BASE_PATH
    candidates = [
        os.path.join(base, "services.yaml"),
        os.path.join(base, "service.yaml"),
    ]
    for p in candidates:
        if os.path.exists(p):
            return p
    log.error("services file not found", tried=candidates)
    sys.exit(1)

def load_configs() -> Dict[str, dict]:
    path = _find_services_yaml()
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    for name, conf in cfg.items():
        cwd = os.path.join(settings.PROJECT_BASE_PATH, conf["cwd"])
        if not os.path.isdir(cwd):
            log.error("Service cwd invalid", service=name, cwd=cwd)
            sys.exit(1)
        conf["cwd"] = cwd

        # normalize command (python in venv, npm.cmd on Windows)
        cmd0 = conf["cmd"][0]
        if "venv/" in cmd0 or "venv\\" in cmd0:
            python_exec = "Scripts/python.exe" if platform.system() == "Windows" else "bin/python"
            conf["cmd"][0] = os.path.join(cwd, "venv", python_exec)
        elif os.path.basename(cmd0) == "npm":
            conf["cmd"][0] = "npm.cmd" if platform.system() == "Windows" else "npm"

        if shutil.which(conf["cmd"][0]) is None:
            log.error("Executable not found", service=name, exe=conf["cmd"][0])
            sys.exit(1)

    return cfg

SERVICE_CONFIGS = load_configs()
processes: Dict[str, asyncio.subprocess.Process] = {}

# ─────────────────────── Lifespan ─────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("orchestrator_start")
    try:
        yield
    finally:
        log.info("orchestrator_shutdown", n=len(processes))
        for name, proc in processes.items():
            if proc and proc.returncode is None:
                log.info("terminating", service=name, pid=proc.pid)
                proc.terminate()
                try:
                    await asyncio.wait_for(proc.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    proc.kill()
                    log.warning("killed_unresponsive", service=name, pid=proc.pid)

# ─────────────────────── FastAPI app ──────────────────────
app = FastAPI(title="Advanced Orchestrator Service", lifespan=lifespan)

allowed_origins = [o.strip() for o in settings.CORS_ORIGINS.split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins or ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.mount("/metrics", make_asgi_app())

# ─────────────────── Security dependency ───────────────────
async def get_api_key(key: str = Security(api_key_header)):
    if key != API_KEY:
        raise HTTPException(403, "Invalid API key")
    return key

# ───────────────────── Helper functions ────────────────────
def validate_service(name: str) -> dict:
    cfg = SERVICE_CONFIGS.get(name)
    if not cfg:
        raise HTTPException(404, "Service not found")
    return cfg

# ───────────────────────── Routes ──────────────────────────
@app.get("/health")
async def health():
    return {"orchestrator": "healthy"}

@app.get("/services/status")
async def services_status(key: str = Depends(get_api_key)):
    result = {}
    for name, conf in SERVICE_CONFIGS.items():
        proc = processes.get(name)
        running = proc and proc.returncode is None
        result[name] = {"status": "running" if running else "stopped", "pid": proc.pid if running else None}
    return result

@app.post("/services/{service_name}/start")
async def start_service(service_name: str, key: str = Depends(get_api_key)):
    cfg = validate_service(service_name)
    existing = processes.get(service_name)
    if existing and existing.returncode is None:
        return {"status": "already_running", "pid": existing.pid}

    try:
        proc = await asyncio.create_subprocess_exec(
            *cfg["cmd"],
            cwd=cfg["cwd"],
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        processes[service_name] = proc
        start_counter.labels(service=service_name).inc()
        status_gauge.labels(service=service_name).set(1)
        log.info("service_started", service=service_name, pid=proc.pid)
        return {"status": "started", "pid": proc.pid}
    except Exception as e:
        log.error("start_failed", service=service_name, error=str(e))
        raise HTTPException(500, f"Failed to start {service_name}: {e}")

@app.post("/services/{service_name}/stop")
async def stop_service(service_name: str, key: str = Depends(get_api_key)):
    proc = processes.get(service_name)
    if not proc or proc.returncode is not None:
        status_gauge.labels(service=service_name).set(0)
        return {"status": "not_running"}
    try:
        proc.terminate()
        stop_counter.labels(service=service_name).inc()
        status_gauge.labels(service=service_name).set(0)
        log.info("stop_signal_sent", service=service_name, pid=proc.pid)
        return {"status": "stopping"}
    except Exception as e:
        log.error("stop_failed", service=service_name, error=str(e))
        raise HTTPException(500, f"Failed to stop {service_name}: {e}")

# WebSocket logs: pass api_key via query param (?api_key=...)
@app.websocket("/ws/logs/{service_name}")
async def ws_logs(websocket: WebSocket, service_name: str):
    await websocket.accept()
    try:
        api_key = websocket.query_params.get("api_key")
        if api_key != API_KEY:
            await websocket.send_text("Forbidden: invalid api key")
            await websocket.close()
            return
    except Exception:
        await websocket.close()
        return

    _ = validate_service(service_name)
    proc = processes.get(service_name)
    if not proc or proc.returncode is not None:
        await websocket.send_text(f"{service_name} is not running.")
        await websocket.close()
        return

    try:
        while proc.returncode is None:
            line = await proc.stdout.readline()
            if not line:
                await asyncio.sleep(0.05)
                continue
            await websocket.send_text(line.decode("utf-8", errors="ignore"))
    except WebSocketDisconnect:
        pass
    finally:
        await websocket.close()
