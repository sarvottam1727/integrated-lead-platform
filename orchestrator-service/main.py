# orchestrator_service.py

import os
import sys
import asyncio
import platform
import yaml
import shutil
import structlog

from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Depends, Security
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import APIKeyHeader
from prometheus_client import Counter, Gauge, make_asgi_app
from pydantic_settings import BaseSettings

# --- 1. Configuration & Logging ---
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.stdlib.add_log_level,
        structlog.processors.JSONRenderer(),
    ]
)
logger = structlog.get_logger()

class Settings(BaseSettings):
    PROJECT_BASE_PATH: str
    ORCHESTRATOR_API_KEY: str

    class Config:
        env_file = ".env"

settings = Settings()
API_KEY = settings.ORCHESTRATOR_API_KEY
api_key_header = APIKeyHeader(name="X-API-Key")

# --- 2. Prometheus metrics ---
start_counter = Counter("orchestrator_service_starts_total", "Total service start attempts", ["service"])
stop_counter  = Counter("orchestrator_service_stops_total", "Total service stop attempts", ["service"])
status_gauge  = Gauge("orchestrator_service_status", "Service status (1=running,0=stopped)", ["service"])

# --- 3. Load service configs from YAML ---
def load_configs():
    path = os.path.join(settings.PROJECT_BASE_PATH, "services.yaml")
    if not os.path.exists(path):
        logger.error("services.yaml not found", path=path)
        sys.exit(1)
    with open(path) as f:
        cfg = yaml.safe_load(f)
    # adjust venv/python paths
    for name, conf in cfg.items():
        cwd = os.path.join(settings.PROJECT_BASE_PATH, conf["cwd"])
        if not os.path.isdir(cwd):
            logger.error("Service cwd invalid", service=name, cwd=cwd)
            sys.exit(1)
        conf["cwd"] = cwd
        cmd0 = conf["cmd"][0]
        if "venv/" in cmd0:
            # use correct python in venv
            python_exec = "Scripts/python.exe" if platform.system()=="Windows" else "bin/python"
            conf["cmd"][0] = os.path.join(cwd, "venv", python_exec)
        elif cmd0.endswith("npm"):
            conf["cmd"][0] = "npm.cmd" if platform.system()=="Windows" else "npm"
        # check executable exists
        if shutil.which(conf["cmd"][0]) is None:
            logger.error("Executable not found", service=name, exe=conf["cmd"][0])
            sys.exit(1)
    return cfg

SERVICE_CONFIGS = load_configs()
processes: dict[str, asyncio.subprocess.Process] = {}

# --- 4. Lifespan: graceful shutdown ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Orchestrator starting up")
    yield
    logger.info("Orchestrator shutting down: terminating services")
    for name, proc in processes.items():
        if proc.returncode is None:
            logger.info("Terminating", service=name, pid=proc.pid)
            proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                proc.kill()
                logger.warning("Killed unresponsive", service=name, pid=proc.pid)

# --- 5. FastAPI app setup ---
app = FastAPI(title="Advanced Orchestrator Service", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.mount("/metrics", make_asgi_app())

# --- 6. Security dependency ---
async def get_api_key(key: str = Security(api_key_header)):
    if key != API_KEY:
        raise HTTPException(403, "Invalid API key")
    return key

# --- 7. Validate service ---
def validate_service(name: str):
    cfg = SERVICE_CONFIGS.get(name)
    if not cfg:
        raise HTTPException(404, "Service not found")
    return cfg

# --- 8. Routes ---

@app.post("/services/{service_name}/start")
async def start_service(service_name: str, key: str = Depends(get_api_key)):
    cfg = validate_service(service_name)
    # already running?
    existing = processes.get(service_name)
    if existing and existing.returncode is None:
        return {"status": "already_running", "pid": existing.pid}
    # launch process
    try:
        proc = await asyncio.create_subprocess_exec(
            *cfg["cmd"],
            cwd=cfg["cwd"],
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT
        )
        processes[service_name] = proc
        start_counter.labels(service=service_name).inc()
        status_gauge.labels(service=service_name).set(1)
        logger.info("Started", service=service_name, pid=proc.pid)
        return {"status": "started", "pid": proc.pid}
    except Exception as e:
        logger.error("Start failed", service=service_name, error=str(e))
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
        logger.info("Stop signal sent", service=service_name, pid=proc.pid)
        return {"status": "stopping"}
    except Exception as e:
        logger.error("Stop failed", service=service_name, error=str(e))
        raise HTTPException(500, f"Failed to stop {service_name}: {e}")

@app.get("/services/status")
async def services_status(key: str = Depends(get_api_key)):
    result = {}
    for name in SERVICE_CONFIGS:
        proc = processes.get(name)
        running = proc and proc.returncode is None
        result[name] = {
            "status": "running" if running else "stopped",
            "pid": proc.pid if running else None
        }
    return result

@app.get("/health")
async def health():
    # aggregator health: check configured services' health_urls if present
    statuses = {"orchestrator": "healthy"}
    # optionally probe each service
    return statuses

@app.websocket("/ws/logs/{service_name}")
async def ws_logs(websocket: WebSocket, service_name: str, key: str = Depends(get_api_key)):
    cfg = validate_service(service_name)
    await websocket.accept()
    proc = processes.get(service_name)
    if not proc or proc.returncode is not None:
        await websocket.send_text(f"{service_name} is not running.")
        await websocket.close()
        return
    # stream stdout lines
    try:
        while proc.returncode is None:
            line = await proc.stdout.readline()
            if not line:
                break
            await websocket.send_text(line.decode("utf-8", errors="ignore"))
    except WebSocketDisconnect:
        pass
    finally:
        await websocket.close()
