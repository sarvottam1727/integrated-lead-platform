# run_uvicorn.py
import sys, asyncio, uvicorn

# Must happen before uvicorn creates the loop
if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

uvicorn.run(
    "main:app",
    host="0.0.0.0",
    port=8005,
    log_level="debug",
    reload=False,  # keep False for now to avoid spawning child processes
)
