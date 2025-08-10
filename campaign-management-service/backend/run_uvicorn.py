# run_uvicorn.py
import os, asyncio
if os.name == "nt":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import uvicorn

if __name__ == "__main__":
    uvicorn.run(
        "main:create_app",
        host="0.0.0.0",
        port=8005,
        reload=True,
        factory=True,
    )
