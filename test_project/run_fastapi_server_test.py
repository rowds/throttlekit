import asyncio
import subprocess
import sys
import time
import httpx
import redis.asyncio as aioredis


def log(msg: str):
    timestamp = time.strftime("%H:%M:%S")
    print(f"[{timestamp}] [FastAPI Test] {msg}")


async def run_client():
    # Let the server start
    await asyncio.sleep(2.0)
    
    async with httpx.AsyncClient(base_url="http://127.0.0.1:8000") as client:
        log("Sending 1st request...")
        r1 = await client.get("/test")
        log(f"1st request response: {r1.status_code}")
        assert r1.status_code == 200

        log("Sending 2nd request...")
        r2 = await client.get("/test")
        log(f"2nd request response: {r2.status_code}")
        assert r2.status_code == 200

        log("Sending 3rd request (should trigger 429)...")
        r3 = await client.get("/test")
        log(f"3rd request response: {r3.status_code} (detail: {r3.text})")
        assert r3.status_code == 429


async def main():
    # Clean up Redis keys from previous runs
    redis_client = aioredis.from_url("redis://localhost:6379")
    await redis_client.delete("throttlekit:fastapi_tb:global")
    await redis_client.aclose()

    log("Starting FastAPI server in the background...")
    # Launch uvicorn background process running the server code
    # We will pass the server app inline by defining the server script and running it
    server_code = """
import uvicorn
from fastapi import FastAPI, Depends
import redis.asyncio as aioredis
from throttlekit import DistributedTokenBucket, RedisBackend
from throttlekit.fastapi import FastAPIRateLimiter

app = FastAPI()
redis_client = aioredis.from_url("redis://localhost:6379")
backend = RedisBackend(redis_client)
limiter = DistributedTokenBucket(backend, max_tokens=2, refill_interval=10.0, name="fastapi_tb")

@app.on_event("startup")
async def startup():
    # Make sure connection is alive
    await redis_client.ping()

@app.get("/test", dependencies=[Depends(FastAPIRateLimiter(limiter, block=False))])
async def test_route():
    return {"status": "ok"}

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000, log_level="warning")
"""
    # Write inline code to a temporary server file
    server_file = "test_project/temp_server.py"
    with open(server_file, "w") as f:
        f.write(server_code)

    # Start the server subprocess
    process = subprocess.Popen(
        [sys.executable, server_file],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    try:
        # Run the HTTP client test
        await run_client()
        log("FastAPI integration test succeeded!")
    finally:
        # Terminate the server process
        process.terminate()
        process.wait()
        # Clean up temp file
        if os.path.exists(server_file):
            os.remove(server_file)
        log("FastAPI server terminated and cleaned up.")


if __name__ == "__main__":
    import os
    asyncio.run(main())
