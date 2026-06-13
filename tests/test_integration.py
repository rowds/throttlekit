import asyncio
import multiprocessing
import socket
import subprocess
import sys
import time
import httpx
import pytest

# Check if redis is available on localhost:6379
try:
    import redis
    r = redis.Redis(host="localhost", port=6379)
    r.ping()
    redis_available = True
    r.close()
except Exception:
    redis_available = False


def get_free_port() -> int:
    s = socket.socket()
    s.bind(("", 0))
    port = s.getsockname()[1]
    s.close()
    return port


@pytest.mark.skipif(not redis_available, reason="Redis is not running on localhost:6379")
def test_fastapi_server_integration():
    # Clean up Redis keys from previous runs using synchronous redis client
    r = redis.Redis(host="localhost", port=6379)
    r.delete("throttlekit:fastapi_tb_integration:global")
    r.close()

    port = get_free_port()
    server_code = f"""
import uvicorn
from fastapi import FastAPI, Depends
import redis.asyncio as aioredis
from throttlekit import DistributedTokenBucket, RedisBackend
from throttlekit.fastapi import FastAPIRateLimiter

app = FastAPI()
redis_client = aioredis.from_url("redis://localhost:6379")
backend = RedisBackend(redis_client)
limiter = DistributedTokenBucket(backend, max_tokens=2, refill_interval=10.0, name="fastapi_tb_integration")

@app.on_event("startup")
async def startup():
    await redis_client.ping()

@app.get("/health")
async def health():
    return {{"status": "ok"}}

@app.get("/test", dependencies=[Depends(FastAPIRateLimiter(limiter, block=False))])
async def test_route():
    return {{"status": "ok"}}

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port={port}, log_level="warning")
"""

    process = subprocess.Popen(
        [sys.executable, "-c", server_code],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    try:
        # Wait for the server to start up (up to 5 seconds)
        client = httpx.Client(base_url=f"http://127.0.0.1:{port}")
        for _ in range(50):
            try:
                response = client.get("/health")
                # If we get a response, the server is ready
                if response.status_code == 200:
                    break
            except httpx.RequestError:
                time.sleep(0.1)
        else:
            pytest.fail("FastAPI server failed to start within 5 seconds")

        # 1st request should succeed
        r1 = client.get("/test")
        assert r1.status_code == 200

        # 2nd request should succeed
        r2 = client.get("/test")
        assert r2.status_code == 200

        # 3rd request should be rate limited
        r3 = client.get("/test")
        assert r3.status_code == 429
    finally:
        process.terminate()
        process.wait()


def worker_task(success_counter, rate_limit_counter):
    import asyncio
    import redis.asyncio as aioredis
    from throttlekit import DistributedTokenBucket, RedisBackend

    async def run():
        redis_client = aioredis.from_url("redis://localhost:6379")
        backend = RedisBackend(redis_client)
        limiter = DistributedTokenBucket(backend, max_tokens=5, refill_interval=10.0, name="mp_tb_integration")

        # Non-blocking acquisition
        acquired = await limiter.acquire("global_mp_key", block=False)
        if acquired:
            success_counter.value += 1
        else:
            rate_limit_counter.value += 1

        await redis_client.aclose()

    asyncio.run(run())


@pytest.mark.skipif(not redis_available, reason="Redis is not running on localhost:6379")
def test_multiprocessing_integration():
    # Clean up Redis keys from previous runs using synchronous redis client
    r = redis.Redis(host="localhost", port=6379)
    r.delete("throttlekit:mp_tb_integration:global_mp_key")
    r.close()

    success_counter = multiprocessing.Value('i', 0)
    rate_limit_counter = multiprocessing.Value('i', 0)

    processes = []
    for _ in range(10):
        p = multiprocessing.Process(
            target=worker_task,
            args=(success_counter, rate_limit_counter)
        )
        processes.append(p)
        p.start()

    for p in processes:
        p.join()

    # With max_tokens=5, we expect exactly 5 successes and 5 failures/rate-limits
    assert success_counter.value == 5
    assert rate_limit_counter.value == 5
