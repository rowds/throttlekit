import asyncio
import os
import time
import redis.asyncio as aioredis
from sqlalchemy.ext.asyncio import create_async_engine

from throttlekit import (
    DistributedTokenBucket,
    DistributedLeakyBucket,
    RedisBackend,
    SQLBackend
)


def log(msg: str):
    timestamp = time.strftime("%H:%M:%S")
    print(f"[{timestamp}] {msg}")


async def run_redis_tests(redis_client):
    log("--- Running Redis Backend Tests ---")
    backend = RedisBackend(redis_client)
    
    # 1. DistributedTokenBucket (Redis)
    # 2 tokens max, refills every 3.0 seconds
    limiter = DistributedTokenBucket(backend, max_tokens=2, refill_interval=3.0, name="redis_tb_demo")
    
    log("[TokenBucket] Acquiring 2 tokens immediately...")
    assert await limiter.acquire("user_demo", block=False) is True
    log("[TokenBucket] Token 1 acquired successfully.")
    assert await limiter.acquire("user_demo", block=False) is True
    log("[TokenBucket] Token 2 acquired successfully.")
    
    log("[TokenBucket] Attempting 3rd token (non-blocking)...")
    success = await limiter.acquire("user_demo", block=False)
    log(f"[TokenBucket] 3rd token status: {'Success' if success else 'Rate-Limited (As Expected)'}")
    assert success is False
    
    log("[TokenBucket] Now waiting 3.0 seconds for tokens to refill...")
    await asyncio.sleep(3.1)
    
    log("[TokenBucket] Retrying 3rd token after refill...")
    assert await limiter.acquire("user_demo", block=False) is True
    log("[TokenBucket] 3rd token acquired successfully after refill!")

    # 2. DistributedLeakyBucket (Redis)
    # Rate of 0.5 requests/sec (1 request every 2.0 seconds), queue size of 1
    leaky = DistributedLeakyBucket(backend, rate=0.5, max_queue_size=1, name="redis_lb_demo")
    
    log("[LeakyBucket] Acquiring 1st request...")
    assert await leaky.acquire("user_demo", block=False) is True
    log("[LeakyBucket] 1st request acquired.")
    
    log("[LeakyBucket] Attempting 2nd request immediately (non-blocking)...")
    success = await leaky.acquire("user_demo", block=False)
    log(f"[LeakyBucket] 2nd request status: {'Success' if success else 'Rate-Limited (As Expected due to 2.0s spacing)'}")
    assert success is False
    
    log("[LeakyBucket] Attempting 2nd request with block=True (should block for ~2.0s)...")
    start = time.time()
    await leaky.acquire("user_demo", block=True)
    duration = time.time() - start
    log(f"[LeakyBucket] 2nd request acquired successfully after blocking for {duration:.2f} seconds!")


async def run_sql_tests(engine):
    log("--- Running SQLite Backend Tests ---")
    backend = SQLBackend(engine)

    # 1. DistributedTokenBucket (SQL)
    # 2 tokens max, refills every 3.0 seconds
    limiter = DistributedTokenBucket(backend, max_tokens=2, refill_interval=3.0, name="sql_tb_demo")
    
    log("[TokenBucket] Acquiring 2 tokens immediately...")
    assert await limiter.acquire("user_demo", block=False) is True
    log("[TokenBucket] Token 1 acquired.")
    assert await limiter.acquire("user_demo", block=False) is True
    log("[TokenBucket] Token 2 acquired.")
    
    log("[TokenBucket] Attempting 3rd token (non-blocking)...")
    success = await limiter.acquire("user_demo", block=False)
    log(f"[TokenBucket] 3rd token status: {'Success' if success else 'Rate-Limited (As Expected)'}")
    assert success is False
    
    log("[TokenBucket] Now waiting 3.0 seconds for tokens to refill...")
    await asyncio.sleep(3.1)
    
    log("[TokenBucket] Retrying 3rd token after refill...")
    assert await limiter.acquire("user_demo", block=False) is True
    log("[TokenBucket] 3rd token acquired successfully after refill!")

    # 2. DistributedLeakyBucket (SQL)
    # Rate of 0.5 requests/sec (1 request every 2.0 seconds), queue size of 1
    leaky = DistributedLeakyBucket(backend, rate=0.5, max_queue_size=1, name="sql_lb_demo")
    
    log("[LeakyBucket] Acquiring 1st request...")
    assert await leaky.acquire("user_demo", block=False) is True
    log("[LeakyBucket] 1st request acquired.")
    
    log("[LeakyBucket] Attempting 2nd request immediately (non-blocking)...")
    success = await leaky.acquire("user_demo", block=False)
    log(f"[LeakyBucket] 2nd request status: {'Success' if success else 'Rate-Limited (As Expected)'}")
    assert success is False
    
    log("[LeakyBucket] Attempting 2nd request with block=True (should block for ~2.0s)...")
    start = time.time()
    await leaky.acquire("user_demo", block=True)
    duration = time.time() - start
    log(f"[LeakyBucket] 2nd request acquired successfully after blocking for {duration:.2f} seconds!")


async def main():
    log("Initializing test connections...")
    
    # Connect to Redis
    redis_client = aioredis.from_url("redis://localhost:6379")
    
    # Connect to local SQLite file
    db_file = "test_limiter.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_file}")

    try:
        # Run tests
        await run_redis_tests(redis_client)
        print()
        await run_sql_tests(engine)
        print()
        log("🎉 All integration checks passed successfully!")
    finally:
        # Clean up database engine
        await engine.dispose()
        # Clean up SQLite file
        if os.path.exists(db_file):
            os.remove(db_file)
            log(f"Cleaned up local database file: {db_file}")
        # Clean up Redis keys
        await redis_client.delete(
            "throttlekit:redis_tb_demo:user_demo",
            "throttlekit:redis_lb_demo:user_demo",
            "throttlekit:sql_tb_demo:user_demo",
            "throttlekit:sql_lb_demo:user_demo"
        )
        log("Cleaned up temporary Redis keys.")
        await redis_client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
