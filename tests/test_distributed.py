import asyncio
import time
import pytest
from fastapi import FastAPI, Depends
from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import create_async_engine

from throttlekit import DistributedTokenBucket, DistributedLeakyBucket, RedisBackend, SQLBackend
from throttlekit.fastapi import FastAPIRateLimiter, RateLimitMiddleware

pytestmark = pytest.mark.asyncio


# --- Mock Redis Client ---
class MockRedis:
    def __init__(self):
        self.state = {}

    def register_script(self, script_text: str):
        if "emission_interval" in script_text:
            # Leaky Bucket (GCRA) Lua script simulation
            async def leaky_executor(keys, args):
                key = keys[0]
                rate = float(args[0])
                max_queue_size = float(args[1])
                now = float(args[2])

                emission_interval = 1.0 / rate
                delay_tolerance = max_queue_size * emission_interval

                tat = self.state.get(key)

                if tat is None:
                    tat = now
                else:
                    tat = max(tat, now)

                new_tat = tat + emission_interval

                if new_tat - now > delay_tolerance:
                    wait_time = new_tat - now - delay_tolerance
                    return str(wait_time)
                else:
                    self.state[key] = new_tat
                    return "0.0"
            return leaky_executor
        else:
            # Token Bucket Lua script simulation
            async def token_executor(keys, args):
                key = keys[0]
                max_tokens = float(args[0])
                refill_rate = float(args[1])
                requested = float(args[2])
                now = float(args[3])

                if key not in self.state:
                    tokens = max_tokens
                    last_refilled = now
                else:
                    tokens, last_refilled = self.state[key]
                    delta = max(0.0, now - last_refilled)
                    added = delta * refill_rate
                    tokens = min(max_tokens, tokens + added)

                if tokens >= requested:
                    tokens -= requested
                    self.state[key] = (tokens, now)
                    return "0.0"
                else:
                    missing = requested - tokens
                    wait_time = missing / refill_rate
                    self.state[key] = (tokens, now)
                    return str(wait_time)
            return token_executor


# --- Redis Backend Tests ---
async def test_redis_token_bucket():
    redis_client = MockRedis()
    backend = RedisBackend(redis_client)
    limiter = DistributedTokenBucket(backend, max_tokens=2, refill_interval=1.0, name="redis_tb")

    assert await limiter.acquire("user1") is True
    assert await limiter.acquire("user1") is True
    assert await limiter.acquire("user1", block=False) is False


async def test_redis_leaky_bucket():
    redis_client = MockRedis()
    backend = RedisBackend(redis_client)
    limiter = DistributedLeakyBucket(backend, rate=10.0, max_queue_size=1, name="redis_lb")

    assert await limiter.acquire("user1") is True
    assert await limiter.acquire("user1", block=False) is False


async def test_redis_leaky_bucket_lazy_refill():
    redis_client = MockRedis()
    backend = RedisBackend(redis_client)
    limiter = DistributedLeakyBucket(backend, rate=10.0, max_queue_size=1, name="redis_lb_lazy")

    assert await limiter.acquire("user1") is True
    assert await limiter.acquire("user1", block=False) is False
    
    # Wait for leakage (1/10 = 0.1s)
    await asyncio.sleep(0.12)
    assert await limiter.acquire("user1", block=False) is True


# --- SQL Backend Tests ---
async def test_sql_token_bucket():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    backend = SQLBackend(engine)
    limiter = DistributedTokenBucket(backend, max_tokens=2, refill_interval=1.0, name="sql_tb")

    assert await limiter.acquire("user1") is True
    assert await limiter.acquire("user1") is True
    assert await limiter.acquire("user1", block=False) is False

    await engine.dispose()


async def test_sql_leaky_bucket():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    backend = SQLBackend(engine)
    limiter = DistributedLeakyBucket(backend, rate=10.0, max_queue_size=1, name="sql_lb")

    assert await limiter.acquire("user1") is True
    assert await limiter.acquire("user1", block=False) is False

    await engine.dispose()


# --- Decorators & Context Managers ---
async def test_distributed_decorators_and_contexts():
    redis_client = MockRedis()
    backend = RedisBackend(redis_client)
    
    # Token Bucket Decorator
    tb_limiter = DistributedTokenBucket(backend, max_tokens=1, refill_interval=10.0, name="deco_tb")
    
    @tb_limiter.limit(key="tbkey", block=False)
    async def tb_task():
        pass

    await tb_task()
    with pytest.raises(RuntimeError, match="Rate limit exceeded"):
        await tb_task()

    # Leaky Bucket Decorator
    lb_limiter = DistributedLeakyBucket(backend, rate=10.0, max_queue_size=1, name="deco_lb")

    @lb_limiter.limit(key="lbkey", block=False)
    async def lb_task():
        pass

    await lb_task()
    with pytest.raises(RuntimeError, match="Rate limit exceeded"):
        await lb_task()

    # Leaky Context Manager
    async with lb_limiter(key="ctx", block=True):
        pass


# --- FastAPI Dependency Tests ---
async def test_fastapi_dependencies():
    redis_client = MockRedis()
    backend = RedisBackend(redis_client)
    
    tb_limiter = DistributedTokenBucket(backend, max_tokens=1, refill_interval=10.0, name="fastapi_tb")
    lb_limiter = DistributedLeakyBucket(backend, rate=10.0, max_queue_size=1, name="fastapi_lb")

    app = FastAPI()

    # FastAPI Dependency for DistributedTokenBucket
    @app.get("/tb", dependencies=[Depends(FastAPIRateLimiter(tb_limiter, block=False))])
    def route_tb():
        return {"status": "ok"}

    # FastAPI Dependency for DistributedLeakyBucket
    @app.get("/lb", dependencies=[Depends(FastAPIRateLimiter(lb_limiter, block=False))])
    def route_lb():
        return {"status": "ok"}

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        # Token Bucket
        assert (await ac.get("/tb")).status_code == 200
        assert (await ac.get("/tb")).status_code == 429

        # Leaky Bucket
        assert (await ac.get("/lb")).status_code == 200
        assert (await ac.get("/lb")).status_code == 429


# --- FastAPI Middleware Tests ---
async def test_fastapi_middleware():
    redis_client = MockRedis()
    backend = RedisBackend(redis_client)
    
    tb_limiter = DistributedTokenBucket(backend, max_tokens=1, refill_interval=10.0, name="middleware_tb")
    lb_limiter = DistributedLeakyBucket(backend, rate=10.0, max_queue_size=1, name="middleware_lb")

    # Token Bucket Middleware App
    app_tb = FastAPI()
    app_tb.add_middleware(RateLimitMiddleware, limiter=tb_limiter, block=False)

    @app_tb.get("/test")
    def route():
        return {"status": "ok"}

    # Leaky Bucket Middleware App
    app_lb = FastAPI()
    app_lb.add_middleware(RateLimitMiddleware, limiter=lb_limiter, block=False)

    @app_lb.get("/test")
    def route_lb():
        return {"status": "ok"}

    # Test Token Bucket Middleware
    async with AsyncClient(transport=ASGITransport(app=app_tb), base_url="http://test") as ac:
        assert (await ac.get("/test")).status_code == 200
        assert (await ac.get("/test")).status_code == 429

    # Test Leaky Bucket Middleware
    async with AsyncClient(transport=ASGITransport(app=app_lb), base_url="http://test") as ac:
        assert (await ac.get("/test")).status_code == 200
        assert (await ac.get("/test")).status_code == 429


# --- FastAPI Middleware Blocking Tests ---
async def test_fastapi_middleware_blocking():
    redis_client = MockRedis()
    backend = RedisBackend(redis_client)
    
    lb_limiter = DistributedLeakyBucket(backend, rate=10.0, max_queue_size=1, name="blocking_middleware_lb")

    app = FastAPI()
    app.add_middleware(RateLimitMiddleware, limiter=lb_limiter, block=True)

    @app.get("/test")
    def route():
        return {"status": "ok"}

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        # First request consumes slots
        assert (await ac.get("/test")).status_code == 200
        # Second request blocks and eventually succeeds (after wait_time)
        assert (await ac.get("/test")).status_code == 200


async def test_invalid_arguments():
    redis_client = MockRedis()
    backend = RedisBackend(redis_client)

    with pytest.raises(ValueError, match="max_tokens must be > 0"):
        DistributedTokenBucket(backend, max_tokens=0)
    with pytest.raises(ValueError, match="refill_interval must be > 0"):
        DistributedTokenBucket(backend, max_tokens=1, refill_interval=-0.5)

    with pytest.raises(ValueError, match="rate must be > 0"):
        DistributedLeakyBucket(backend, rate=0)
    with pytest.raises(ValueError, match="max_queue_size must be > 0"):
        DistributedLeakyBucket(backend, rate=1.0, max_queue_size=-1)


async def test_distributed_token_bucket_context_manager_fail():
    redis_client = MockRedis()
    backend = RedisBackend(redis_client)
    limiter = DistributedTokenBucket(backend, max_tokens=1, refill_interval=10.0, name="tb_ctx_fail")

    async with limiter(key="ctx", block=True):
        pass

    with pytest.raises(RuntimeError, match="Rate limit exceeded"):
        async with limiter(key="ctx", block=False):
            pass


async def test_distributed_token_bucket_blocking_sleep():
    redis_client = MockRedis()
    backend = RedisBackend(redis_client)
    limiter = DistributedTokenBucket(backend, max_tokens=1, refill_interval=0.05, name="tb_sleep")

    assert await limiter.acquire("user1") is True
    # Second acquisition will block/sleep for ~0.05s and succeed
    start = time.time()
    assert await limiter.acquire("user1", block=True) is True
    assert time.time() - start >= 0.04


async def test_distributed_logger_input():
    import logging
    my_logger = logging.getLogger("custom_dist_logger")
    redis_client = MockRedis()
    backend = RedisBackend(redis_client)
    
    tb = DistributedTokenBucket(backend, max_tokens=2, name="tb_log", logger=my_logger)
    assert tb._logger is my_logger

    lb = DistributedLeakyBucket(backend, rate=2.0, name="lb_log", logger=my_logger)
    assert lb._logger is my_logger


async def test_sql_backend_schema():
    from sqlalchemy.ext.asyncio import create_async_engine
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    
    backend = SQLBackend(engine, table_name="custom_table", schema="custom_schema")
    assert backend.schema == "custom_schema"
    assert backend.table_name == "custom_table"
    assert backend.full_table_name == "custom_schema.custom_table"
    assert backend.full_leaky_table_name == "custom_schema.custom_table_leaky"
    
    await engine.dispose()



