import asyncio
import pytest
from fastapi import FastAPI, Depends
from httpx import AsyncClient, ASGITransport
from throttlekit import TokenBucketRateLimiter, LeakyBucketRateLimiter
from throttlekit.fastapi import FastAPIRateLimiter, RateLimitMiddleware

pytestmark = pytest.mark.asyncio


async def test_fastapi_dependency_token_bucket_blocking():
    limiter = TokenBucketRateLimiter(max_tokens=2, refill_interval=1.0)
    await limiter.start()

    app = FastAPI()
    dependency = FastAPIRateLimiter(limiter, block=True)

    @app.get("/test")
    def route(dep=Depends(dependency)):
        return {"status": "ok"}

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response1 = await ac.get("/test")
        response2 = await ac.get("/test")
        assert response1.status_code == 200
        assert response2.status_code == 200

    await limiter.stop()


async def test_fastapi_dependency_token_bucket_non_blocking():
    limiter = TokenBucketRateLimiter(max_tokens=2, refill_interval=10.0)
    await limiter.start()

    app = FastAPI()
    dependency = FastAPIRateLimiter(limiter, block=False)

    @app.get("/test")
    def route(dep=Depends(dependency)):
        return {"status": "ok"}

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response1 = await ac.get("/test")
        response2 = await ac.get("/test")
        response3 = await ac.get("/test")

        assert response1.status_code == 200
        assert response2.status_code == 200
        assert response3.status_code == 429
        assert response3.json() == {"detail": "Too Many Requests"}

    await limiter.stop()


async def test_fastapi_dependency_leaky_bucket_non_blocking():
    limiter = LeakyBucketRateLimiter(rate=10.0, max_queue_size=1)
    await limiter.start()

    app = FastAPI()
    dependency = FastAPIRateLimiter(limiter, block=False)

    @app.get("/test")
    async def route(dep=Depends(dependency)):
        return {"status": "ok"}

    # Fill the queue directly to force next request to fail with 429
    loop = asyncio.get_running_loop()
    fut1 = loop.create_future()
    await limiter.queue.put(fut1)
    fut2 = loop.create_future()
    await limiter.queue.put(fut2)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/test")
        assert response.status_code == 429

    await limiter.stop()


async def test_fastapi_dependency_leaky_bucket_non_blocking_success():
    limiter = LeakyBucketRateLimiter(rate=10.0, max_queue_size=2)
    await limiter.start()

    app = FastAPI()
    dependency = FastAPIRateLimiter(limiter, block=False)

    @app.get("/test")
    async def route(dep=Depends(dependency)):
        return {"status": "ok"}

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/test")
        assert response.status_code == 200

    await limiter.stop()


async def test_fastapi_dependency_token_bucket_concurrency_limit_non_blocking():
    limiter = TokenBucketRateLimiter(max_tokens=2, concurrency_limit=1, refill_interval=10.0)
    await limiter.start()

    app = FastAPI()
    dependency = FastAPIRateLimiter(limiter, block=False)

    # Let's acquire the semaphore to lock it
    assert limiter.semaphore is not None
    await limiter.semaphore.acquire()

    @app.get("/test")
    def route(dep=Depends(dependency)):
        return {"status": "ok"}

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/test")
        assert response.status_code == 429

    limiter.release_semaphore()
    await limiter.stop()


async def test_fastapi_dependency_token_bucket_concurrency_limit_non_blocking_success():
    limiter = TokenBucketRateLimiter(max_tokens=2, concurrency_limit=1, refill_interval=10.0)
    await limiter.start()

    app = FastAPI()
    dependency = FastAPIRateLimiter(limiter, block=False)

    @app.get("/test")
    def route(dep=Depends(dependency)):
        return {"status": "ok"}

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/test")
        assert response.status_code == 200

    await limiter.stop()


async def test_fastapi_middleware_token_bucket_non_blocking():
    limiter = TokenBucketRateLimiter(max_tokens=1, refill_interval=10.0)
    await limiter.start()

    app = FastAPI()
    app.add_middleware(RateLimitMiddleware, limiter=limiter, block=False, detail="Custom Limit Exceeded")

    @app.get("/test")
    def route():
        return {"status": "ok"}

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response1 = await ac.get("/test")
        response2 = await ac.get("/test")

        assert response1.status_code == 200
        assert response2.status_code == 429
        assert response2.text == "Custom Limit Exceeded"

    await limiter.stop()


async def test_fastapi_middleware_token_bucket_concurrency_limit_non_blocking():
    limiter = TokenBucketRateLimiter(max_tokens=2, concurrency_limit=1, refill_interval=10.0)
    await limiter.start()

    app = FastAPI()
    app.add_middleware(RateLimitMiddleware, limiter=limiter, block=False, detail="Custom Concurrency Limit Exceeded")

    # Lock the semaphore
    assert limiter.semaphore is not None
    await limiter.semaphore.acquire()

    @app.get("/test")
    def route():
        return {"status": "ok"}

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/test")
        assert response.status_code == 429
        assert response.text == "Custom Concurrency Limit Exceeded"

    limiter.release_semaphore()
    await limiter.stop()


async def test_fastapi_middleware_token_bucket_concurrency_limit_non_blocking_success():
    limiter = TokenBucketRateLimiter(max_tokens=2, concurrency_limit=1, refill_interval=10.0)
    await limiter.start()

    app = FastAPI()
    app.add_middleware(RateLimitMiddleware, limiter=limiter, block=False)

    @app.get("/test")
    def route():
        return {"status": "ok"}

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/test")
        assert response.status_code == 200

    await limiter.stop()


async def test_fastapi_middleware_leaky_bucket_non_blocking_full():
    limiter = LeakyBucketRateLimiter(rate=10.0, max_queue_size=1)
    await limiter.start()

    app = FastAPI()
    app.add_middleware(RateLimitMiddleware, limiter=limiter, block=False, detail="Queue Full")

    # Fill the queue directly
    loop = asyncio.get_running_loop()
    fut1 = loop.create_future()
    await limiter.queue.put(fut1)
    fut2 = loop.create_future()
    await limiter.queue.put(fut2)

    @app.get("/test")
    def route():
        return {"status": "ok"}

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/test")
        assert response.status_code == 429
        assert response.text == "Queue Full"

    await limiter.stop()


async def test_fastapi_middleware_leaky_bucket_non_blocking_success():
    limiter = LeakyBucketRateLimiter(rate=10.0, max_queue_size=2)
    await limiter.start()

    app = FastAPI()
    app.add_middleware(RateLimitMiddleware, limiter=limiter, block=False)

    @app.get("/test")
    def route():
        return {"status": "ok"}

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/test")
        assert response.status_code == 200

    await limiter.stop()


async def test_fastapi_middleware_token_bucket_blocking():
    limiter = TokenBucketRateLimiter(max_tokens=2, refill_interval=1.0)
    await limiter.start()

    app = FastAPI()
    app.add_middleware(RateLimitMiddleware, limiter=limiter, block=True)

    @app.get("/test")
    def route():
        return {"status": "ok"}

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response1 = await ac.get("/test")
        response2 = await ac.get("/test")
        assert response1.status_code == 200
        assert response2.status_code == 200

    await limiter.stop()


async def test_fastapi_logger_input():
    import logging
    my_logger = logging.getLogger("custom_fastapi_logger")
    limiter = TokenBucketRateLimiter(max_tokens=2)
    
    dep = FastAPIRateLimiter(limiter, logger=my_logger)
    assert dep._logger is my_logger

    app = FastAPI()
    mw = RateLimitMiddleware(app, limiter=limiter, logger=my_logger)
    assert mw._logger is my_logger

