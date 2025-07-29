import asyncio
import pytest
import pytest_asyncio
from throttlekit import TokenBucketRateLimiter

pytestmark = pytest.mark.asyncio

@pytest_asyncio.fixture
async def limiter_factory():
    async def _create(
        max_tokens: int = 3,
        refill_interval: float = 1,
        concurrency_limit: int | None = None
    ) -> TokenBucketRateLimiter:
        limiter = TokenBucketRateLimiter(
            max_tokens=max_tokens,
            refill_interval=refill_interval,
            concurrency_limit=concurrency_limit
        )
        await limiter.start()
        return limiter
    return _create


async def test_acquire_all_tokens(limiter_factory):
    limiter = await limiter_factory(max_tokens=3, refill_interval=1.0)
    await limiter.acquire()
    await limiter.acquire()
    await limiter.acquire()

    acquire_future = asyncio.create_task(limiter.acquire())
    try:
        await asyncio.wait_for(acquire_future, timeout=0.2)
        assert False, "Should not acquire before refill"
    except asyncio.TimeoutError:
        pass

async def test_refill_tokens(limiter_factory):
    limiter = await limiter_factory(max_tokens=2, refill_interval=0.3)
    await limiter.acquire()
    await limiter.acquire()
    await asyncio.sleep(0.4)
    await limiter.acquire()

async def test_concurrency_limit(limiter_factory):
    limiter = await limiter_factory(max_tokens=5, refill_interval=1.0, concurrency_limit=2)
    running = 0
    max_running = 0
    lock = asyncio.Lock()

    async def task():
        nonlocal running, max_running
        async with limiter:
            async with lock:
                running += 1
                max_running = max(max_running, running)
            await asyncio.sleep(0.2)
            async with lock:
                running -= 1

    await asyncio.gather(*[task() for _ in range(10)])
    assert max_running <= 2

async def test_release_token_manually(limiter_factory):
    limiter = await limiter_factory(max_tokens=1, refill_interval=10.0)
    await limiter.acquire()
    task = asyncio.create_task(limiter.acquire())
    await asyncio.sleep(0.2)
    assert not task.done()
    limiter.release_token()
    await asyncio.wait_for(task, timeout=0.2)

async def test_double_start_safe(limiter_factory):
    limiter = await limiter_factory(max_tokens=2, refill_interval=1.0)
    await limiter.start()  # called again — should not raise
    await limiter.acquire()
    await limiter.acquire()

async def test_release_token_on_full_bucket(limiter_factory):
    limiter = await limiter_factory(max_tokens=1)
    # Bucket is full after start
    limiter.release_token()  # Should silently pass (QueueFull caught)


async def test_release_semaphore_without_concurrency_limit():
    limiter = TokenBucketRateLimiter(max_tokens=1)
    await limiter.start()
    # No semaphore created
    limiter.release_semaphore()  # Should be a no-op


async def test_context_manager_behavior(limiter_factory):
    limiter = await limiter_factory(max_tokens=1, concurrency_limit=1)
    async with limiter:
        assert True  # Inside context, token and semaphore acquired
    # After context, semaphore should be released

async def test_decorator_default_tokens(limiter_factory):
    limiter = await limiter_factory(max_tokens=1)

    @limiter.limit
    async def decorated_func():
        return "ok"

    result = await decorated_func()
    assert result == "ok"

async def test_decorator_custom_tokens(limiter_factory):
    limiter = await limiter_factory(max_tokens=2)

    @limiter.limit(tokens=2)
    async def decorated():
        return "ok"

    result = await decorated()
    assert result == "ok"

async def test_decorator_token_refund_on_exception(limiter_factory):
    limiter = await limiter_factory(max_tokens=2)

    @limiter.limit(tokens=2)
    async def error_func():
        raise ValueError("boom")

    with pytest.raises(ValueError):
        await error_func()

    # After exception, tokens should be refunded
    await limiter.acquire()
    await limiter.acquire()

async def test_refill_cannot_overfill(limiter_factory):
    limiter = await limiter_factory(max_tokens=2, refill_interval=0.1)
    await asyncio.sleep(0.3)
    # Tokens should not exceed max_tokens
    assert limiter.bucket.qsize() == 2


async def test_acquire_without_start():
    """Test that acquire without start raises RuntimeError"""
    limiter = TokenBucketRateLimiter(max_tokens=1)
    with pytest.raises(RuntimeError, match="RateLimiter not started"):
        await limiter.acquire()


async def test_refill_queue_full_exception():
    """Test that _refill handles QueueFull exception gracefully"""
    # Create a limiter that will definitely trigger QueueFull
    limiter = TokenBucketRateLimiter(max_tokens=1, refill_interval=0.01)
    
    # Start the limiter - this fills the bucket with 1 token
    await limiter.start()
    
    # Consume the token to make room
    await limiter.acquire()
    
    # Now wait for refill to add a token back
    await asyncio.sleep(0.02)
    
    # The bucket should now have 1 token again
    original_qsize = limiter.bucket.qsize()
    
    # Wait for multiple refill cycles - the refill loop should try to add tokens
    await asyncio.sleep(0.1)  # Multiple refill intervals
    
    # Verify the bucket size hasn't exceeded max_tokens
    assert limiter.bucket.qsize() <= limiter.max_tokens
    
    # Verify the limiter still works
    await limiter.acquire()
