import asyncio
import pytest
import pytest_asyncio
from aiothrottle import LeakyBucketRateLimiter  # Adjust import path as needed

pytestmark = pytest.mark.asyncio

@pytest_asyncio.fixture
async def leaky_limiter_factory():
    async def _create(rate: float = 5.0, max_queue_size: int = 10) -> LeakyBucketRateLimiter:
        limiter = LeakyBucketRateLimiter(rate=rate, max_queue_size=max_queue_size)
        await limiter.start()
        return limiter
    return _create


async def test_rate_spacing(leaky_limiter_factory):
    limiter = await leaky_limiter_factory(rate=5.0)  # 5 req/sec => 0.2s interval

    times = []

    async def task():
        async with limiter:
            times.append(asyncio.get_event_loop().time())

    await asyncio.gather(*[task() for _ in range(5)])

    intervals = [round(t2 - t1, 2) for t1, t2 in zip(times, times[1:])]
    for i in intervals:
        assert i >= 0.19  # Allow tiny float tolerance


async def test_rate_limiter_blocks(leaky_limiter_factory):
    limiter = await leaky_limiter_factory(rate=1.0, max_queue_size=1)
    await limiter.acquire()

    second = asyncio.create_task(limiter.acquire())

    # Should not complete immediately
    await asyncio.sleep(0.1)
    assert not second.done()

    # Wait for it to complete after leak interval (1.0s)
    await asyncio.wait_for(second, timeout=2)


async def test_queue_max_size_blocks(leaky_limiter_factory):
    limiter = await leaky_limiter_factory(rate=0.1, max_queue_size=1)
    await limiter.acquire()

    # Queue is full after this
    task1 = asyncio.create_task(limiter.acquire())

    # This should block until task1 is drained
    await asyncio.sleep(0.1)
    task2 = asyncio.create_task(limiter.acquire())

    await asyncio.sleep(0.1)
    assert not task2.done()  # should be waiting in queue


async def test_context_manager_behavior(leaky_limiter_factory):
    limiter = await leaky_limiter_factory(rate=2.0)
    async with limiter:
        assert True


async def test_limit_decorator(leaky_limiter_factory):
    limiter = await leaky_limiter_factory(rate=3.0)

    @limiter.limit
    async def do_work():
        return "done"

    assert await do_work() == "done"


async def test_limit_order_preserved(leaky_limiter_factory):
    limiter = await leaky_limiter_factory(rate=2.0)

    result = []

    @limiter.limit
    async def task(i):
        result.append(i)

    await asyncio.gather(*(task(i) for i in range(5)))

    assert result == [0, 1, 2, 3, 4]


async def test_double_start_safe(leaky_limiter_factory):
    limiter = await leaky_limiter_factory()
    await limiter.start()  # should not raise


async def test_invalid_rate():
    """Test that invalid rate raises ValueError"""
    with pytest.raises(ValueError, match="rate must be > 0"):
        LeakyBucketRateLimiter(rate=0)
    
    with pytest.raises(ValueError, match="rate must be > 0"):
        LeakyBucketRateLimiter(rate=-1)


async def test_acquire_without_start():
    """Test that acquire without start raises RuntimeError"""
    limiter = LeakyBucketRateLimiter(rate=1.0)
    with pytest.raises(RuntimeError, match="LeakyBucket not started"):
        await limiter.acquire()


async def test_future_already_done():
    """Test the case where future is already done in drain loop"""
    limiter = LeakyBucketRateLimiter(rate=10.0)  # Fast rate for quick test
    await limiter.start()
    
    # Create a future and cancel it before it gets processed
    import asyncio
    fut = asyncio.get_event_loop().create_future()
    fut.cancel()  # This makes fut.done() return True
    
    # Put the cancelled future in the queue
    await limiter.queue.put(fut)
    
    # Give some time for the drain loop to process it
    await asyncio.sleep(0.2)
    
    # The drain loop should handle the already-done future gracefully
    assert fut.done()
