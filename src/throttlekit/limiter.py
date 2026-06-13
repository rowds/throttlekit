import asyncio
from functools import wraps
from typing import Callable, Awaitable, TypeVar, Optional, Union
import logging

try:
    from typing import ParamSpec
except ImportError:
    from typing_extensions import ParamSpec  # type: ignore

T = TypeVar("T")
P = ParamSpec("P")

class TokenBucketRateLimiter:
    def __init__(
        self,
        max_tokens: int,
        refill_interval: float = 1.0,
        concurrency_limit: Optional[int] = None,
        logger: Optional[logging.Logger] = None
    ) -> None:
        if max_tokens <= 0:
            raise ValueError("max_tokens must be > 0")
        if refill_interval <= 0:
            raise ValueError("refill_interval must be > 0")
        if concurrency_limit is not None and concurrency_limit <= 0:
            raise ValueError("concurrency_limit must be > 0")

        self.max_tokens: int = max_tokens
        self.refill_interval: float = refill_interval
        self.bucket: asyncio.Queue[int] = asyncio.Queue(maxsize=max_tokens)
        self.semaphore: Optional[asyncio.Semaphore] = asyncio.Semaphore(concurrency_limit) if concurrency_limit else None
        self._started: bool = False
        self._logger: logging.Logger = logger or logging.getLogger(__name__)
        self._refill_task: Optional[asyncio.Task[None]] = None

    async def start(self) -> None:
        """Start the rate limiter and begin refilling tokens."""
        if self._started:
            return
        self._started = True
        for _ in range(self.max_tokens):
            self.bucket.put_nowait(1)
        self._refill_task = asyncio.create_task(self._refill())
        self._logger.debug("Rate limiter started with %d tokens", self.max_tokens)

    async def stop(self) -> None:
        """Stop the rate limiter and cancel the refill task."""
        if not self._started:
            return
        self._started = False
        if self._refill_task is not None:
            self._refill_task.cancel()
            try:
                await self._refill_task
            except asyncio.CancelledError:
                pass
            self._refill_task = None
        self._logger.debug("Rate limiter stopped")

    async def _refill(self) -> None:
        while True:
            await asyncio.sleep(self.refill_interval)
            for _ in range(self.max_tokens - self.bucket.qsize()):
                try:
                    self.bucket.put_nowait(1)
                except asyncio.QueueFull:
                    break

    async def acquire(self) -> None:
        if not self._started:
            raise RuntimeError("RateLimiter not started. Call await limiter.start() before use.")
        await self.bucket.get()
        if self.semaphore:
            await self.semaphore.acquire()

    def release_token(self) -> None:
        """Refund a token manually (not normally used)."""
        try:
            self.bucket.put_nowait(1)
        except asyncio.QueueFull:
            pass

    def release_semaphore(self) -> None:
        """Manually release the semaphore if acquired outside context."""
        if self.semaphore:
            self.semaphore.release()

    async def __aenter__(self) -> "TokenBucketRateLimiter":
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> Optional[bool]:
        self.release_semaphore()
        return None
    
    def _release(self) -> None:
        """Internal helper for releasing either semaphore or bucket."""
        self.release_semaphore()
        self.release_token()

    def limit(
        self,
        fn: Optional[Callable[..., Awaitable[T]]] = None,
        *,
        tokens: int = 1
    ) -> Union[
        Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]],
        Callable[..., Awaitable[T]]
    ]:

        def decorator(func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
            @wraps(func)
            async def wrapper(*args, **kwargs) -> T:
                for _ in range(tokens):
                    await self.acquire()
                try:
                    return await func(*args, **kwargs)
                finally:
                    for _ in range(tokens):
                        self._release()
            return wrapper

        if fn is None:
            return decorator
        else:
            return decorator(fn)
