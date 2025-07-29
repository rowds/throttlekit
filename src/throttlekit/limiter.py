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
        self.max_tokens: int = max_tokens
        self.refill_interval: float = refill_interval
        self.bucket: asyncio.Queue[int] = asyncio.Queue(maxsize=max_tokens)
        self.semaphore: Optional[asyncio.Semaphore] = asyncio.Semaphore(concurrency_limit) if concurrency_limit else None
        self.started: bool = False
        self.logger: logging.Logger = logger or logging.getLogger(__name__)

    async def start(self) -> None:
        if self.started:
            return
        self.started = True
        for _ in range(self.max_tokens):
            self.bucket.put_nowait(1)
        asyncio.create_task(self._refill())
        self.logger.debug("Rate limiter started with %d tokens", self.max_tokens)

    async def _refill(self) -> None:
        while True:
            await asyncio.sleep(self.refill_interval)
            for _ in range(self.max_tokens - self.bucket.qsize()):
                try:
                    self.bucket.put_nowait(1)
                except asyncio.QueueFull:
                    break

    async def acquire(self) -> None:
        if not self.started:
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
