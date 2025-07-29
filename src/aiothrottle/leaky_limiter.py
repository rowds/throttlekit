import asyncio
import logging
from functools import wraps
from typing import Callable, Awaitable, Optional, TypeVar, Union

try:
    from typing import ParamSpec
except ImportError:
    from typing_extensions import ParamSpec  # type: ignore

T = TypeVar("T")
P = ParamSpec("P")

class LeakyBucketRateLimiter:
    def __init__(
        self,
        rate: float = 1.0,  # requests per second
        max_queue_size: int = 100,
        logger: Optional[logging.Logger] = None
    ) -> None:
        if rate <= 0:
            raise ValueError("rate must be > 0")
        self.leak_interval = 1.0 / rate  # convert to delay between each request
        self.queue: asyncio.Queue[asyncio.Future[None]] = asyncio.Queue(maxsize=max_queue_size)
        self._started = False
        self._logger = logger or logging.getLogger(__name__)
        self._drain_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        self._drain_task = asyncio.create_task(self._drain_loop())
        self._logger.debug("LeakyBucket started with leak_interval=%s", self.leak_interval)

    async def _drain_loop(self) -> None:
        while True:
            fut = await self.queue.get()
            if not fut.done():
                fut.set_result(None)
            await asyncio.sleep(self.leak_interval)

    async def acquire(self) -> None:
        if not self._started:
            raise RuntimeError("LeakyBucket not started. Call await limiter.start() before use.")
        fut: asyncio.Future[None] = asyncio.get_event_loop().create_future()
        await self.queue.put(fut)
        await fut

    async def __aenter__(self) -> "LeakyBucketRateLimiter":
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> Optional[bool]:
        return None

    def limit(
        self,
        fn: Optional[Callable[..., Awaitable[T]]] = None
    ) -> Union[
        Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]],
        Callable[..., Awaitable[T]]
    ]:

        def decorator(func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
            @wraps(func)
            async def wrapper(*args, **kwargs) -> T:
                await self.acquire()
                return await func(*args, **kwargs)
            return wrapper

        return decorator if fn is None else decorator(fn)
