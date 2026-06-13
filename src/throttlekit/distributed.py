import asyncio
import logging
from functools import wraps
from typing import Callable, Awaitable, TypeVar, Union, Optional
from .backends.base import BaseBackend

T = TypeVar("T")

class DistributedTokenBucket:
    """Distributed token-bucket rate limiter that integrates with Redis or SQL backends.
    
    Can be used directly, as a decorator, or as an async context manager.
    """
    def __init__(
        self,
        backend: BaseBackend,
        max_tokens: int,
        refill_interval: float = 1.0,
        name: str = "default",
        logger: Optional[logging.Logger] = None
    ):
        if max_tokens <= 0:
            raise ValueError("max_tokens must be > 0")
        if refill_interval <= 0:
            raise ValueError("refill_interval must be > 0")

        self.backend = backend
        self.max_tokens = max_tokens
        self.refill_interval = refill_interval
        self.name = name
        self._logger = logger or logging.getLogger(__name__)

    async def acquire(self, key: str = "global", tokens: int = 1, block: bool = True) -> bool:
        """Acquire tokens for a specific key.
        
        Args:
            key: Unique key to identify the bucket (e.g. user ID, API key, endpoint).
            tokens: Number of tokens to consume.
            block: If True, sleep and retry until acquired. If False, fail immediately.
            
        Returns:
            True if tokens were acquired, False otherwise.
        """
        full_key = f"throttlekit:{self.name}:{key}"
        while True:
            wait_time = await self.backend.acquire(
                full_key, self.max_tokens, self.refill_interval, tokens
            )
            if wait_time == 0.0:
                return True
            if not block:
                return False
            await asyncio.sleep(wait_time)

    def __call__(self, key: str = "global", tokens: int = 1, block: bool = True):
        """Allows using the limiter as a parameterized async context manager:
        
        >>> async with limiter(key="user_123"):
        >>>     pass
        """
        return DistributedTokenBucketContext(self, key, tokens, block)

    def limit(
        self,
        fn: Optional[Callable[..., Awaitable[T]]] = None,
        *,
        key: Union[str, Callable[..., str]] = "global",
        tokens: int = 1,
        block: bool = True
    ) -> Union[
        Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]],
        Callable[..., Awaitable[T]]
    ]:
        """Decorator to rate limit an async function.
        
        The `key` parameter can be a static string or a callable that takes the
        same arguments as the decorated function to dynamically resolve the key.
        """
        def decorator(func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
            @wraps(func)
            async def wrapper(*args, **kwargs) -> T:
                resolved_key = key(*args, **kwargs) if callable(key) else key
                acquired = await self.acquire(resolved_key, tokens, block)
                if not acquired:
                    raise RuntimeError("Rate limit exceeded")
                return await func(*args, **kwargs)
            return wrapper

        if fn is None:
            return decorator
        else:
            return decorator(fn)


# Alias for backward compatibility
DistributedRateLimiter = DistributedTokenBucket


class DistributedTokenBucketContext:
    def __init__(self, limiter: DistributedTokenBucket, key: str, tokens: int, block: bool):
        self.limiter = limiter
        self.key = key
        self.tokens = tokens
        self.block = block

    async def __aenter__(self) -> "DistributedTokenBucketContext":
        acquired = await self.limiter.acquire(self.key, self.tokens, self.block)
        if not acquired:
            raise RuntimeError("Rate limit exceeded")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> Optional[bool]:
        return None


class DistributedLeakyBucket:
    """Distributed leaky-bucket (GCRA) rate limiter that integrates with Redis or SQL backends.
    
    Can be used directly, as a decorator, or as an async context manager.
    """
    def __init__(
        self,
        backend: BaseBackend,
        rate: float = 1.0,
        max_queue_size: int = 100,
        name: str = "default",
        logger: Optional[logging.Logger] = None
    ):
        if rate <= 0:
            raise ValueError("rate must be > 0")
        if max_queue_size <= 0:
            raise ValueError("max_queue_size must be > 0")

        self.backend = backend
        self.rate = rate
        self.max_queue_size = max_queue_size
        self.name = name
        self._logger = logger or logging.getLogger(__name__)

    async def acquire(self, key: str = "global", block: bool = True) -> bool:
        """Acquire permission for a specific key.
        
        Args:
            key: Unique key to identify the bucket (e.g. user ID, API key, endpoint).
            block: If True, sleep and retry until space is available. If False, fail immediately.
            
        Returns:
            True if permission was acquired, False otherwise.
        """
        full_key = f"throttlekit:{self.name}:{key}"
        while True:
            wait_time = await self.backend.acquire_leaky(
                full_key, self.rate, self.max_queue_size
            )
            if wait_time == 0.0:
                return True
            if not block:
                return False
            await asyncio.sleep(wait_time)

    def __call__(self, key: str = "global", block: bool = True):
        """Allows using the limiter as a parameterized async context manager:
        
        >>> async with leaky_limiter(key="user_123"):
        >>>     pass
        """
        return DistributedLeakyBucketContext(self, key, block)

    def limit(
        self,
        fn: Optional[Callable[..., Awaitable[T]]] = None,
        *,
        key: Union[str, Callable[..., str]] = "global",
        block: bool = True
    ) -> Union[
        Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]],
        Callable[..., Awaitable[T]]
    ]:
        """Decorator to rate limit an async function.
        
        The `key` parameter can be a static string or a callable that takes the
        same arguments as the decorated function to dynamically resolve the key.
        """
        def decorator(func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
            @wraps(func)
            async def wrapper(*args, **kwargs) -> T:
                resolved_key = key(*args, **kwargs) if callable(key) else key
                acquired = await self.acquire(resolved_key, block)
                if not acquired:
                    raise RuntimeError("Rate limit exceeded")
                return await func(*args, **kwargs)
            return wrapper

        if fn is None:
            return decorator
        else:
            return decorator(fn)


class DistributedLeakyBucketContext:
    def __init__(self, limiter: DistributedLeakyBucket, key: str, block: bool):
        self.limiter = limiter
        self.key = key
        self.block = block

    async def __aenter__(self) -> "DistributedLeakyBucketContext":
        acquired = await self.limiter.acquire(self.key, self.block)
        if not acquired:
            raise RuntimeError("Rate limit exceeded")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> Optional[bool]:
        return None
