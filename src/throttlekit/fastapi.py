import asyncio
import logging
from typing import Union, Optional, Callable
from fastapi import Request, HTTPException, status
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response, PlainTextResponse

from .limiter import TokenBucketRateLimiter
from .leaky_limiter import LeakyBucketRateLimiter
from .distributed import DistributedTokenBucket, DistributedLeakyBucket

class FastAPIRateLimiter:
    """FastAPI Dependency for route-level or global rate limiting.
    
    Can be used as a dependency in routes, routers, or globally.
    Supports in-memory limiters and DistributedTokenBucket/DistributedLeakyBucket.
    
    >>> limiter = DistributedTokenBucket(redis_backend, max_tokens=5)
    >>> @app.get("/items", dependencies=[Depends(FastAPIRateLimiter(limiter, block=False))])
    """
    def __init__(
        self,
        limiter: Union[TokenBucketRateLimiter, LeakyBucketRateLimiter, DistributedTokenBucket, DistributedLeakyBucket],
        block: bool = True,
        detail: str = "Too Many Requests",
        key: Optional[Union[str, Callable[[Request], str]]] = None,
        logger: Optional[logging.Logger] = None
    ):
        self.limiter = limiter
        self.block = block
        self.detail = detail
        self.key = key
        self._logger = logger or logging.getLogger(__name__)

    async def __call__(self, request: Request):
        is_distributed = isinstance(self.limiter, (DistributedTokenBucket, DistributedLeakyBucket))
        resolved_key = "global"
        
        if is_distributed:
            if self.key is not None:
                resolved_key = self.key(request) if callable(self.key) else self.key
            else:
                resolved_key = request.client.host if request.client else "global"

        if not self.block:
            # Reject immediately (HTTP 429) if rate/concurrency limit is reached
            if is_distributed:
                if isinstance(self.limiter, DistributedTokenBucket):
                    acquired = await self.limiter.acquire(key=resolved_key, tokens=1, block=False)
                else:
                    acquired = await self.limiter.acquire(key=resolved_key, block=False)
                if not acquired:
                    raise HTTPException(
                        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                        detail=self.detail
                    )
            elif isinstance(self.limiter, TokenBucketRateLimiter):
                if self.limiter.semaphore and self.limiter.semaphore.locked():
                    raise HTTPException(
                        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                        detail=self.detail
                    )
                try:
                    self.limiter.bucket.get_nowait()
                    if self.limiter.semaphore:
                        await self.limiter.semaphore.acquire()
                except asyncio.QueueEmpty:
                    raise HTTPException(
                        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                        detail=self.detail
                    )
            elif isinstance(self.limiter, LeakyBucketRateLimiter):
                if self.limiter.queue.full():
                    raise HTTPException(
                        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                        detail=self.detail
                    )
                await self.limiter.acquire()
        else:
            # Block/Queue request until a token becomes available (throttling)
            if is_distributed:
                if isinstance(self.limiter, DistributedTokenBucket):
                    await self.limiter.acquire(key=resolved_key, tokens=1, block=True)
                else:
                    await self.limiter.acquire(key=resolved_key, block=True)
            else:
                await self.limiter.acquire()

        try:
            yield
        finally:
            # Automatically release the concurrency limit semaphore if present (only for in-memory TokenBucket)
            if isinstance(self.limiter, TokenBucketRateLimiter):
                self.limiter.release_semaphore()


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Starlette/FastAPI Middleware for application-wide rate limiting.
    
    Supports in-memory limiters and DistributedTokenBucket/DistributedLeakyBucket.
    
    >>> limiter = DistributedTokenBucket(redis_backend, max_tokens=10)
    >>> app.add_middleware(RateLimitMiddleware, limiter=limiter, block=False)
    """
    def __init__(
        self,
        app,
        limiter: Union[TokenBucketRateLimiter, LeakyBucketRateLimiter, DistributedTokenBucket, DistributedLeakyBucket],
        block: bool = True,
        detail: str = "Too Many Requests",
        key: Optional[Union[str, Callable[[Request], str]]] = None,
        logger: Optional[logging.Logger] = None
    ):
        super().__init__(app)
        self.limiter = limiter
        self.block = block
        self.detail = detail
        self.key = key
        self._logger = logger or logging.getLogger(__name__)

    async def dispatch(self, request: Request, call_next) -> Response:
        is_distributed = isinstance(self.limiter, (DistributedTokenBucket, DistributedLeakyBucket))
        resolved_key = "global"
        
        if is_distributed:
            if self.key is not None:
                resolved_key = self.key(request) if callable(self.key) else self.key
            else:
                resolved_key = request.client.host if request.client else "global"

        if not self.block:
            if is_distributed:
                if isinstance(self.limiter, DistributedTokenBucket):
                    acquired = await self.limiter.acquire(key=resolved_key, tokens=1, block=False)
                else:
                    acquired = await self.limiter.acquire(key=resolved_key, block=False)
                if not acquired:
                    return PlainTextResponse(self.detail, status_code=status.HTTP_429_TOO_MANY_REQUESTS)
            elif isinstance(self.limiter, TokenBucketRateLimiter):
                if self.limiter.semaphore and self.limiter.semaphore.locked():
                    return PlainTextResponse(self.detail, status_code=status.HTTP_429_TOO_MANY_REQUESTS)
                try:
                    self.limiter.bucket.get_nowait()
                    if self.limiter.semaphore:
                        await self.limiter.semaphore.acquire()
                except asyncio.QueueEmpty:
                    return PlainTextResponse(self.detail, status_code=status.HTTP_429_TOO_MANY_REQUESTS)
            elif isinstance(self.limiter, LeakyBucketRateLimiter):
                if self.limiter.queue.full():
                    return PlainTextResponse(self.detail, status_code=status.HTTP_429_TOO_MANY_REQUESTS)
                await self.limiter.acquire()
        else:
            if is_distributed:
                if isinstance(self.limiter, DistributedTokenBucket):
                    await self.limiter.acquire(key=resolved_key, tokens=1, block=True)
                else:
                    await self.limiter.acquire(key=resolved_key, block=True)
            else:
                await self.limiter.acquire()

        try:
            response = await call_next(request)
            return response
        finally:
            if isinstance(self.limiter, TokenBucketRateLimiter):
                self.limiter.release_semaphore()
