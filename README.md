# 🔄 throttlekit

[![PyPI Version](https://img.shields.io/pypi/v/throttlekit.svg)](https://pypi.org/project/throttlekit/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Build Status](https://github.com/rowds/throttlekit/actions/workflows/test.yaml/badge.svg)](https://github.com/rowds/throttlekit/actions)

A lightweight, high-performance, and feature-rich asyncio rate limiting library for Python. Fully supports local (in-memory) and distributed (Redis, SQL) deployments, with first-class support for FastAPI.

---

## 🚀 Key Features

* ⚡ **Two Algorithms:** Token Bucket (bursts-tolerant) and Leaky Bucket / GCRA (steady pacing).
* 🌐 **Distributed Support:** Redis (atomic Lua script) and SQL (Alchemy-compatible row-level locking).
* 🛣️ **Multiple Usage Patterns:** Decorators (`@limit`), context managers (`async with`), or manual `.acquire()`.
* ⚡ **FastAPI Ready:** Clean route dependencies (`Depends`) and global middleware options.
* 🚦 **Concurrency Limits:** Enforce concurrent call limits alongside rate limits.
* 🛡️ **Graceful Shutdown:** `stop()` method to cleanly cancel background tasks.

---

## ⚙️ Installation

```bash
# Core in-memory limiters
uv add throttlekit

# With optional Redis / SQL / FastAPI support
uv add "throttlekit[redis,sql,fastapi]"
```

---

## 🛠️ Usage Patterns

Both local and distributed limiters support these three standard async patterns:

### 1️⃣ Decorator Pattern
```python
# Rate limit dynamically by a callable key (useful for distributed limits)
@limiter.limit(key=lambda request: request.headers.get("X-API-Key"), block=False)
async def process_task(request):
    return "succeeded"
```

### 2️⃣ Context Manager
```python
async with limiter(key="my-bucket"):
    result = await perform_action()
```

### 3️⃣ Manual Control
```python
await limiter.acquire()
```

---

## ⚡ Core Rate Limiters

### 1. In-Memory Limiters (Single Instance)

```python
from throttlekit import TokenBucketRateLimiter, LeakyBucketRateLimiter

# Token Bucket: 10 requests every 60 seconds with a concurrency limit of 5
limiter = TokenBucketRateLimiter(max_tokens=10, refill_interval=60.0, concurrency_limit=5)
await limiter.start()

# Leaky Bucket: 2 requests per second max, queuing up to 100 requests
leaky_limiter = LeakyBucketRateLimiter(rate=2.0, max_queue_size=100)
await leaky_limiter.start()

# Clean up on shutdown
await limiter.stop()
```

### 2. Distributed Limiters (Multi-Container Deployments)

```python
import redis.asyncio as aioredis
from throttlekit import DistributedTokenBucket, DistributedLeakyBucket, RedisBackend

redis_client = aioredis.from_url("redis://localhost")
backend = RedisBackend(redis_client)

# Token Bucket (Redis-backed)
tb_limiter = DistributedTokenBucket(backend, max_tokens=10, refill_interval=60.0)

# Leaky Bucket (GCRA algorithm, Redis-backed)
lb_limiter = DistributedLeakyBucket(backend, rate=5.0, max_queue_size=10)
```

> [!NOTE]
> For SQL databases (PostgreSQL, MySQL, SQLite), import and use `SQLBackend` configured with a SQLAlchemy `AsyncEngine`.

---

## 🌐 FastAPI & Starlette Integration

### 1️⃣ Route-level Dependency
Use `FastAPIRateLimiter` to apply rates per route. Supports blocking (throttling) or returning `HTTP 429` immediately:

```python
from fastapi import FastAPI, Depends
from throttlekit.fastapi import FastAPIRateLimiter

app = FastAPI()

# block=False immediately rejects with HTTP 429 if the rate limit is exceeded
@app.get("/data", dependencies=[Depends(FastAPIRateLimiter(tb_limiter, block=False))])
async def get_data():
    return {"status": "ok"}
```

### 2️⃣ Global Application Middleware
Protect all routes globally at the application level:

```python
from throttlekit.fastapi import RateLimitMiddleware

app.add_middleware(RateLimitMiddleware, limiter=tb_limiter, block=False)
```

---

## 📁 Project Structure

```tree
throttlekit/
├── src/
│   └── throttlekit/
│       ├── __init__.py
│       ├── limiter.py           # In-memory Token Bucket
│       ├── leaky_limiter.py     # In-memory Leaky Bucket
│       ├── distributed.py       # Distributed Token & Leaky Buckets
│       ├── fastapi.py           # FastAPI dependency and middleware
│       └── backends/            # Storage backends
│           ├── base.py
│           ├── redis.py         # Redis Lua-based backend
│           └── sql.py           # SQL/SQLAlchemy database backend
├── tests/
│   ├── test_token_bucket_limiter.py
│   ├── test_leaky_bucket_limiter.py
│   ├── test_fastapi.py
│   └── test_distributed.py
├── pyproject.toml
└── README.md
```

---

## 🧪 Testing

```bash
uv pip install pytest pytest-asyncio pytest-cov
pytest --cov=src/throttlekit --cov-report=term-missing
```

---

## 📜 License

MIT License © [Roudrasekhar Majumder](https://github.com/rowds)
