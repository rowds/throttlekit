# 🔄 throttlekit

[![PyPI Version](https://img.shields.io/pypi/v/throttlekit.svg)](https://pypi.org/project/throttlekit/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Build Status](https://github.com/rowds/throttlekit/actions/workflows/test.yaml/badge.svg)](https://github.com/rowds/throttlekit/actions)

A lightweight, high-performance, and feature-rich async rate limiting library for Python. Fully supports local (in-memory) and distributed (Redis, SQL) deployments, with first-class support for FastAPI and custom logging.

---

## 🚀 Key Features

* ⚡ **Two Powerful Algorithms:**
  * **Token Bucket:** Perfect for allowing bursts of requests up to a maximum limit, refilled at a constant rate.
  * **Leaky Bucket (GCRA):** Enforces a steady flow of traffic, smooth pacing, and rejects/delays bursts.
* 🌐 **Distributed Coordination:** Share rate-limiting states across multiple instances/containers using:
  * **Redis Backend:** High-performance, atomic, Lua-scripted rate limiting.
  * **SQL Backend:** Database-backed rate limiting using SQLAlchemy async engine with row-level locking (supports PostgreSQL, MySQL, SQLite, etc.).
* 🛣️ **FastAPI/Starlette Ready:** Easily rate limit web endpoints using:
  * **Dependency Injection (`Depends`):** Route-specific rate limits.
  * **Application Middleware:** Global, application-wide rate limits.
  * **Blocking (Throttling) vs. Non-blocking (Immediate HTTP 429 Rejection)** modes.
* 🚦 **Concurrency Limits:** Control the number of concurrent executions alongside rate limits.
* 🪵 **Custom Logging Support:** Fully integrates with Python's built-in `logging.Logger` for monitoring rate-limiting operations.
* 🛡️ **Clean Async Architecture:** Thread-safe, async-native, with graceful shutdowns via `.stop()`.

---

## ⚙️ Installation

Install the core package or include optional dependencies depending on your storage backend:

```bash
# Core package (in-memory limiters only)
uv add throttlekit

# With Redis support
uv add "throttlekit[redis]"

# With SQL/SQLAlchemy support
uv add "throttlekit[sql]"

# With FastAPI/Starlette support
uv add "throttlekit[fastapi]"

# Install everything
uv add "throttlekit[redis,sql,fastapi]"
```

---

## 🛠️ Usage Patterns

All limiters (local and distributed) support three standard interaction patterns:

### 1️⃣ Decorator Pattern
Decorate async functions to apply rate limits automatically. Supports static keys or dynamic callable keys:

```python
# Static key
@limiter.limit(key="my-key", block=False)
async def process_task():
    return "success"

# Dynamic key (receives the decorated function's arguments)
@limiter.limit(key=lambda user_id, *args: f"user:{user_id}", block=True)
async def fetch_user_data(user_id: int):
    return {"data": "..."}
```

### 2️⃣ Context Manager Pattern
Enforce limits cleanly within a code block:

```python
# Using standard context manager
async with limiter(key="my-key", block=True):
    await do_some_work()
```

### 3️⃣ Manual Control Pattern
Call `.acquire()` directly in your application code:

```python
# Acquire token manually (blocks until a token is available)
await limiter.acquire("my-key")

# Non-blocking check
acquired = await limiter.acquire("my-key", block=False)
if not acquired:
    raise RuntimeError("Rate limit exceeded")
```

---

## ⚡ Core Rate Limiters

### 1. In-Memory Limiters (Single Instance)

Best for single-server processes where rate limiting state resides only in memory.

#### Token Bucket (`TokenBucketRateLimiter`)
Allows bursty traffic. You can optionally enforce a maximum concurrency limit.

```python
import logging
from throttlekit import TokenBucketRateLimiter

logger = logging.getLogger("my_app")

# Refill 10 tokens every 60 seconds. Restrict concurrency to max 5 workers.
limiter = TokenBucketRateLimiter(
    max_tokens=10,
    refill_interval=60.0,
    concurrency_limit=5,
    logger=logger
)

# You MUST start the background refill loop before using in-memory limiters
await limiter.start()

# Acquire tokens
async with limiter:
    await fetch_api_data()

# Refund a token manually if an operation fails (optional)
limiter.release_token()

# Clean up / stop the background task gracefully on application shutdown
await limiter.stop()
```

#### Leaky Bucket (`LeakyBucketRateLimiter`)
Enforces a smooth, consistent request rate. Requests are queued up to a maximum capacity.

```python
from throttlekit import LeakyBucketRateLimiter

# Paced rate of 2 requests per second. Queue up to 100 requests.
limiter = LeakyBucketRateLimiter(
    rate=2.0,
    max_queue_size=100
)

await limiter.start()

# Paces execution: blocks if queue space is available; raises full queue exception if exceeded
async with limiter:
    await send_notification()

await limiter.stop()
```

---

### 2. Distributed Limiters (Multi-Container Deployments)

Best for microservices, Kubernetes pods, or multi-replica FastAPI nodes. State is persisted and synchronized through a shared backend.

#### Step A: Configure the Backend

##### Redis Backend (Recommended)
Uses highly efficient, atomic Lua scripts.

```python
import redis.asyncio as aioredis
from throttlekit import RedisBackend

# Initialize async redis client
redis_client = aioredis.from_url("redis://localhost:6379")
backend = RedisBackend(redis_client)
```

##### SQL Backend (SQLAlchemy compatible)
Supports PostgreSQL, MySQL, SQLite, etc. Uses row-level lock-based synchronization. Tables are automatically initialized.

```python
from sqlalchemy.ext.asyncio import create_async_engine
from throttlekit import SQLBackend

# Initialize SQLAlchemy async engine
engine = create_async_engine("postgresql+asyncpg://user:pass@localhost/db")
backend = SQLBackend(engine)
```

#### Step B: Instantiate the Distributed Limiter

##### Distributed Token Bucket (`DistributedTokenBucket`)
```python
from throttlekit import DistributedTokenBucket

# 10 tokens, refilling every 60 seconds.
# The 'name' namespace separates keys in the shared storage backend.
limiter = DistributedTokenBucket(
    backend=backend,
    max_tokens=10,
    refill_interval=60.0,
    name="api_user_limits"
)

# Enforce rate limit globally for user "user_123"
await limiter.acquire(key="user_123", tokens=1, block=True)
```

##### Distributed Leaky Bucket (`DistributedLeakyBucket` - GCRA)
```python
from throttlekit import DistributedLeakyBucket

# Paces at 5 requests per second, queue up to 10 requests.
limiter = DistributedLeakyBucket(
    backend=backend,
    rate=5.0,
    max_queue_size=10,
    name="sms_pacing"
)

# Acquire space for phone number
await limiter.acquire(key="+1234567890", block=True)
```

---

## 🌐 FastAPI & Starlette Integration

`throttlekit` comes with complete support for FastAPI route dependencies (`Depends`) and global middleware.

### 1️⃣ Route-level Dependency (`FastAPIRateLimiter`)
Use `FastAPIRateLimiter` to apply custom rate limits to specific endpoints or routers.

```python
from fastapi import FastAPI, Depends, Request
from throttlekit.fastapi import FastAPIRateLimiter

app = FastAPI()

# Custom function to resolve user ID or IP globally
def resolve_client_key(request: Request) -> str:
    return request.headers.get("X-User-ID", request.client.host or "anonymous")

# Route limited by dynamic key: 2 requests per 10 seconds max.
# block=False returns HTTP 429 immediately if exceeded.
@app.get(
    "/expensive-endpoint",
    dependencies=[
        Depends(FastAPIRateLimiter(
            limiter=tb_limiter,
            block=False,
            detail="Too many expensive requests. Please wait.",
            key=resolve_client_key
        ))
    ]
)
async def read_expensive_data():
    return {"data": "rich content"}
```

### 2️⃣ Global Application Middleware (`RateLimitMiddleware`)
Protect the entire application under a global rate-limiting policy.

```python
from fastapi import FastAPI
from throttlekit.fastapi import RateLimitMiddleware

app = FastAPI()

# Enforce rate limits globally on all incoming paths
app.add_middleware(
    RateLimitMiddleware,
    limiter=tb_limiter,
    block=False,
    detail="Global rate limit exceeded",
    key=lambda req: req.client.host or "global"
)
```

> [!TIP]
> Use `block=True` in both dependency and middleware modes to **throttle** requests (making them wait/queue in line until a token is refilled) instead of rejecting them with an HTTP 429 error.

---

## ⚙️ Configuration Options Reference

### In-Memory Limiters
| Limiter Class | Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- | :--- |
| `TokenBucketRateLimiter` | `max_tokens` | `int` | *Required* | Max tokens the bucket can hold. |
| | `refill_interval` | `float` | `1.0` | Duration (seconds) to refill the bucket to max capacity. |
| | `concurrency_limit` | `Optional[int]` | `None` | Restricts concurrent active requests. |
| | `logger` | `Optional[logging.Logger]` | `None` | Custom logger instance. |
| `LeakyBucketRateLimiter` | `rate` | `float` | `1.0` | Target rate of requests per second. |
| | `max_queue_size` | `int` | `100` | Max queued requests allowed before failing. |
| | `logger` | `Optional[logging.Logger]` | `None` | Custom logger instance. |

### Distributed Limiters
| Limiter Class | Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- | :--- |
| `DistributedTokenBucket` | `backend` | `BaseBackend` | *Required* | RedisBackend or SQLBackend storage instance. |
| | `max_tokens` | `int` | *Required* | Max tokens the bucket holds. |
| | `refill_interval` | `float` | `1.0` | Time (seconds) to completely refill the bucket. |
| | `name` | `str` | `"default"` | Unique namespace key for backend storage. |
| | `logger` | `Optional[logging.Logger]` | `None` | Custom logger instance. |
| `DistributedLeakyBucket` | `backend` | `BaseBackend` | *Required* | RedisBackend or SQLBackend storage instance. |
| | `rate` | `float` | `1.0` | Requests allowed per second. |
| | `max_queue_size` | `int` | `100` | Queue limit capacity (GCRA buffer size). |
| | `name` | `str` | `"default"` | Unique namespace key for backend storage. |
| | `logger` | `Optional[logging.Logger]` | `None` | Custom logger instance. |

---

## 🪵 Custom Logging

`throttlekit` limiters support passing standard loggers. All internal debug, warning, and startup logs will flow through your logger instance:

```python
import logging
from throttlekit import TokenBucketRateLimiter

# Define a custom logging format
logging.basicConfig(level=logging.DEBUG)
custom_logger = logging.getLogger("throttlekit_custom")

limiter = TokenBucketRateLimiter(
    max_tokens=10, 
    refill_interval=1.0, 
    logger=custom_logger
)
```

---

## 🧪 Testing

We maintain 100% code coverage on the package modules. Run the test suite:

```bash
# Run pytest with coverage reporting
uv run pytest --cov=src/throttlekit --cov-report=term-missing
```

To run the integration tests targeting local Redis (at `localhost:6379`) and multiprocessing interpreters:
```bash
uv run pytest tests/test_integration.py
```

---

## 📜 License

MIT License © [Roudrasekhar Majumder](https://github.com/rowds)
