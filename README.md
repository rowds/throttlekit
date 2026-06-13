# 🔄 throttlekit

A lightweight, asyncio-based rate limiting library for Python that provides flexible and efficient rate limiting solutions.

## 📋 Overview

**throttlekit** offers two proven rate limiting algorithms:

- **⚡ TokenBucketRateLimiter**: Allows controlled bursts of activity
- **💧 LeakyBucketRateLimiter**: Enforces a strict, steady rate

Perfect for API throttling, web scrapers, background jobs, and queue management.

## 🚀 Features

- ✅ **Two proven algorithms**: Token Bucket (burst-tolerant) and Leaky Bucket (evenly-paced)
- ✅ **Multiple usage patterns**: `@decorator`, `async with`, and manual `.acquire()`
- ✅ **Concurrency control**: Optional `concurrency_limit` parameter
- ✅ **High performance**: Low-overhead design optimized for async workloads
- ✅ **asyncio integration**: Works seamlessly with `asyncio.gather()` and `TaskGroup`

## ⚙️ Installation

### Using uv (recommended)

```bash
uv add throttlekit
```

### Using pip

```bash
pip install throttlekit
```

## ✨ Quick Start

```python
import asyncio
from throttlekit import TokenBucketRateLimiter

# Create a rate limiter (5 tokens, refill every second)
limiter = TokenBucketRateLimiter(max_tokens=5, refill_interval=1.0)

@limiter.limit
async def call_api(i):
    await asyncio.sleep(0.2)
    return f"Request {i} completed"

async def main():
    await limiter.start()
    
    # Process 10 requests with rate limiting
    results = await asyncio.gather(*(call_api(i) for i in range(10)))
    print(results)
    
    await limiter.stop()

# Run the example
asyncio.run(main())
```

## 🧠 Which Limiter Should I Use?

| Use Case | Recommended Limiter | Why? |
|----------|-------------------|------|
| Allow short bursts (e.g., 5 calls at once) |  **Token Bucket** | Accumulates tokens for burst capacity |
| Require steady pacing (e.g., 1 call/sec max) |  **Leaky Bucket** | Maintains consistent rate |
| Queue smoothing, task draining |  **Leaky Bucket** | FIFO processing at fixed rate |
| Per-user or per-key API quotas |  **Token Bucket** | Flexible burst handling |

## Rate Limiters

### TokenBucketRateLimiter

Allows bursts up to `max_tokens`, then refills at a steady rate.

```python
from throttlekit import TokenBucketRateLimiter

limiter = TokenBucketRateLimiter(
    max_tokens=10,           # Maximum burst size
    refill_interval=1.0,     # Refill every second
    concurrency_limit=5      # Optional: limit concurrent operations
)
```

**Supports:**

- `@limiter.limit` decorator
- `async with limiter` context manager  
- `await limiter.acquire()` manual usage
- `await limiter.stop()` graceful shutdown

### 💧 LeakyBucketRateLimiter

Processes requests at a fixed rate, queuing excess requests.

```python
from throttlekit import LeakyBucketRateLimiter

limiter = LeakyBucketRateLimiter(
    rate=2.0,              # 2 requests per second
    max_queue_size=100     # Maximum queued requests
)
```

**Behavior:**

- Drains 1 request every `1/rate` seconds in FIFO order
- Queued requests are processed at a fixed rate
- Bursts are automatically queued (up to `max_queue_size`)
- Call `await limiter.stop()` to gracefully shut down the drain loop

## 📘 Usage Examples

### 1️⃣ Decorator Pattern

```python
@limiter.limit
async def fetch_data(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return await response.json()
```

### 2️⃣ Context Manager

```python
async with limiter:
    result = await expensive_operation()
    return result
```

### 3️⃣ Manual Control

```python
await limiter.acquire()
try:
    result = await do_work()
finally:
    limiter.release_semaphore()  # If using concurrency_limit
```

## 🧪 Testing

Install test dependencies:

```bash
uv pip install pytest pytest-asyncio
```

Run tests with coverage:

```bash
pytest --cov=src/throttlekit --cov-report=term-missing
```

## 📁 Project Structure

```tree
throttlekit/
├── src/
│   └── throttlekit/
│       ├── __init__.py
│       ├── limiter.py           # TokenBucketRateLimiter
│       └── leaky_limiter.py     # LeakyBucketRateLimiter
├── tests/
│   ├── test_token_bucket_limiter.py
│   └── test_leaky_bucket_limiter.py
├── pyproject.toml
├── README.md
└── LICENSE
```

## 📜 License

MIT License © [Roudrasekhar Majumder](https://github.com/rowds)

## 🙋 Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for detailed setup instructions and guidelines.

---

**⭐ Star this repo if you find it useful!**

[Report Bug](https://github.com/rowds/throttlekit/issues) • [Request Feature](https://github.com/rowds/throttlekit/issues) • [Documentation](https://github.com/rowds/throttlekit)
