import asyncio
import multiprocessing
import os
import sys
import time
import redis


def log(msg: str):
    timestamp = time.strftime("%H:%M:%S")
    print(f"[{timestamp}] [PID {os.getpid()}] {msg}")


def worker_task(success_counter, rate_limit_counter):
    # Each process runs its own event loop and connects to Redis independently
    import asyncio
    import redis.asyncio as aioredis
    from throttlekit import DistributedTokenBucket, RedisBackend

    async def run():
        redis_client = aioredis.from_url("redis://localhost:6379")
        backend = RedisBackend(redis_client)
        limiter = DistributedTokenBucket(backend, max_tokens=5, refill_interval=10.0, name="mp_tb")

        # Non-blocking acquisition
        acquired = await limiter.acquire("global_mp_key", block=False)
        if acquired:
            log("Acquired token successfully!")
            success_counter.value += 1
        else:
            log("Rate limited!")
            rate_limit_counter.value += 1

        await redis_client.aclose()

    asyncio.run(run())


def main():
    # Clean up Redis keys from previous runs using synchronous redis client
    r = redis.Redis(host="localhost", port=6379)
    r.delete("throttlekit:mp_tb:global_mp_key")
    r.close()

    # Shared counters to check results from subprocesses
    success_counter = multiprocessing.Value('i', 0)
    rate_limit_counter = multiprocessing.Value('i', 0)

    log("Spawning 10 parallel Python processes to compete for 5 tokens...")
    processes = []
    for _ in range(10):
        p = multiprocessing.Process(
            target=worker_task,
            args=(success_counter, rate_limit_counter)
        )
        processes.append(p)
        p.start()

    for p in processes:
        p.join()

    log("\n--- Multiprocessing Results ---")
    log(f"Successful acquisitions: {success_counter.value} (Expected: 5)")
    log(f"Rate limited processes: {rate_limit_counter.value} (Expected: 5)")
    
    assert success_counter.value == 5
    assert rate_limit_counter.value == 5
    log("Multiprocessing distributed rate limit test succeeded!")


if __name__ == "__main__":
    main()
