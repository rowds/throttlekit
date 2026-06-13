import time
from .base import BaseBackend

class RedisBackend(BaseBackend):
    """Redis backend for distributed rate limiting.
    
    Uses atomic Lua scripts to implement both lazy-refilling token bucket
    and GCRA leaky bucket rate limiting.
    """
    def __init__(self, redis_client):
        self.redis = redis_client
        self._lua_script = None
        self._lua_leaky_script = None

    async def _get_script(self):
        if self._lua_script is None:
            # Register Lua script for atomic token bucket acquisition.
            script = """
            local key = KEYS[1]
            local max_tokens = tonumber(ARGV[1])
            local refill_rate = tonumber(ARGV[2])
            local requested = tonumber(ARGV[3])
            local now = tonumber(ARGV[4])

            local data = redis.call('HMGET', key, 'tokens', 'last_refilled')
            local tokens = tonumber(data[1])
            local last_refilled = tonumber(data[2])

            if not tokens then
                tokens = max_tokens
                last_refilled = now
            else
                local delta = math.max(0, now - last_refilled)
                local added = delta * refill_rate
                tokens = math.min(max_tokens, tokens + added)
            end

            if tokens >= requested then
                tokens = tokens - requested
                redis.call('HMSET', key, 'tokens', tokens, 'last_refilled', now)
                local ttl = math.ceil(max_tokens / refill_rate) * 2
                redis.call('EXPIRE', key, ttl)
                return tostring(0.0)
            else
                local missing = requested - tokens
                local wait_time = missing / refill_rate
                redis.call('HMSET', key, 'tokens', tokens, 'last_refilled', now)
                local ttl = math.ceil(max_tokens / refill_rate) * 2
                redis.call('EXPIRE', key, ttl)
                return tostring(wait_time)
            end
            """
            self._lua_script = self.redis.register_script(script)
        return self._lua_script

    async def _get_leaky_script(self):
        if self._lua_leaky_script is None:
            # Register Lua script for atomic GCRA (Leaky Bucket) algorithm.
            script = """
            local key = KEYS[1]
            local rate = tonumber(ARGV[1])
            local max_queue_size = tonumber(ARGV[2])
            local now = tonumber(ARGV[3])

            local emission_interval = 1.0 / rate
            local delay_tolerance = max_queue_size * emission_interval

            local tat = tonumber(redis.call('GET', key))

            if not tat then
                tat = now
            else
                tat = math.max(tat, now)
            end

            local new_tat = tat + emission_interval

            if new_tat - now > delay_tolerance + 1e-6 then
                local wait_time = new_tat - now - delay_tolerance
                return tostring(wait_time)
            else
                redis.call('SET', key, new_tat)
                local ttl = math.ceil(new_tat - now) + 2
                redis.call('EXPIRE', key, ttl)
                return tostring(0.0)
            end
            """
            self._lua_leaky_script = self.redis.register_script(script)
        return self._lua_leaky_script

    async def acquire(
        self,
        key: str,
        max_tokens: int,
        refill_interval: float,
        requested: int = 1
    ) -> float:
        refill_rate = max_tokens / refill_interval
        now = time.time()
        script = await self._get_script()
        # Execute Lua script
        wait_time_str = await script(keys=[key], args=[max_tokens, refill_rate, requested, now])
        return float(wait_time_str)

    async def acquire_leaky(
        self,
        key: str,
        rate: float,
        max_queue_size: int
    ) -> float:
        now = time.time()
        script = await self._get_leaky_script()
        # Execute Lua script
        wait_time_str = await script(keys=[key], args=[rate, max_queue_size, now])
        return float(wait_time_str)
