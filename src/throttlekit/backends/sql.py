import time
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from .base import BaseBackend

class SQLBackend(BaseBackend):
    """SQL database backend for distributed rate limiting.
    
    Compatible with SQLAlchemy's AsyncEngine. Uses pessimistic locking (SELECT FOR UPDATE)
    to guarantee atomicity in distributed environments. Supports both Token Bucket
    and GCRA Leaky Bucket algorithms.
    """
    def __init__(self, engine: AsyncEngine, table_name: str = "throttlekit_buckets"):
        self.engine = engine
        self.table_name = table_name
        self._table_created = False
        self._leaky_table_created = False

    async def _ensure_table(self):
        if self._table_created:
            return
        async with self.engine.begin() as conn:
            # Create a table for tracking token bucket states
            await conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                    key VARCHAR(255) PRIMARY KEY,
                    tokens DOUBLE PRECISION NOT NULL,
                    last_refilled DOUBLE PRECISION NOT NULL
                )
            """))
        self._table_created = True

    async def _ensure_leaky_table(self):
        if self._leaky_table_created:
            return
        async with self.engine.begin() as conn:
            # Create a table for tracking leaky bucket tat states
            await conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {self.table_name}_leaky (
                    key VARCHAR(255) PRIMARY KEY,
                    tat DOUBLE PRECISION NOT NULL
                )
            """))
        self._leaky_table_created = True

    async def acquire(
        self,
        key: str,
        max_tokens: int,
        refill_interval: float,
        requested: int = 1
    ) -> float:
        await self._ensure_table()
        refill_rate = max_tokens / refill_interval
        now = time.time()

        async with AsyncSession(self.engine) as session:
            async with session.begin():
                # SQLite doesn't support SELECT ... FOR UPDATE, but its file-based locking
                # makes it unnecessary since it locks the database for write transactions.
                select_sql = f"SELECT tokens, last_refilled FROM {self.table_name} WHERE key = :key"
                if self.engine.dialect.name != "sqlite":
                    select_sql += " FOR UPDATE"

                result = await session.execute(text(select_sql), {"key": key})
                row = result.fetchone()

                if row is None:
                    tokens = float(max_tokens)
                    last_refilled = now
                    await session.execute(text(f"""
                        INSERT INTO {self.table_name} (key, tokens, last_refilled)
                        VALUES (:key, :tokens, :last_refilled)
                    """), {"key": key, "tokens": tokens, "last_refilled": last_refilled})
                else:
                    tokens, last_refilled = row
                    delta = max(0.0, now - last_refilled)
                    added = delta * refill_rate
                    tokens = min(float(max_tokens), tokens + added)

                if tokens >= requested:
                    tokens -= requested
                    await session.execute(text(f"""
                        UPDATE {self.table_name}
                        SET tokens = :tokens, last_refilled = :last_refilled
                        WHERE key = :key
                    """), {"key": key, "tokens": tokens, "last_refilled": now})
                    return 0.0
                else:
                    missing = requested - tokens
                    wait_time = missing / refill_rate
                    # Update bucket state to save refilled tokens
                    await session.execute(text(f"""
                        UPDATE {self.table_name}
                        SET tokens = :tokens, last_refilled = :last_refilled
                        WHERE key = :key
                    """), {"key": key, "tokens": tokens, "last_refilled": now})
                    return wait_time

    async def acquire_leaky(
        self,
        key: str,
        rate: float,
        max_queue_size: int
    ) -> float:
        await self._ensure_leaky_table()
        emission_interval = 1.0 / rate
        delay_tolerance = max_queue_size * emission_interval
        now = time.time()

        async with AsyncSession(self.engine) as session:
            async with session.begin():
                select_sql = f"SELECT tat FROM {self.table_name}_leaky WHERE key = :key"
                if self.engine.dialect.name != "sqlite":
                    select_sql += " FOR UPDATE"

                result = await session.execute(text(select_sql), {"key": key})
                row = result.fetchone()

                if row is None:
                    tat = now
                    await session.execute(text(f"""
                        INSERT INTO {self.table_name}_leaky (key, tat)
                        VALUES (:key, :tat)
                    """), {"key": key, "tat": tat})
                else:
                    tat = row[0]
                    tat = max(tat, now)

                new_tat = tat + emission_interval

                if new_tat - now > delay_tolerance + 1e-6:
                    # Rate limited. Calculate wait time to free space in queue.
                    wait_time = new_tat - now - delay_tolerance
                    return wait_time
                else:
                    # Success. Update TAT.
                    await session.execute(text(f"""
                        UPDATE {self.table_name}_leaky
                        SET tat = :tat
                        WHERE key = :key
                    """), {"key": key, "tat": new_tat})
                    return 0.0
