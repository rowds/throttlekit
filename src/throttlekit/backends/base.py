from abc import ABC, abstractmethod

class BaseBackend(ABC):
    @abstractmethod
    async def acquire(
        self,
        key: str,
        max_tokens: int,
        refill_interval: float,
        requested: int = 1
    ) -> float:
        """Attempt to acquire tokens using the Token Bucket algorithm.
        
        Args:
            key: The unique identifier for the rate limit bucket.
            max_tokens: Maximum number of tokens the bucket can hold.
            refill_interval: Time in seconds to refill the bucket to max_tokens.
            requested: Number of tokens to consume.
            
        Returns:
            0.0 if tokens were successfully acquired.
            The estimated wait time (in seconds) until tokens are available if rate-limited.
        """
        pass

    @abstractmethod
    async def acquire_leaky(
        self,
        key: str,
        rate: float,
        max_queue_size: int
    ) -> float:
        """Attempt to acquire permission using the GCRA (Leaky Bucket) algorithm.
        
        Args:
            key: The unique identifier for the leaky bucket.
            rate: The steady processing rate (requests per second).
            max_queue_size: Maximum queue capacity.
            
        Returns:
            0.0 if successfully acquired.
            The estimated wait time (in seconds) until space is available in the queue if rate-limited.
        """
        pass
