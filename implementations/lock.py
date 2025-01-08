from redis import Redis
from redis.lock import Lock
from typing import Optional, Union

class RedisLock:
    """
    A wrapper class for redis.Lock.
    """

    def __init__(self, redis_client: Redis, lock_name: str, timeout: Optional[float] = None):
        """
        Initialize the wrapper for redis.Lock.

        :param redis_client: The Redis client instance.
        :param lock_name: The name of the lock.
        :param timeout: Optional timeout for the lock in seconds.
        """
        self.redis_client = redis_client
        self.lock_name = lock_name
        self.lock = Lock(redis_client, lock_name, timeout=timeout)

    def acquire(
        self,
        sleep: Optional[Union[int, float]] = None,
        blocking: Optional[bool] = None,
        blocking_timeout: Optional[Union[int, float]] = None,
        token: Optional[str] = None
    ) -> bool:
        """
        Acquire the lock.

        :param sleep: Amount of time to sleep between attempts to acquire the lock.
        :param blocking: Whether to block until the lock is acquired.
        :param blocking_timeout: Maximum time to block waiting for the lock.
        :param token: Optional token to use for acquiring the lock.
        :return: True if the lock is acquired, False otherwise.
        """
        return self.lock.acquire(
            sleep=sleep,
            blocking=blocking,
            blocking_timeout=blocking_timeout,
            token=token
        )

    def locked(self) -> bool:
        """
        Check if the lock is held by any process.

        :return: True if the lock is held, False otherwise.
        """
        return self.lock.locked()

    def release(self) -> None:
        """
        Release the lock.
        """
        self.lock.release()

    def owned(self) -> bool:
        """Returns a boolean indicating if the lock is owned by the current instance."""
        return self.lock.owned()