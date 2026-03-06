"""
Reusable async primitives — rate limiting, retry configuration.

These are domain-agnostic utilities used by async workers and API clients.
"""

import asyncio


class AsyncRateLimiter:
    """
    Async rate limiter using semaphore + inter-request delay.

    Usage::

        limiter = AsyncRateLimiter(max_concurrent=3, delay_between=0.5)

        async with limiter:
            await make_request()
    """

    def __init__(self, max_concurrent: int, delay_between: float):
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._delay = delay_between
        self._last_request = 0.0
        self._lock = asyncio.Lock()

    async def __aenter__(self):
        await self._semaphore.acquire()
        async with self._lock:
            now = asyncio.get_event_loop().time()
            wait_time = max(0, self._delay - (now - self._last_request))
            if wait_time > 0:
                await asyncio.sleep(wait_time)
            self._last_request = asyncio.get_event_loop().time()
        return self

    async def __aexit__(self, *args):
        self._semaphore.release()


class RetryConfig:
    """Configuration for retry logic with exponential backoff."""

    MAX_RETRIES = 3
    BASE_DELAY = 0.5  # seconds
    MAX_DELAY = 8.0  # seconds
    EXPONENTIAL_BASE = 2
