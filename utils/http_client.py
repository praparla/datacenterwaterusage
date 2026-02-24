import asyncio
import random
from pathlib import Path

import httpx
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from utils.user_agents import get_random_user_agent

logger = structlog.get_logger()


class RateLimitedClient:
    """httpx.AsyncClient wrapper with rate limiting, retries, and user-agent rotation."""

    def __init__(self, min_delay: float = 2.0, max_delay: float = 5.0):
        self.min_delay = min_delay
        self.max_delay = max_delay
        self._client = httpx.AsyncClient(
            timeout=30.0,
            follow_redirects=True,
            headers={"User-Agent": get_random_user_agent()},
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.close()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.ConnectError, httpx.TimeoutException)),
    )
    async def get(self, url: str, **kwargs) -> httpx.Response:
        await self._rate_limit_delay()
        self._client.headers["User-Agent"] = get_random_user_agent()
        resp = await self._client.get(url, **kwargs)
        resp.raise_for_status()
        return resp

    async def download_file(self, url: str, dest_path: str) -> str:
        """Download a file to dest_path. Creates parent dirs if needed. Returns dest_path."""
        Path(dest_path).parent.mkdir(parents=True, exist_ok=True)

        # Skip if file already exists and has content
        if Path(dest_path).exists() and Path(dest_path).stat().st_size > 0:
            logger.info("file_already_exists", path=dest_path)
            return dest_path

        resp = await self.get(url)
        with open(dest_path, "wb") as f:
            f.write(resp.content)
        logger.info("file_downloaded", path=dest_path, size=len(resp.content))
        return dest_path

    async def _rate_limit_delay(self):
        delay = random.uniform(self.min_delay, self.max_delay)
        await asyncio.sleep(delay)

    async def close(self):
        await self._client.aclose()
