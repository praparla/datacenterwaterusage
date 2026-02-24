from __future__ import annotations

from playwright.async_api import async_playwright, Browser, BrowserContext, Page

from utils.user_agents import get_random_user_agent


class BrowserManager:
    """Async context manager for Playwright browser lifecycle."""

    def __init__(self, headless: bool = True):
        self.headless = headless
        self._pw = None
        self._browser: Browser | None = None

    async def __aenter__(self) -> "BrowserManager":
        self._pw = await async_playwright().start()
        self._browser = await self._pw.chromium.launch(headless=self.headless)
        return self

    async def __aexit__(self, *args):
        if self._browser:
            await self._browser.close()
        if self._pw:
            await self._pw.stop()

    async def new_context(self) -> BrowserContext:
        return await self._browser.new_context(
            user_agent=get_random_user_agent(),
            viewport={"width": 1920, "height": 1080},
        )

    async def new_page(self) -> Page:
        ctx = await self.new_context()
        return await ctx.new_page()
