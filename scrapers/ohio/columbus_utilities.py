"""Scraper for Columbus City Utilities Board agendas and minutes.

WordPress site with DataTables. Attempts HTTP + BeautifulSoup first,
falls back to Playwright if needed.
"""

from __future__ import annotations

from datetime import datetime
from typing import AsyncGenerator, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from models.document import DocumentSource
from scrapers.base import BaseScraper
from utils.http_client import RateLimitedClient


class ColumbusUtilitiesScraper(BaseScraper):

    BASE_URL = "https://www.columbusutilities.org"

    @property
    def name(self) -> str:
        return "oh_columbus_utilities"

    @property
    def source(self) -> DocumentSource:
        return DocumentSource.OH_COLUMBUS_UTILITIES

    async def discover(self, limit: int | None = None) -> AsyncGenerator[dict, None]:
        """Scrape the Columbus Utilities Board agendas & minutes page for PDF links."""
        page_url = self.config["oh_columbus_utilities"]
        count = 0

        # Try HTTP first
        pdf_entries = await self._discover_via_http(page_url)

        # Fall back to Playwright if HTTP didn't find PDFs
        if not pdf_entries and self.browser:
            pdf_entries = await self._discover_via_playwright(page_url)

        for entry in pdf_entries:
            if limit and count >= limit:
                return

            entry["state"] = "OH"
            entry["agency"] = "Columbus City Utilities Board"
            entry["id"] = f"columbus-utilities-{count}"
            yield entry
            count += 1

    async def fetch_document(self, metadata: dict) -> Optional[str]:
        """Download a PDF from Columbus Utilities Board."""
        pdf_url = metadata.get("pdf_url")
        if not pdf_url:
            return None

        filename = pdf_url.split("/")[-1] or f"columbus_utilities_{metadata.get('id', 'unknown')}.pdf"
        dest_path = self.file_store.get_path("ohio", "columbus", filename)

        if self.file_store.exists("ohio", "columbus", filename):
            return dest_path

        async with RateLimitedClient(
            min_delay=self.config["min_delay"],
            max_delay=self.config["max_delay"],
        ) as client:
            try:
                await client.download_file(pdf_url, dest_path)
                return dest_path
            except Exception as e:
                self.logger.error("pdf_download_failed", url=pdf_url, error=str(e))
                return None

    async def _discover_via_http(self, page_url: str) -> list[dict]:
        """Parse the page with BeautifulSoup to find PDF links."""
        entries = []

        async with RateLimitedClient(
            min_delay=self.config["min_delay"],
            max_delay=self.config["max_delay"],
        ) as client:
            try:
                resp = await client.get(page_url)
                soup = BeautifulSoup(resp.text, "lxml")

                # Find all PDF links
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    text = a.get_text(strip=True)

                    if href.lower().endswith(".pdf"):
                        full_url = urljoin(self.BASE_URL, href)
                        entries.append({
                            "title": text or href.split("/")[-1],
                            "url": full_url,
                            "pdf_url": full_url,
                            "date": None,
                        })

                # Also check DataTables JSON source if present
                scripts = soup.find_all("script")
                for script in scripts:
                    if script.string and "DataTable" in (script.string or ""):
                        # DataTables might load data via AJAX — we'd need Playwright for that
                        self.logger.debug("datatables_detected_may_need_playwright")

                self.logger.info("http_discovery_complete", entries=len(entries))

            except Exception as e:
                self.logger.error("http_discovery_failed", error=str(e))

        return entries

    async def _discover_via_playwright(self, page_url: str) -> list[dict]:
        """Use Playwright to handle DataTables JS rendering."""
        entries = []

        try:
            page = await self.browser.new_page()
            await page.goto(page_url, wait_until="networkidle", timeout=30000)
            await page.wait_for_timeout(3000)

            # Extract PDF links after JS rendering
            links = await page.query_selector_all("a[href$='.pdf']")
            for link in links:
                href = await link.get_attribute("href")
                text = (await link.inner_text()).strip()
                if href:
                    full_url = urljoin(self.BASE_URL, href)
                    entries.append({
                        "title": text or href.split("/")[-1],
                        "url": full_url,
                        "pdf_url": full_url,
                        "date": None,
                    })

            await page.close()
            self.logger.info("playwright_discovery_complete", entries=len(entries))

        except Exception as e:
            self.logger.error("playwright_discovery_failed", error=str(e))

        return entries
