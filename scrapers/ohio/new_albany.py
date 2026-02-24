"""Scraper for New Albany, Ohio city council meeting documents.

WordPress site with PDFs at predictable URL patterns:
newalbanyohio.org/wp-content/uploads/YYYY/MM/filename.pdf
"""

from __future__ import annotations

from typing import AsyncGenerator, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from models.document import DocumentSource
from scrapers.base import BaseScraper
from utils.http_client import RateLimitedClient


class NewAlbanyScraper(BaseScraper):

    BASE_URL = "https://newalbanyohio.org"

    @property
    def name(self) -> str:
        return "oh_new_albany"

    @property
    def source(self) -> DocumentSource:
        return DocumentSource.OH_NEW_ALBANY

    async def discover(self, limit: int | None = None) -> AsyncGenerator[dict, None]:
        """Scrape New Albany council and public records pages for document links."""
        urls = [
            self.config["oh_new_albany_council"],
            f"{self.BASE_URL}/administration/public-records/",
        ]

        count = 0
        seen_urls = set()

        for page_url in urls:
            if limit and count >= limit:
                return

            entries = await self._scrape_page(page_url)

            # Fall back to Playwright if HTTP didn't work
            if not entries and self.browser:
                entries = await self._scrape_page_playwright(page_url)

            for entry in entries:
                if limit and count >= limit:
                    return

                url = entry.get("url", "")
                if url in seen_urls:
                    continue
                seen_urls.add(url)

                entry["state"] = "OH"
                entry["agency"] = "New Albany"
                entry["id"] = f"new-albany-{count}"
                yield entry
                count += 1

    async def fetch_document(self, metadata: dict) -> Optional[str]:
        """Download a PDF from New Albany."""
        pdf_url = metadata.get("pdf_url")
        if not pdf_url:
            return None

        filename = pdf_url.split("/")[-1] or f"new_albany_{metadata.get('id', 'unknown')}.pdf"
        dest_path = self.file_store.get_path("ohio", "new_albany", filename)

        if self.file_store.exists("ohio", "new_albany", filename):
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

    async def _scrape_page(self, page_url: str) -> list[dict]:
        """Parse page with BeautifulSoup for PDF links."""
        entries = []

        async with RateLimitedClient(
            min_delay=self.config["min_delay"],
            max_delay=self.config["max_delay"],
        ) as client:
            try:
                resp = await client.get(page_url)
                soup = BeautifulSoup(resp.text, "lxml")

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
                            "match_term": "PDF link on council page",
                        })

                    # Also catch links with relevant keywords
                    else:
                        search_terms = self.config.get("search_keywords", [])
                        matched_kw = next(
                            (kw for kw in search_terms if kw.lower() in text.lower()),
                            None,
                        )
                        if matched_kw:
                            full_url = urljoin(self.BASE_URL, href)
                            entries.append({
                                "title": text,
                                "url": full_url,
                                "pdf_url": full_url if href.endswith(".pdf") else None,
                                "date": None,
                                "match_term": f"link text matched: '{matched_kw}'",
                            })

            except Exception as e:
                self.logger.error("page_scrape_failed", url=page_url, error=str(e))

        return entries

    async def _scrape_page_playwright(self, page_url: str) -> list[dict]:
        """Fallback: use Playwright for JS-rendered content (Elementor)."""
        entries = []

        try:
            page = await self.browser.new_page()
            await page.goto(page_url, wait_until="networkidle", timeout=30000)
            await page.wait_for_timeout(3000)

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
                        "match_term": "PDF link on council page (Playwright)",
                    })

            await page.close()

        except Exception as e:
            self.logger.error("playwright_scrape_failed", url=page_url, error=str(e))

        return entries
