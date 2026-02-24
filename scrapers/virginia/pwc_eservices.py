"""Scraper for Prince William County eServices portal and development records.

Searches for data center permits and zoning applications in the
Data Center Opportunity Zone Overlay District.
"""

from __future__ import annotations

from typing import AsyncGenerator, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from models.document import DocumentSource
from scrapers.base import BaseScraper
from utils.http_client import RateLimitedClient


class PWCEServicesScraper(BaseScraper):

    BASE_URL = "https://www.pwcva.gov"
    ESERVICE_URL = "https://eservice.pwcgov.org"

    @property
    def name(self) -> str:
        return "va_pwc_eservices"

    @property
    def source(self) -> DocumentSource:
        return DocumentSource.VA_PWC_ESERVICES

    async def discover(self, limit: int | None = None) -> AsyncGenerator[dict, None]:
        """Search PWC portals for data center related records."""
        count = 0

        # Source 1: Development Services Records Center page
        entries = await self._scrape_records_center()
        for entry in entries:
            if limit and count >= limit:
                return
            entry["state"] = "VA"
            entry["agency"] = "Prince William County"
            entry["id"] = f"pwc-{count}"
            yield entry
            count += 1

        # Source 2: eServices portal search (Playwright required)
        if self.browser:
            eservice_entries = await self._search_eservices()
            for entry in eservice_entries:
                if limit and count >= limit:
                    return
                entry["state"] = "VA"
                entry["agency"] = "Prince William County"
                entry["id"] = f"pwc-eservice-{count}"
                yield entry
                count += 1

    async def fetch_document(self, metadata: dict) -> Optional[str]:
        """Download a document from PWC portals."""
        pdf_url = metadata.get("pdf_url")
        if not pdf_url:
            return None

        filename = pdf_url.split("/")[-1] or f"pwc_{metadata.get('id', 'unknown')}.pdf"
        dest_path = self.file_store.get_path("virginia", "pwc", filename)

        if self.file_store.exists("virginia", "pwc", filename):
            return dest_path

        async with RateLimitedClient(
            min_delay=self.config["min_delay"],
            max_delay=self.config["max_delay"],
        ) as client:
            try:
                await client.download_file(pdf_url, dest_path)
                return dest_path
            except Exception as e:
                self.logger.error("pwc_pdf_download_failed", url=pdf_url, error=str(e))
                return None

    async def _scrape_records_center(self) -> list[dict]:
        """Scrape the Development Services Records Center page."""
        entries = []
        records_url = f"{self.BASE_URL}/department/development-services/development-services-records-center/"

        async with RateLimitedClient(
            min_delay=self.config["min_delay"],
            max_delay=self.config["max_delay"],
        ) as client:
            try:
                resp = await client.get(records_url)
                soup = BeautifulSoup(resp.text, "lxml")

                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    text = a.get_text(strip=True)

                    if not text:
                        continue

                    text_lower = text.lower()
                    search_terms = self.config.get("search_keywords", [])
                    matched_kw = next(
                        (kw for kw in search_terms if kw.lower() in text_lower),
                        None,
                    )

                    if href.lower().endswith(".pdf") or matched_kw:
                        reason = f"link text matched: '{matched_kw}'" if matched_kw else "PDF link"
                        full_url = urljoin(self.BASE_URL, href)
                        entries.append({
                            "title": text[:300],
                            "url": full_url,
                            "pdf_url": full_url if href.endswith(".pdf") else None,
                            "date": None,
                            "match_term": reason,
                        })

            except Exception as e:
                self.logger.error("records_center_scrape_failed", error=str(e))

        return entries

    async def _search_eservices(self) -> list[dict]:
        """Use Playwright to search the PWC eServices portal."""
        entries = []

        try:
            page = await self.browser.new_page()
            await page.goto(
                self.config["va_pwc_eservices"],
                wait_until="networkidle",
                timeout=30000,
            )
            await page.wait_for_timeout(3000)

            # Look for search functionality
            search_input = await page.query_selector(
                "input[type='text'], input[type='search'], "
                "input[id*='search'], input[name*='search']"
            )

            if search_input:
                await search_input.fill("data center")

                # Find and click search button
                search_btn = await page.query_selector(
                    "button[type='submit'], input[type='submit'], "
                    "button:has-text('Search'), a:has-text('Search')"
                )
                if search_btn:
                    await search_btn.click()
                    await page.wait_for_timeout(5000)

                    # Parse results
                    links = await page.query_selector_all("a[href]")
                    for link in links:
                        href = await link.get_attribute("href")
                        text = (await link.inner_text()).strip()

                        if href and text and len(text) > 5:
                            full_url = urljoin(self.ESERVICE_URL, href)
                            entries.append({
                                "title": text[:300],
                                "url": full_url,
                                "pdf_url": full_url if href.endswith(".pdf") else None,
                                "date": None,
                                "match_term": "PWC eServices search: 'data center'",
                            })

            await page.close()

        except Exception as e:
            self.logger.error("eservices_search_failed", error=str(e))

        return entries
