"""Scraper for Ohio EPA eDocument Portal.

ASP.NET WebForms application with AJAX UpdatePanels.
Requires Playwright to handle __VIEWSTATE and dynamic form submissions.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import AsyncGenerator, Optional
from urllib.parse import urljoin

from models.document import DocumentSource
from scrapers.base import BaseScraper
from utils.http_client import RateLimitedClient


class OhioEPAScraper(BaseScraper):

    BASE_URL = "https://edocpub.epa.ohio.gov"

    @property
    def name(self) -> str:
        return "oh_epa_edocument"

    @property
    def source(self) -> DocumentSource:
        return DocumentSource.OH_EPA_EDOCUMENT

    async def discover(self, limit: int | None = None) -> AsyncGenerator[dict, None]:
        """Search the Ohio EPA eDocument portal for data center related permits."""
        if not self.browser:
            self.logger.error("browser_required_for_ohio_epa")
            return

        page = await self.browser.new_page()
        count = 0

        try:
            await page.goto(
                self.config["oh_epa_edocument"],
                wait_until="networkidle",
                timeout=45000,
            )
            await page.wait_for_timeout(3000)

            # Search with different keywords
            search_terms = ["data center", "OHD000001"]

            for term in search_terms:
                if limit and count >= limit:
                    break

                results = await self._search_portal(page, term)

                for result in results:
                    if limit and count >= limit:
                        break

                    result["state"] = "OH"
                    result["agency"] = "Ohio EPA"
                    result["id"] = f"oh-epa-{count}-{term.replace(' ', '-')}"
                    yield result
                    count += 1

        except Exception as e:
            self.logger.error("ohio_epa_discovery_failed", error=str(e))
        finally:
            await page.close()

    async def fetch_document(self, metadata: dict) -> Optional[str]:
        """Download a document from Ohio EPA portal."""
        pdf_url = metadata.get("pdf_url")
        if not pdf_url:
            return None

        filename = metadata.get("filename") or f"oh_epa_{metadata.get('id', 'unknown')}.pdf"
        dest_path = self.file_store.get_path("ohio", "epa", filename)

        if self.file_store.exists("ohio", "epa", filename):
            return dest_path

        async with RateLimitedClient(
            min_delay=self.config["min_delay"],
            max_delay=self.config["max_delay"],
        ) as client:
            try:
                await client.download_file(pdf_url, dest_path)
                return dest_path
            except Exception as e:
                self.logger.error("epa_pdf_download_failed", url=pdf_url, error=str(e))
                return None

    async def _search_portal(self, page, search_term: str) -> list[dict]:
        """Perform a search on the Ohio EPA eDocument portal."""
        results = []

        try:
            # Navigate back to the search page if needed
            current_url = page.url
            if "edochome" not in current_url.lower():
                await page.goto(
                    self.config["oh_epa_edocument"],
                    wait_until="networkidle",
                    timeout=30000,
                )
                await page.wait_for_timeout(2000)

            # Find the full-text search input
            search_input = await page.query_selector(
                "input[id*='FullText'], input[id*='fulltext'], "
                "input[id*='SearchText'], input[name*='FullText'], "
                "input[type='text'][id*='txt']"
            )

            if search_input:
                await search_input.fill("")
                await search_input.fill(search_term)
            else:
                # Try broader selector
                inputs = await page.query_selector_all("input[type='text']")
                if inputs:
                    # Use the last text input (often the full-text search)
                    await inputs[-1].fill(search_term)
                else:
                    self.logger.warning("no_search_input_found")
                    return results

            # Set date range to past 5 years
            await self._set_date_range(page)

            # Click search button
            search_btn = await page.query_selector(
                "input[type='submit'][value*='Search'], "
                "button:has-text('Search'), "
                "input[id*='btnSearch'], "
                "a:has-text('Search')"
            )

            if search_btn:
                await search_btn.click()
                # Wait for AJAX update to complete
                await page.wait_for_timeout(5000)
                try:
                    await page.wait_for_load_state("networkidle", timeout=15000)
                except Exception:
                    pass

            # Parse results from the page
            results = await self._parse_results(page, search_term)

        except Exception as e:
            self.logger.error("portal_search_failed", term=search_term, error=str(e))

        return results

    async def _set_date_range(self, page):
        """Set the date range fields to cover the past 5 years."""
        try:
            five_years_ago = (datetime.now() - timedelta(days=5 * 365)).strftime("%m/%d/%Y")
            today = datetime.now().strftime("%m/%d/%Y")

            from_date = await page.query_selector(
                "input[id*='FromDate'], input[id*='fromDate'], input[id*='dateFrom']"
            )
            to_date = await page.query_selector(
                "input[id*='ToDate'], input[id*='toDate'], input[id*='dateTo']"
            )

            if from_date:
                await from_date.fill(five_years_ago)
            if to_date:
                await to_date.fill(today)

        except Exception as e:
            self.logger.debug("date_range_set_failed", error=str(e))

    async def _parse_results(self, page, search_term: str) -> list[dict]:
        """Parse search results from the Ohio EPA portal."""
        results = []

        try:
            # Look for result rows in a table or grid
            rows = await page.query_selector_all(
                "table tr, .grid-row, [class*='result'], "
                ".RadGrid tr, .rgRow, .rgAltRow"
            )

            for row in rows:
                try:
                    text = (await row.inner_text()).strip()
                    if not text or len(text) < 10:
                        continue

                    # Skip header rows
                    if any(header in text.lower() for header in ["document type", "entity name", "date range"]):
                        continue

                    # Find PDF link in this row
                    pdf_link = await row.query_selector("a[href*='.pdf'], a[href*='download']")
                    pdf_url = None
                    if pdf_link:
                        href = await pdf_link.get_attribute("href")
                        if href:
                            pdf_url = urljoin(self.BASE_URL, href)

                    # Find any link for the document URL
                    any_link = await row.query_selector("a[href]")
                    doc_url = ""
                    if any_link:
                        href = await any_link.get_attribute("href")
                        doc_url = urljoin(self.BASE_URL, href) if href else ""

                    results.append({
                        "title": text[:300],
                        "url": doc_url or self.config["oh_epa_edocument"],
                        "pdf_url": pdf_url,
                        "date": None,
                        "search_term": search_term,
                    })

                except Exception:
                    continue

            self.logger.info("search_results_parsed", term=search_term, count=len(results))

        except Exception as e:
            self.logger.error("results_parse_failed", error=str(e))

        return results
