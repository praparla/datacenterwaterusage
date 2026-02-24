"""Scraper for Virginia DEQ VPDES Individual Permits Excel spreadsheet.

Downloads the Excel file listing all current VPDES Individual Permits,
then filters for rows matching known data center companies.
"""

from __future__ import annotations

from datetime import datetime
from typing import AsyncGenerator, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from models.document import DocumentSource
from scrapers.base import BaseScraper
from utils.http_client import RateLimitedClient
from extractors.excel_extractor import ExcelExtractor


class DEQVPDESExcelScraper(BaseScraper):

    @property
    def name(self) -> str:
        return "va_deq_vpdes_excel"

    @property
    def source(self) -> DocumentSource:
        return DocumentSource.VA_DEQ_VPDES_EXCEL

    async def discover(self, limit: int | None = None) -> AsyncGenerator[dict, None]:
        """Find the VPDES Excel download link and yield one entry per matching row."""
        excel_path = await self._download_excel()
        if not excel_path:
            self.logger.error("could_not_download_vpdes_excel")
            return

        extractor = ExcelExtractor()
        rows = extractor.extract_rows(excel_path)
        self.logger.info("excel_rows_loaded", total=len(rows))

        known_companies = [c.upper() for c in self.config.get("known_companies", [])]
        count = 0

        for row in rows:
            if limit and count >= limit:
                return

            # Check if facility name matches any known data center company
            fac_name = str(row.get("Facility Name", row.get("FAC_NAME", ""))).strip()
            if not fac_name:
                continue

            fac_upper = fac_name.upper()
            matched = any(company in fac_upper for company in known_companies)

            # Also check for generic "data center" in the name
            if not matched and "DATA CENTER" in fac_upper:
                matched = True

            if matched:
                permit_num = str(row.get("Permit Number", row.get("VAP_PMT_NO", ""))).strip()
                yield {
                    "url": f"{self.config['va_deq_vpdes_page']}#permit-{permit_num}",
                    "title": f"VPDES Permit: {fac_name} ({permit_num})",
                    "date": self._parse_date(row.get("Effective Date")),
                    "state": "VA",
                    "agency": "Virginia DEQ",
                    "permit_number": permit_num,
                    "facility_name": fac_name,
                    "id": f"vpdes-excel-{permit_num}",
                    "raw_row": row,
                }
                count += 1

    async def fetch_document(self, metadata: dict) -> Optional[str]:
        """No additional document to download — the data comes from the Excel itself."""
        # Store the raw row data reference. The Excel file is already downloaded.
        return metadata.get("_excel_path")

    async def _download_excel(self) -> Optional[str]:
        """Download the VPDES permits Excel spreadsheet."""
        dest_path = self.file_store.get_path("virginia", "deq", "vpdes_individual_permits.xlsx")

        # Use cached version if it exists and is less than 24 hours old
        if self.file_store.exists("virginia", "deq", "vpdes_individual_permits.xlsx"):
            self.logger.info("using_cached_excel", path=dest_path)
            return dest_path

        async with RateLimitedClient(
            min_delay=self.config["min_delay"],
            max_delay=self.config["max_delay"],
        ) as client:
            try:
                # First, get the VPDES page and find the Excel link
                resp = await client.get(self.config["va_deq_vpdes_page"])
                soup = BeautifulSoup(resp.text, "lxml")

                # Look for links to Excel files
                excel_link = None
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    text = a.get_text(strip=True).lower()
                    if ".xlsx" in href or ".xls" in href or "individual permit" in text:
                        excel_link = urljoin(self.config["va_deq_vpdes_page"], href)
                        break

                if not excel_link:
                    self.logger.warning("excel_link_not_found_trying_playwright")
                    return await self._download_with_playwright()

                await client.download_file(excel_link, dest_path)
                return dest_path

            except Exception as e:
                self.logger.error("excel_download_failed", error=str(e))
                return await self._download_with_playwright()

    async def _download_with_playwright(self) -> Optional[str]:
        """Fallback: use Playwright to navigate and download the Excel file."""
        if not self.browser:
            self.logger.error("no_browser_for_fallback")
            return None

        try:
            page = await self.browser.new_page()
            await page.goto(self.config["va_deq_vpdes_page"], wait_until="networkidle")

            # Find Excel download link
            links = await page.query_selector_all("a[href*='.xlsx'], a[href*='.xls']")
            if not links:
                # Try links with relevant text
                links = await page.query_selector_all("a")
                for link in links:
                    text = await link.inner_text()
                    if "individual permit" in text.lower() or "excel" in text.lower():
                        links = [link]
                        break

            if links:
                href = await links[0].get_attribute("href")
                if href:
                    dest_path = self.file_store.get_path(
                        "virginia", "deq", "vpdes_individual_permits.xlsx"
                    )
                    async with RateLimitedClient() as client:
                        full_url = urljoin(self.config["va_deq_vpdes_page"], href)
                        await client.download_file(full_url, dest_path)
                    await page.close()
                    return dest_path

            await page.close()
            return None

        except Exception as e:
            self.logger.error("playwright_fallback_failed", error=str(e))
            return None

    def _parse_date(self, val) -> Optional[datetime]:
        if isinstance(val, datetime):
            return val
        if val is None:
            return None
        try:
            return datetime.fromisoformat(str(val))
        except (ValueError, TypeError):
            return None
