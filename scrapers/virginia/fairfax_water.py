"""Scraper for Fairfax Water annual financial reports.

Fairfax Water is Virginia's largest water utility and the upstream wholesale
supplier to Loudoun Water (~18 MGD) and Prince William Water. Tracking
changes in wholesale delivery volumes provides a proxy measure for data
center area growth, since ~47% of Fairfax Water's total sales volume goes
to wholesale customers that serve data center-dense areas.

Key 2024 figures confirmed:
  - Operating revenues: $226.8M (+3.7%)
  - Retail water sales: +4.8%
  - Wholesale water sales: +2.8%
  - Loudoun Water purchases: ~18 MGD

No browser automation needed — PDF links are in static HTML.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import AsyncGenerator, Optional
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from models.document import DocumentSource
from scrapers.base import BaseScraper
from utils.http_client import RateLimitedClient

# Keywords that identify financial report PDFs vs other documents
_REPORT_KEYWORDS = [
    "financial report", "annual report", "cafr", "acfr", "popular annual",
    "comprehensive", "budget",
]

_YEAR_RE = re.compile(r"(20\d{2})(?!\d)")

# Known direct PDF URLs to supplement page scraping
_KNOWN_REPORT_URLS = [
    {
        "url": "https://www.fairfaxwater.org/sites/default/files/about_us/2024%20Financial%20Report.pdf",
        "title": "Fairfax Water 2024 Comprehensive Financial Report",
        "year": "2024",
        "id": "fairfax-water-financial-report-2024",
    },
    {
        "url": "https://www.fairfaxwater.org/sites/default/files/about_us/2023%20Financial%20Report.pdf",
        "title": "Fairfax Water 2023 Comprehensive Financial Report",
        "year": "2023",
        "id": "fairfax-water-financial-report-2023",
    },
]


class FairfaxWaterScraper(BaseScraper):
    """Scraper for Fairfax Water annual financial report PDFs.

    Discovers PDF links on the Fairfax Water About Us page, supplements
    with known direct URLs, and downloads each report.
    """

    @property
    def name(self) -> str:
        return "va_fairfax_water"

    @property
    def source(self) -> DocumentSource:
        return DocumentSource.VA_FAIRFAX_WATER

    async def discover(self, limit: int | None = None) -> AsyncGenerator[dict, None]:
        """Scrape the about-us page for financial report PDFs plus known direct URLs."""
        page_url = self.config["va_fairfax_water_about_page"]
        seen_urls: set[str] = set()
        count = 0

        async with RateLimitedClient(
            min_delay=self.config["min_delay"],
            max_delay=self.config["max_delay"],
        ) as client:
            # First: scrape the about-us page for PDF links
            try:
                resp = await client.get(page_url)
                soup = BeautifulSoup(resp.text, "lxml")

                for link in soup.find_all("a", href=True):
                    if limit and count >= limit:
                        return

                    href = link["href"]
                    text = link.get_text(strip=True)

                    if not href.lower().endswith(".pdf"):
                        continue

                    combined = (href + " " + text).lower()
                    if not any(kw in combined for kw in _REPORT_KEYWORDS):
                        continue

                    pdf_url = urljoin(page_url, href)
                    if pdf_url in seen_urls:
                        continue
                    seen_urls.add(pdf_url)

                    year = self._extract_year(href, text)
                    doc_id = f"fairfax-water-report-{year or urlparse(pdf_url).path.split('/')[-1]}"
                    title = text or f"Fairfax Water Financial Report {year or 'Unknown Year'}"

                    yield self._make_meta(pdf_url, title, year, doc_id, page_url)
                    count += 1

            except Exception as e:
                self.logger.warning("page_scrape_failed", url=page_url, error=str(e))

            # Second: ensure known direct URLs are always included
            for known in _KNOWN_REPORT_URLS:
                if limit and count >= limit:
                    return
                if known["url"] in seen_urls:
                    continue
                seen_urls.add(known["url"])
                try:
                    # HEAD request to verify the PDF exists
                    await client.get(known["url"], method="HEAD")
                    yield self._make_meta(
                        known["url"], known["title"], known["year"],
                        known["id"], page_url,
                    )
                    count += 1
                except Exception as e:
                    self.logger.warning(
                        "known_url_check_failed", url=known["url"], error=str(e)
                    )

        self.logger.info("fairfax_discovery_complete", count=count)

    async def fetch_document(self, metadata: dict) -> Optional[str]:
        """Download the financial report PDF."""
        pdf_url = metadata.get("pdf_url") or metadata.get("document_url")
        if not pdf_url:
            return None

        year = metadata.get("year", "unknown")
        filename = f"fairfax_water_financial_report_{year}.pdf"

        async with RateLimitedClient(
            min_delay=self.config["min_delay"],
            max_delay=self.config["max_delay"],
        ) as client:
            try:
                resp = await client.get(pdf_url)
                return await self.file_store.save(
                    content=resp.content,
                    state="va",
                    agency="fairfax-water",
                    filename=filename,
                )
            except Exception as e:
                self.logger.error("pdf_download_failed", url=pdf_url, error=str(e))
                return None

    def _make_meta(
        self,
        pdf_url: str,
        title: str,
        year: Optional[str],
        doc_id: str,
        page_url: str,
    ) -> dict:
        return {
            "url": page_url,
            "document_url": pdf_url,
            "pdf_url": pdf_url,
            "title": title,
            "date": datetime(int(year), 1, 1) if year else None,
            "state": "VA",
            "agency": "Fairfax Water",
            "id": doc_id,
            "year": year,
            "match_term": "Fairfax Water financial report — wholesale water delivery to DC clusters",
            "matched_company": None,
        }

    def _extract_year(self, href: str, text: str) -> Optional[str]:
        for source in (href, text):
            m = _YEAR_RE.search(source)
            if m:
                return m.group(1)
        return None
