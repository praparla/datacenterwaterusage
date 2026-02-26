"""Scraper for Loudoun Water Annual Comprehensive Financial Reports (ACFRs).

Loudoun Water publishes ACFRs that contain statistical tables showing water
sales volume by customer class — including a dedicated data center category.
In 2023, data centers consumed 899 million gallons of potable water plus
~736 million gallons of reclaimed water, totaling ~1.6 billion gallons/year
(250% increase from 2019). This is the best publicly available aggregate
source for data center water consumption in Northern Virginia.

No browser automation needed — PDF links are in static HTML.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import AsyncGenerator, Optional
from urllib.parse import urljoin, urlparse

import structlog
from bs4 import BeautifulSoup

from models.document import DocumentSource
from scrapers.base import BaseScraper
from utils.http_client import RateLimitedClient

logger = structlog.get_logger()

# Keywords that indicate a link is an ACFR or annual financial report PDF
_REPORT_KEYWORDS = [
    "comprehensive", "acfr", "annual", "financial report", "cafr",
]

# Regex to extract fiscal year from filename or link text.
# No word-boundary requirement: match "2024" inside "ACFR2024Final.pdf" too.
# The negative lookahead (?!\d) prevents matching inside longer numbers.
_YEAR_RE = re.compile(r"(20\d{2})(?!\d)")


class LoudounACFRScraper(BaseScraper):
    """Scraper for Loudoun Water ACFR PDFs.

    Discovers annual report PDF links on the Loudoun Water website and
    downloads each report. The main pipeline post-processes the PDFs to
    extract water sales volume by customer class (residential, commercial,
    data center, reclaimed).
    """

    @property
    def name(self) -> str:
        return "va_loudoun_acfr"

    @property
    def source(self) -> DocumentSource:
        return DocumentSource.VA_LOUDOUN_ACFR

    async def discover(self, limit: int | None = None) -> AsyncGenerator[dict, None]:
        """Fetch the ACFR listing page and yield metadata for each PDF."""
        page_url = self.config["va_loudoun_acfr_page"]
        count = 0

        async with RateLimitedClient(
            min_delay=self.config["min_delay"],
            max_delay=self.config["max_delay"],
        ) as client:
            try:
                resp = await client.get(page_url)
                soup = BeautifulSoup(resp.text, "lxml")
            except Exception as e:
                self.logger.error("page_fetch_failed", url=page_url, error=str(e))
                return

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
                year = self._extract_year(href, text)
                doc_id = f"loudoun-acfr-{year or urlparse(pdf_url).path.split('/')[-1]}"
                title = text or f"Loudoun Water ACFR {year or 'Unknown Year'}"

                self.logger.info("discovered_acfr", title=title, year=year, url=pdf_url)
                yield {
                    "url": page_url,
                    "document_url": pdf_url,
                    "pdf_url": pdf_url,
                    "title": title,
                    "date": datetime(int(year), 1, 1) if year else None,
                    "state": "VA",
                    "agency": "Loudoun Water",
                    "id": doc_id,
                    "year": year,
                    "match_term": "Loudoun Water ACFR — data center water sales",
                    "matched_company": None,
                }
                count += 1

        self.logger.info("acfr_discovery_complete", count=count)

    async def fetch_document(self, metadata: dict) -> Optional[str]:
        """Download the ACFR PDF and return the local file path."""
        pdf_url = metadata.get("pdf_url") or metadata.get("document_url")
        if not pdf_url:
            return None

        year = metadata.get("year", "unknown")
        filename = f"loudoun_acfr_{year}.pdf"

        async with RateLimitedClient(
            min_delay=self.config["min_delay"],
            max_delay=self.config["max_delay"],
        ) as client:
            try:
                resp = await client.get(pdf_url)
                return await self.file_store.save(
                    content=resp.content,
                    state="va",
                    agency="loudoun-water",
                    filename=filename,
                )
            except Exception as e:
                self.logger.error("pdf_download_failed", url=pdf_url, error=str(e))
                return None

    def _extract_year(self, href: str, text: str) -> Optional[str]:
        """Extract a 4-digit fiscal year from the URL or link text."""
        for source in (href, text):
            m = _YEAR_RE.search(source)
            if m:
                return m.group(1)
        return None
