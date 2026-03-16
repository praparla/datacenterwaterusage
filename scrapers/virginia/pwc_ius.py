"""Scraper for Prince William Water Industrial User Survey (IUS).

The March 2024 IUS lists industrial customers including data centers,
with ERU (Equivalent Residential Unit) capacity allocations. Key facts:
  - 56 data centers in Prince William County
  - Data centers consumed ~2.7% of average daily demand
  - Data centers consumed ~5.3% of max daily demand
  - Each ERU = 400 GPD max (10,000 gallons/month)

No browser automation needed — direct PDF download.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import AsyncGenerator, Optional

import structlog

from models.document import DocumentRecord, DocumentSource
from scrapers.base import BaseScraper
from utils.http_client import RateLimitedClient

logger = structlog.get_logger()

# Known IUS report URLs — supplement with page discovery
_KNOWN_IUS_URLS = [
    {
        "url": "https://princewilliamwater.org/sites/default/files/IUS_March%202024.pdf",
        "title": "Prince William Water Industrial User Survey — March 2024",
        "year": "2024",
        "id": "pwc-ius-2024",
    },
]

# Regex patterns for extracting water data from IUS PDF text
_ERU_PATTERN = re.compile(
    r"(\d[\d,]*)\s*ERU", re.IGNORECASE
)
_GPD_PATTERN = re.compile(
    r"([\d,]+(?:\.\d+)?)\s*(GPD|gallons?\s*per\s*day)", re.IGNORECASE
)
_MGD_PATTERN = re.compile(
    r"([\d,]+(?:\.\d+)?)\s*MGD", re.IGNORECASE
)
_PERCENT_PATTERN = re.compile(
    r"([\d.]+)\s*%\s*(?:of\s+)?(average|max|peak|total)", re.IGNORECASE
)
_DATA_CENTER_COUNT_PATTERN = re.compile(
    r"(\d+)\s+data\s+cent(?:er|re)s?", re.IGNORECASE
)


class PWCIUSScraper(BaseScraper):
    """Scraper for Prince William Water Industrial User Survey PDFs.

    Downloads IUS reports and extracts data center water allocation data.
    Each ERU = 400 GPD max capacity (10,000 gallons/month).
    """

    @property
    def name(self) -> str:
        return "va_pwc_ius"

    @property
    def source(self) -> DocumentSource:
        return DocumentSource.VA_PWC_IUS

    async def discover(self, limit: int | None = None) -> AsyncGenerator[dict, None]:
        """Discover IUS reports from known URLs and the PWC website."""
        count = 0
        seen_urls: set[str] = set()

        async with RateLimitedClient(
            min_delay=self.config["min_delay"],
            max_delay=self.config["max_delay"],
        ) as client:
            # Yield known IUS URLs (verified live)
            for report in _KNOWN_IUS_URLS:
                if limit and count >= limit:
                    return

                if report["url"] in seen_urls:
                    continue
                seen_urls.add(report["url"])

                # Verify the PDF is still accessible
                try:
                    resp = await client.get(report["url"])
                    if resp.status_code == 200:
                        yield {
                            "url": report["url"],
                            "document_url": report["url"],
                            "title": report["title"],
                            "date": datetime(int(report["year"]), 3, 1),
                            "state": "VA",
                            "agency": "Prince William Water",
                            "id": report["id"],
                            "year": report["year"],
                            "match_term": "Prince William Water Industrial User Survey — data center ERU allocations",
                            "matched_company": None,
                            "filename": f"pwc_ius_{report['year']}.pdf",
                        }
                        count += 1
                except Exception as e:
                    self.logger.warning(
                        "ius_url_check_failed",
                        url=report["url"],
                        error=str(e),
                    )

            # Also check the PWC commercial customers page for more reports
            commercial_url = self.config.get(
                "va_pwc_commercial_customers",
                "https://princewilliamwater.org/our-customers/commercial-customers",
            )
            try:
                from bs4 import BeautifulSoup
                from urllib.parse import urljoin

                resp = await client.get(commercial_url)
                soup = BeautifulSoup(resp.text, "lxml")

                for link in soup.find_all("a", href=True):
                    if limit and count >= limit:
                        return

                    href = link["href"]
                    text = link.get_text(strip=True).lower()

                    if not href.lower().endswith(".pdf"):
                        continue

                    if "ius" in text or "industrial user" in text or "survey" in text:
                        pdf_url = urljoin(commercial_url, href)
                        if pdf_url in seen_urls:
                            continue
                        seen_urls.add(pdf_url)

                        year = self._extract_year(href + " " + text)
                        doc_id = f"pwc-ius-{year or 'unknown'}"

                        yield {
                            "url": pdf_url,
                            "document_url": pdf_url,
                            "title": f"Prince William Water IUS {year or ''}".strip(),
                            "date": datetime(int(year), 1, 1) if year else None,
                            "state": "VA",
                            "agency": "Prince William Water",
                            "id": doc_id,
                            "year": year,
                            "match_term": "Prince William Water Industrial User Survey",
                            "matched_company": None,
                            "filename": f"pwc_ius_{year or 'unknown'}.pdf",
                        }
                        count += 1

            except Exception as e:
                self.logger.warning(
                    "pwc_page_scrape_failed",
                    url=commercial_url,
                    error=str(e),
                )

        self.logger.info("pwc_ius_discovery_complete", count=count)

    async def fetch_document(self, metadata: dict) -> Optional[str]:
        """Download the IUS PDF."""
        pdf_url = metadata.get("document_url")
        if not pdf_url:
            return None

        filename = metadata.get("filename", "pwc_ius.pdf")
        local_path = self.file_store.get_path("va", "prince-william-water", filename)

        async with RateLimitedClient(
            min_delay=self.config["min_delay"],
            max_delay=self.config["max_delay"],
        ) as client:
            try:
                return await client.download_file(pdf_url, local_path)
            except Exception as e:
                self.logger.error(
                    "ius_pdf_download_failed",
                    url=pdf_url,
                    error=str(e),
                )
                return None

    @staticmethod
    def extract_eru_values(text: str) -> list[dict]:
        """Extract ERU (Equivalent Residential Unit) values from text.

        Returns list of dicts with 'value' (int) and 'context' (surrounding text).
        """
        results = []
        for match in _ERU_PATTERN.finditer(text):
            value_str = match.group(1).replace(",", "")
            try:
                value = int(value_str)
                # Get surrounding context (100 chars before and after)
                start = max(0, match.start() - 100)
                end = min(len(text), match.end() + 100)
                context = text[start:end].strip()
                results.append({"value": value, "context": context})
            except ValueError:
                continue
        return results

    @staticmethod
    def extract_gpd_values(text: str) -> list[dict]:
        """Extract gallons-per-day values from text."""
        results = []
        for match in _GPD_PATTERN.finditer(text):
            value_str = match.group(1).replace(",", "")
            try:
                value = float(value_str)
                start = max(0, match.start() - 80)
                end = min(len(text), match.end() + 80)
                context = text[start:end].strip()
                results.append({
                    "value": value,
                    "unit": "GPD",
                    "context": context,
                })
            except ValueError:
                continue
        return results

    @staticmethod
    def extract_mgd_values(text: str) -> list[dict]:
        """Extract MGD (million gallons per day) values from text."""
        results = []
        for match in _MGD_PATTERN.finditer(text):
            value_str = match.group(1).replace(",", "")
            try:
                value = float(value_str)
                start = max(0, match.start() - 80)
                end = min(len(text), match.end() + 80)
                context = text[start:end].strip()
                results.append({
                    "value": value,
                    "unit": "MGD",
                    "context": context,
                })
            except ValueError:
                continue
        return results

    @staticmethod
    def extract_data_center_count(text: str) -> Optional[int]:
        """Extract the number of data centers mentioned in text."""
        match = _DATA_CENTER_COUNT_PATTERN.search(text)
        if match:
            return int(match.group(1))
        return None

    @staticmethod
    def extract_percent_demand(text: str) -> list[dict]:
        """Extract percentage-of-demand figures from text."""
        results = []
        for match in _PERCENT_PATTERN.finditer(text):
            try:
                pct = float(match.group(1))
                demand_type = match.group(2).lower()
                start = max(0, match.start() - 80)
                end = min(len(text), match.end() + 80)
                context = text[start:end].strip()
                results.append({
                    "percent": pct,
                    "demand_type": demand_type,
                    "context": context,
                })
            except ValueError:
                continue
        return results

    @staticmethod
    def eru_to_gpd(eru_count: int) -> int:
        """Convert ERU count to gallons per day (max capacity).

        1 ERU = 400 GPD max capacity (10,000 gallons/month).
        """
        return eru_count * 400

    @staticmethod
    def eru_to_monthly_gallons(eru_count: int) -> int:
        """Convert ERU count to monthly gallon capacity.

        1 ERU = 10,000 gallons/month.
        """
        return eru_count * 10_000

    def _extract_year(self, text: str) -> Optional[str]:
        """Extract a 4-digit year from text."""
        match = re.search(r"(20\d{2})(?!\d)", text)
        return match.group(1) if match else None
