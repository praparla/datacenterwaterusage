"""Scraper for Virginia DEQ PEEP/VPT Tableau dashboard.

Attempts to use the Tableau dashboard at PEEP for permit search data.
Falls back to the VPT Power BI portal at permits.virginia.gov if needed.
"""

from __future__ import annotations

from datetime import datetime
from typing import AsyncGenerator, Optional
from urllib.parse import urljoin

from models.document import DocumentSource
from scrapers.base import BaseScraper


class DEQPEEPScraper(BaseScraper):

    @property
    def name(self) -> str:
        return "va_deq_peep"

    @property
    def source(self) -> DocumentSource:
        return DocumentSource.VA_DEQ_PEEP

    async def discover(self, limit: int | None = None) -> AsyncGenerator[dict, None]:
        """Try Tableau dashboard, fall back to VPT portal."""
        # Strategy 1: Try the VPT search portal (more structured)
        async for meta in self._discover_via_vpt(limit):
            yield meta

    async def fetch_document(self, metadata: dict) -> Optional[str]:
        """Download linked permit document if available."""
        doc_url = metadata.get("doc_url")
        if not doc_url:
            return None

        filename = f"vpt_permit_{metadata.get('permit_number', 'unknown')}.pdf"
        dest_path = self.file_store.get_path("virginia", "deq", filename)

        if self.file_store.exists("virginia", "deq", filename):
            return dest_path

        # VPT may not have direct PDF downloads — the value is in the metadata itself
        return None

    async def _discover_via_vpt(self, limit: int | None = None) -> AsyncGenerator[dict, None]:
        """Use Playwright to search VPT (permits.virginia.gov) for DEQ water permits."""
        if not self.browser:
            self.logger.error("browser_required_for_vpt")
            return

        page = await self.browser.new_page()
        count = 0

        try:
            await page.goto(
                "https://permits.virginia.gov/Permit/Search",
                wait_until="networkidle",
                timeout=45000,
            )

            # The VPT search page has filters for Agency, Locality, etc.
            # Try to set Agency filter to DEQ and look for water permits.

            # Look for search/filter elements
            # The page uses Power BI embedded, so direct DOM interaction may be limited.
            # Extract whatever data is visible on the initial load.

            await page.wait_for_timeout(5000)  # Allow Power BI to render

            # Try to find permit data in the page
            text_content = await page.inner_text("body")

            # Parse visible permit entries from the text
            entries = self._parse_vpt_text(text_content)

            for entry in entries:
                if limit and count >= limit:
                    break

                # Check relevance
                title = entry.get("title", "").upper()
                known = [c.upper() for c in self.config.get("known_companies", [])]
                relevant = (
                    "DATA CENTER" in title
                    or any(c in title for c in known)
                    or "WATER" in title
                )

                if relevant:
                    entry["state"] = "VA"
                    entry["agency"] = "Virginia DEQ"
                    entry["id"] = f"vpt-{entry.get('permit_number', count)}"
                    yield entry
                    count += 1

        except Exception as e:
            self.logger.error("vpt_discovery_failed", error=str(e))
        finally:
            await page.close()

    def _parse_vpt_text(self, text: str) -> list[dict]:
        """Best-effort parsing of VPT page text content.

        Since this is a Power BI dashboard, the text may be fragmented.
        We look for patterns like permit numbers, facility names, and dates.
        """
        import re

        entries = []

        # Look for VPDES permit number patterns (e.g., VA0001234)
        permit_pattern = re.compile(r'(VA\d{7})', re.IGNORECASE)
        permits_found = permit_pattern.findall(text)

        for permit_num in set(permits_found):
            # Try to find surrounding context for this permit
            idx = text.upper().find(permit_num.upper())
            if idx >= 0:
                context = text[max(0, idx - 200):idx + 200]
                entries.append({
                    "title": f"VPT Permit: {permit_num}",
                    "url": f"https://permits.virginia.gov/Permit/Search?q={permit_num}",
                    "permit_number": permit_num,
                    "date": None,
                    "context": context.strip(),
                })

        self.logger.info("vpt_entries_parsed", count=len(entries))
        return entries
