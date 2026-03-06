"""Scraper for the Central Ohio Regional Water Study PDFs (Ohio EPA, March 2025).

This 15-county study, released March 2025, quantifies the water demand impact
of data centers and Intel's New Albany chip campus. It is the most authoritative
public projection of data center water consumption in Ohio:

  Industrial water demand (data centers + Intel):
    2021 baseline: ~negligible
    2030 projection: >40 MGD
    2040 projection: ~70 MGD
    2050 projection: ~90 MGD (120% increase over 2021–2050)

Columbus is building a $1.6B fourth water treatment plant largely to meet
data center and Intel demand. This module downloads the study PDFs and stores
them as reference data. The main pipeline extracts demand projection tables
and key statistics.

PDFs confirmed directly downloadable as of Feb 2026 (no auth/WAF blocking).
"""

from __future__ import annotations

from datetime import datetime
from typing import AsyncGenerator, Optional

from models.document import DocumentSource
from scrapers.base import BaseScraper
from utils.http_client import RateLimitedClient


class CentralOhioWaterStudyScraper(BaseScraper):
    """Downloads Central Ohio Regional Water Study PDFs from Ohio EPA.

    All three PDFs (overview + two county detail reports) are fetched.
    Treated as reference/context data rather than permit records.
    """

    @property
    def name(self) -> str:
        return "oh_central_water_study"

    @property
    def source(self) -> DocumentSource:
        return DocumentSource.OH_CENTRAL_WATER_STUDY

    async def discover(self, limit: int | None = None) -> AsyncGenerator[dict, None]:
        """Yield one metadata entry per study PDF from config."""
        pdfs = self.config.get("oh_central_water_study_pdfs", [])
        for i, pdf_info in enumerate(pdfs):
            if limit and i >= limit:
                return
            yield {
                "url": pdf_info["url"],
                "document_url": pdf_info["url"],
                "pdf_url": pdf_info["url"],
                "title": pdf_info["title"],
                "date": datetime(2025, 3, 1),
                "state": "OH",
                "agency": "Ohio EPA",
                "id": pdf_info["id"],
                "match_term": "Central Ohio Regional Water Study — data center demand projections",
                "matched_company": "Intel",
            }

    async def fetch_document(self, metadata: dict) -> Optional[str]:
        """Download the study PDF and return the local path."""
        pdf_url = metadata.get("pdf_url")
        if not pdf_url:
            return None

        doc_id = metadata.get("id", "central-ohio-water-study")
        filename = f"{doc_id}.pdf"

        async with RateLimitedClient(
            min_delay=self.config["min_delay"],
            max_delay=self.config["max_delay"],
        ) as client:
            try:
                resp = await client.get(pdf_url)
                return await self.file_store.save(
                    content=resp.content,
                    state="oh",
                    agency="ohio-epa",
                    filename=filename,
                )
            except Exception as e:
                self.logger.error("pdf_download_failed", url=pdf_url, error=str(e))
                return None
