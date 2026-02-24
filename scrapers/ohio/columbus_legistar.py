"""Scraper for Columbus, Ohio city council via Legistar REST API.

The Legistar Web API is open and returns JSON. No browser automation needed.
Queries for matters and attachments related to data centers and water.
"""

from __future__ import annotations

from datetime import datetime
from typing import AsyncGenerator, Optional

from models.document import DocumentSource
from scrapers.base import BaseScraper
from utils.http_client import RateLimitedClient


class ColumbusLegistarScraper(BaseScraper):

    @property
    def name(self) -> str:
        return "oh_columbus_legistar"

    @property
    def source(self) -> DocumentSource:
        return DocumentSource.OH_COLUMBUS_LEGISTAR

    async def discover(self, limit: int | None = None) -> AsyncGenerator[dict, None]:
        """Query Legistar API for matters matching data center keywords."""
        api_base = self.config["oh_columbus_legistar_api"]
        keywords = [
            "data center", "water service", "cooling", "utility agreement",
            "water supply", "gallons", "consumptive",
        ]
        count = 0
        seen_ids = set()

        async with RateLimitedClient(
            min_delay=self.config["min_delay"],
            max_delay=self.config["max_delay"],
        ) as client:
            for keyword in keywords:
                if limit and count >= limit:
                    return

                # Query matters with keyword in title
                url = (
                    f"{api_base}/matters"
                    f"?$filter=substringof('{keyword}',MatterTitle)"
                    f"&$top=100&$skip=0"
                    f"&$orderby=MatterLastModifiedUtc desc"
                )

                try:
                    resp = await client.get(url)
                    matters = resp.json()
                except Exception as e:
                    self.logger.error("legistar_query_failed", keyword=keyword, error=str(e))
                    continue

                if not isinstance(matters, list):
                    continue

                for matter in matters:
                    if limit and count >= limit:
                        return

                    matter_id = matter.get("MatterId")
                    if matter_id in seen_ids:
                        continue
                    seen_ids.add(matter_id)

                    # Get attachments for this matter
                    attachments = await self._get_attachments(client, api_base, matter_id)

                    matter_date = self._parse_date(matter.get("MatterIntroDate"))
                    title = matter.get("MatterTitle", "")
                    file_num = matter.get("MatterFile", "")

                    yield {
                        "title": f"{file_num}: {title}",
                        "url": f"https://columbus.legistar.com/LegislationDetail.aspx?ID={matter_id}",
                        "date": matter_date,
                        "state": "OH",
                        "agency": "Columbus City Council",
                        "id": f"legistar-{matter_id}",
                        "matter_id": matter_id,
                        "attachments": attachments,
                    }
                    count += 1

        self.logger.info("legistar_discovery_complete", total=count)

    async def fetch_document(self, metadata: dict) -> Optional[str]:
        """Download attachments from a Legistar matter."""
        attachments = metadata.get("attachments", [])
        if not attachments:
            return None

        # Download the first PDF attachment
        async with RateLimitedClient(
            min_delay=self.config["min_delay"],
            max_delay=self.config["max_delay"],
        ) as client:
            for att in attachments:
                url = att.get("MatterAttachmentHyperlink", "")
                name = att.get("MatterAttachmentName", "attachment.pdf")

                if not url:
                    continue

                dest_path = self.file_store.get_path("ohio", "columbus", name)
                if self.file_store.exists("ohio", "columbus", name):
                    return dest_path

                try:
                    await client.download_file(url, dest_path)
                    return dest_path
                except Exception as e:
                    self.logger.error("legistar_attachment_download_failed", url=url, error=str(e))
                    continue

        return None

    async def _get_attachments(self, client: RateLimitedClient, api_base: str, matter_id: int) -> list[dict]:
        """Get attachments for a Legistar matter."""
        try:
            resp = await client.get(f"{api_base}/matters/{matter_id}/attachments")
            attachments = resp.json()
            return attachments if isinstance(attachments, list) else []
        except Exception as e:
            self.logger.debug("legistar_attachments_failed", matter_id=matter_id, error=str(e))
            return []

    def _parse_date(self, date_str: str | None) -> Optional[datetime]:
        if not date_str:
            return None
        try:
            # Legistar dates: "2025-01-15T00:00:00"
            return datetime.fromisoformat(date_str.replace("Z", "+00:00").split("T")[0])
        except (ValueError, TypeError):
            return None
