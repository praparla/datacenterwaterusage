"""Scraper for Virginia DEQ ArcGIS REST API — VPDES Outfalls layer.

Queries the public ArcGIS MapServer for VPDES outfall data,
filters for data-center-related facilities by name.
No browser automation needed — pure REST API.
"""

from datetime import datetime
from typing import AsyncGenerator, Optional

from models.document import DocumentSource
from scrapers.base import BaseScraper
from utils.http_client import RateLimitedClient


class DEQArcGISScraper(BaseScraper):

    @property
    def name(self) -> str:
        return "va_deq_arcgis"

    @property
    def source(self) -> DocumentSource:
        return DocumentSource.VA_DEQ_ARCGIS

    async def discover(self, limit: int | None = None) -> AsyncGenerator[dict, None]:
        """Query ArcGIS REST API and yield matching facility records."""
        api_url = self.config["va_deq_arcgis_vpdes_outfalls"]
        known_companies = [c.upper() for c in self.config.get("known_companies", [])]
        count = 0
        offset = 0
        batch_size = 1000

        async with RateLimitedClient(
            min_delay=self.config["min_delay"],
            max_delay=self.config["max_delay"],
        ) as client:
            while True:
                if limit and count >= limit:
                    return

                params = {
                    "where": "1=1",
                    "outFields": "*",
                    "f": "json",
                    "resultRecordCount": str(batch_size),
                    "resultOffset": str(offset),
                }

                try:
                    resp = await client.get(api_url, params=params)
                    data = resp.json()
                except Exception as e:
                    self.logger.error("arcgis_query_failed", offset=offset, error=str(e))
                    break

                features = data.get("features", [])
                if not features:
                    break

                for feature in features:
                    if limit and count >= limit:
                        return

                    attrs = feature.get("attributes", {})
                    fac_name = str(attrs.get("FAC_NAME", "")).strip()
                    if not fac_name:
                        continue

                    fac_upper = fac_name.upper()
                    matched = any(company in fac_upper for company in known_companies)
                    if not matched and "DATA CENTER" in fac_upper:
                        matched = True

                    if matched:
                        permit_num = str(attrs.get("VAP_PMT_NO", "")).strip()
                        outfall = str(attrs.get("OUTFALLNO", "")).strip()
                        yield {
                            "url": f"{api_url}?where=VAP_PMT_NO='{permit_num}'&f=html",
                            "title": f"VPDES Outfall: {fac_name} - {permit_num} (Outfall {outfall})",
                            "date": None,
                            "state": "VA",
                            "agency": "Virginia DEQ",
                            "permit_number": permit_num,
                            "facility_name": fac_name,
                            "id": f"arcgis-{permit_num}-{outfall}",
                            "attributes": attrs,
                        }
                        count += 1

                # Check if there are more results
                exceeded = data.get("exceededTransferLimit", False)
                if not exceeded or len(features) < batch_size:
                    break
                offset += batch_size

        self.logger.info("arcgis_discovery_complete", total_matches=count)

    async def fetch_document(self, metadata: dict) -> Optional[str]:
        """No file to download — data comes from the API response itself."""
        return None

    async def run(self, limit: int | None = None) -> list:
        """Override run() since this scraper produces records directly from API data."""
        from models.document import DocumentRecord

        self.logger.info("scraper_starting", limit=limit)
        results = []
        count = 0

        async for meta in self.discover(limit=limit):
            doc_id = meta.get("id", "")

            if await self.state_manager.is_fetched(self.name, doc_id):
                self.logger.debug("skipping_already_fetched", doc_id=doc_id)
                continue

            record = self._build_record(meta, None)
            # Enrich with attributes directly
            attrs = meta.get("attributes", {})
            record.company_llc_name = meta.get("facility_name")
            record.extracted_water_metric = self._extract_metrics_from_attrs(attrs)

            await self.state_manager.mark_fetched(self.name, doc_id)
            results.append(record)
            count += 1

        self.logger.info("scraper_finished", total_documents=count)
        return results

    def _extract_metrics_from_attrs(self, attrs: dict) -> Optional[str]:
        """Pull any water-related numeric fields from ArcGIS attributes."""
        metrics = []
        for key, val in attrs.items():
            if val is not None and any(
                term in key.upper()
                for term in ["FLOW", "DISCHARGE", "VOLUME", "GPD", "MGD", "GALLON"]
            ):
                metrics.append(f"{key}: {val}")
        return "; ".join(metrics) if metrics else None
