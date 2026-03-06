"""Scraper for Ohio EPA ArcGIS Open Data — NPDES Individual Permits.

Queries the Ohio EPA Open Data portal for NPDES individual permits filtered
to SIC code 7374 (Computer Processing and Data Preparation), which covers
data center facilities. The dataset is updated nightly.

Dataset: https://data-oepa.opendata.arcgis.com/datasets/npdes-individual-permits
Dataset ID: 1118cdc038884214ba79a0712b60ece7_0
Filter: SIC_CODE = '7374'

Also queries by county (Franklin, Licking, Delaware, Union) to catch
any data center facilities classified under different SIC codes.

No browser automation needed — ArcGIS Open Data REST API.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import AsyncGenerator, Optional

from models.document import DocumentSource
from scrapers.base import BaseScraper
from utils.http_client import RateLimitedClient

# ArcGIS Open Data Hub GeoJSON API for Ohio EPA NPDES permits
# The query endpoint accepts OGC filter expressions
_ARCGIS_QUERY_BASE = (
    "https://opendata.arcgis.com/api/v3/datasets/1118cdc038884214ba79a0712b60ece7_0"
    "/downloads/data"
)

# Fallback: county-based query via the Ohio EPA permit list HTML page
_COUNTY_PERMITS_URL = "http://wwwapp.epa.ohio.gov/dsw/permits/permit_list.php"

# Target counties with significant data center presence
_TARGET_COUNTIES = ["Franklin", "Licking", "Delaware", "Union"]

# SIC code for data centers (Computer Processing and Data Preparation)
_DC_SIC_CODE = "7374"


class OhioEPANPDESArcGISScraper(BaseScraper):
    """Scraper for Ohio EPA NPDES permit data via ArcGIS Open Data.

    Primary filter: SIC_CODE = '7374' statewide.
    Secondary filter: Any permit in Franklin/Licking/Delaware/Union counties.

    Both sets are combined and deduplicated before yielding.
    """

    @property
    def name(self) -> str:
        return "oh_epa_npdes_arcgis"

    @property
    def source(self) -> DocumentSource:
        return DocumentSource.OH_EPA_NPDES_ARCGIS

    async def discover(self, limit: int | None = None) -> AsyncGenerator[dict, None]:
        """Query Ohio EPA ArcGIS Open Data and yield permit records."""
        seen_ids: set[str] = set()
        count = 0

        async with RateLimitedClient(
            min_delay=self.config["min_delay"],
            max_delay=self.config["max_delay"],
        ) as client:
            # Pass 1: SIC 7374 — direct data center permits statewide
            features = await self._fetch_geojson(
                client,
                where=f"SIC_CODE='{_DC_SIC_CODE}'",
            )
            for feat in features:
                if limit and count >= limit:
                    return
                meta = self._feature_to_meta(feat)
                if meta and meta["id"] not in seen_ids:
                    seen_ids.add(meta["id"])
                    yield meta
                    count += 1

            # Pass 2: Target counties — catch any DCs under other SIC codes
            for county in _TARGET_COUNTIES:
                features = await self._fetch_geojson(
                    client,
                    where=f"COUNTY_NAME='{county}'",
                )
                for feat in features:
                    if limit and count >= limit:
                        return
                    meta = self._feature_to_meta(feat)
                    if meta and meta["id"] not in seen_ids:
                        seen_ids.add(meta["id"])
                        yield meta
                        count += 1

        self.logger.info("oh_npdes_arcgis_complete", count=count)

    async def _fetch_geojson(
        self,
        client: RateLimitedClient,
        where: str,
    ) -> list[dict]:
        """Fetch GeoJSON features from the ArcGIS Open Data hub."""
        try:
            resp = await client.get(
                _ARCGIS_QUERY_BASE,
                params={
                    "format": "geojson",
                    "spatialRefId": "4326",
                    "where": where,
                },
            )
            data = resp.json()
            return data.get("features", [])
        except Exception as e:
            self.logger.error("arcgis_fetch_failed", where=where, error=str(e))
            return []

    def _feature_to_meta(self, feature: dict) -> Optional[dict]:
        """Convert a GeoJSON feature to a scraper metadata dict."""
        props = feature.get("properties") or feature.get("attributes", {})
        if not props:
            return None

        permit_id = str(props.get("PERMIT_ID") or props.get("permit_id") or "").strip()
        if not permit_id:
            return None

        facility = str(props.get("FACILITY_NAME") or props.get("facility_name") or "").strip()
        county = str(props.get("COUNTY_NAME") or props.get("county_name") or "").strip()
        sic = str(props.get("SIC_CODE") or props.get("sic_code") or "").strip()
        status = str(props.get("PERMIT_STATUS") or props.get("permit_status") or "").strip()
        issue_date = props.get("ISSUE_DATE") or props.get("issue_date")
        expiry_date = props.get("EXPIRY_DATE") or props.get("expiry_date")

        match_term = "Ohio EPA NPDES — "
        if sic == _DC_SIC_CODE:
            match_term += f"SIC 7374 (data center) permit"
        else:
            match_term += f"{county} County target area"

        return {
            "url": self.config.get("oh_epa_npdes_arcgis_page", _ARCGIS_QUERY_BASE),
            "document_url": (
                f"https://edocpub.epa.ohio.gov/publicportal/"
                f"portaldocumentquery.aspx?permit={permit_id}"
            ),
            "title": f"Ohio NPDES Permit: {facility} ({permit_id})",
            "date": self._parse_epoch_ms(issue_date),
            "state": "OH",
            "agency": "Ohio EPA",
            "permit_number": permit_id,
            "id": f"oh-npdes-{permit_id}",
            "facility_name": facility,
            "county": county,
            "sic_code": sic,
            "permit_status": status,
            "expiry_date": self._parse_epoch_ms(expiry_date),
            "match_term": match_term,
            "matched_company": None,
            "attributes": props,
        }

    async def fetch_document(self, metadata: dict) -> Optional[str]:
        """No file to download — data comes from the ArcGIS API."""
        return None

    async def run(self, limit: int | None = None) -> list:
        """Override run() to populate record fields from ArcGIS attributes."""
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
            record.company_llc_name = meta.get("facility_name")
            record.match_term = meta.get("match_term")
            record.extracted_water_metric = self._summarize_permit(meta)
            record.extracted_quote = self._build_quote(meta)

            await self.state_manager.mark_fetched(self.name, doc_id)
            results.append(record)
            count += 1

        self.logger.info("scraper_finished", total_documents=count)
        return results

    def _summarize_permit(self, meta: dict) -> Optional[str]:
        """Summarize permit attributes as a water metric string."""
        parts = []
        if meta.get("sic_code") == _DC_SIC_CODE:
            parts.append("SIC 7374 (data center)")
        if meta.get("permit_status"):
            parts.append(f"Status: {meta['permit_status']}")
        if meta.get("county"):
            parts.append(f"County: {meta['county']}")
        return " | ".join(parts) if parts else None

    def _build_quote(self, meta: dict) -> Optional[str]:
        parts = [f"Permit: {meta.get('permit_number', '')}"]
        if meta.get("facility_name"):
            parts.append(f"Facility: {meta['facility_name']}")
        if meta.get("county"):
            parts.append(f"County: {meta['county']}")
        if meta.get("sic_code"):
            parts.append(f"SIC: {meta['sic_code']}")
        return " | ".join(parts)

    def _parse_epoch_ms(self, val) -> Optional[datetime]:
        """Parse ArcGIS epoch-millisecond timestamps."""
        if val is None:
            return None
        try:
            return datetime.utcfromtimestamp(int(val) / 1000)
        except (ValueError, TypeError):
            return None
