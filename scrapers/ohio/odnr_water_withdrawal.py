"""Scraper for ODNR Water Withdrawal Facility Viewer (Ohio).

Ohio requires registration for facilities withdrawing >100,000 GPD. The
ODNR maintains an ArcGIS-based facility viewer showing historical annual
withdrawal volumes per facility. This captures municipal water systems
serving data center clusters (Columbus, New Albany, Newark area) and
any self-supplied industrial facilities.

ArcGIS Experience Builder app (confirmed live Feb 2026):
  https://experience.arcgis.com/experience/0605c2eaf8fe458481ac323404b4ab36

The backing ArcGIS FeatureServer is queried directly. Target counties:
Franklin, Licking, Delaware, Union (central Ohio data center cluster).

If the configured service URL returns errors, the scraper falls back to
the app definition endpoint to auto-discover the FeatureServer URL.

No browser automation needed — ArcGIS REST API.
"""

from __future__ import annotations

from datetime import datetime
from typing import AsyncGenerator, Optional

from models.document import DocumentSource
from scrapers.base import BaseScraper
from utils.http_client import RateLimitedClient

# ArcGIS Online item ID for the ODNR Water Withdrawal Experience Builder app
_APP_ITEM_ID = "0605c2eaf8fe458481ac323404b4ab36"

# ArcGIS Online REST endpoint to fetch the app's item data (reveals service URLs)
_ITEM_DATA_URL = (
    f"https://www.arcgis.com/sharing/rest/content/items/{_APP_ITEM_ID}/data?f=json"
)

# Expected county values in the ODNR dataset
_TARGET_COUNTIES = ["Franklin", "Licking", "Delaware", "Union"]

# Water source type codes that indicate municipal supply (used by data centers)
_MUNICIPAL_SOURCE_TYPES = {"Municipal", "Public Water System", "PWS", "Surface Water"}


class ODNRWaterWithdrawalScraper(BaseScraper):
    """Scraper for Ohio ODNR water withdrawal registrations.

    Queries the ArcGIS FeatureServer backing the ODNR Water Withdrawal
    Facility Viewer for facilities in central Ohio data center counties.
    Extracts annual withdrawal volumes and water source types.
    """

    @property
    def name(self) -> str:
        return "oh_odnr_water_withdrawal"

    @property
    def source(self) -> DocumentSource:
        return DocumentSource.OH_ODNR_WATER_WITHDRAWAL

    async def discover(self, limit: int | None = None) -> AsyncGenerator[dict, None]:
        """Query ODNR ArcGIS service for water withdrawal facilities."""
        service_url = self.config.get("oh_odnr_water_withdrawal_service", "")
        viewer_url = self.config["oh_odnr_water_withdrawal_viewer"]
        target_counties = self.config.get("oh_odnr_target_counties", _TARGET_COUNTIES)
        seen_ids: set[str] = set()
        count = 0

        async with RateLimitedClient(
            min_delay=self.config["min_delay"],
            max_delay=self.config["max_delay"],
        ) as client:
            # If no service URL configured, attempt to discover it from the app definition
            if not service_url:
                service_url = await self._discover_service_url(client)
                if not service_url:
                    self.logger.error(
                        "no_service_url",
                        msg="Could not determine ODNR FeatureServer URL. "
                            "Inspect network requests on the Experience Builder app "
                            "to find the backing FeatureServer and set "
                            "oh_odnr_water_withdrawal_service in config.py.",
                    )
                    return

            for county in target_counties:
                features = await self._query_county(client, service_url, county)
                for feat in features:
                    if limit and count >= limit:
                        return

                    meta = self._feature_to_meta(feat, viewer_url)
                    if meta and meta["id"] not in seen_ids:
                        seen_ids.add(meta["id"])
                        yield meta
                        count += 1

        self.logger.info("odnr_discovery_complete", count=count)

    async def _discover_service_url(self, client: RateLimitedClient) -> Optional[str]:
        """Attempt to extract the FeatureServer URL from the Experience Builder app config."""
        try:
            resp = await client.get(_ITEM_DATA_URL)
            # The app JSON contains datasource configs that reference FeatureServer URLs
            text = resp.text
            import re
            # Look for a FeatureServer URL pattern in the app config JSON
            match = re.search(
                r'"(https://[^"]+/FeatureServer/\d+)"',
                text,
            )
            if match:
                base = match.group(1)
                self.logger.info("discovered_service_url", url=base)
                return f"{base}/query"
        except Exception as e:
            self.logger.warning("service_url_discovery_failed", error=str(e))
        return None

    async def _query_county(
        self,
        client: RateLimitedClient,
        service_url: str,
        county: str,
    ) -> list[dict]:
        """Query the FeatureServer for a specific county."""
        try:
            resp = await client.get(
                service_url,
                params={
                    "where": f"COUNTY='{county}' OR COUNTY_NAME='{county}'",
                    "outFields": "*",
                    "f": "json",
                    "resultRecordCount": "2000",
                },
            )
            data = resp.json()
            features = data.get("features", [])
            self.logger.info("odnr_county_queried", county=county, records=len(features))
            return features
        except Exception as e:
            self.logger.error("odnr_query_failed", county=county, error=str(e))
            return []

    def _feature_to_meta(self, feature: dict, viewer_url: str) -> Optional[dict]:
        """Convert an ArcGIS feature to scraper metadata."""
        attrs = feature.get("attributes", {})
        if not attrs:
            return None

        fac_id = str(
            attrs.get("FACILITY_ID")
            or attrs.get("FacilityID")
            or attrs.get("OBJECTID")
            or ""
        ).strip()
        if not fac_id:
            return None

        fac_name = str(
            attrs.get("FACILITY_NAME") or attrs.get("FacilityName") or ""
        ).strip()
        county = str(
            attrs.get("COUNTY") or attrs.get("COUNTY_NAME") or ""
        ).strip()
        source_type = str(
            attrs.get("WATER_SOURCE_TYPE") or attrs.get("SOURCE_TYPE") or ""
        ).strip()
        annual_vol = (
            attrs.get("ANNUAL_WITHDRAWAL_VOLUME")
            or attrs.get("AnnualWithdrawalVolume")
            or attrs.get("ANNUAL_VOLUME_GAL")
        )
        report_year = attrs.get("REPORT_YEAR") or attrs.get("ReportYear")

        volume_str = None
        if annual_vol is not None:
            try:
                vol_mgd = float(annual_vol) / 1_000_000 / 365
                volume_str = f"{float(annual_vol):,.0f} gal/yr ({vol_mgd:.2f} MGD avg)"
            except (ValueError, TypeError):
                volume_str = str(annual_vol)

        return {
            "url": viewer_url,
            "document_url": viewer_url,
            "title": f"ODNR Water Withdrawal: {fac_name} ({county} County)",
            "date": datetime(int(report_year), 1, 1) if report_year else None,
            "state": "OH",
            "agency": "Ohio ODNR",
            "id": f"odnr-withdrawal-{fac_id}",
            "facility_name": fac_name,
            "county": county,
            "source_type": source_type,
            "annual_volume": annual_vol,
            "volume_str": volume_str,
            "report_year": report_year,
            "match_term": f"ODNR water withdrawal registration — {county} County",
            "matched_company": None,
            "attributes": attrs,
        }

    async def fetch_document(self, metadata: dict) -> Optional[str]:
        """No file to download — data comes from the ArcGIS API."""
        return None

    async def run(self, limit: int | None = None) -> list:
        """Override run() to populate record fields from ArcGIS attributes."""
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
            record.extracted_water_metric = meta.get("volume_str")
            record.extracted_quote = self._build_quote(meta)

            await self.state_manager.mark_fetched(self.name, doc_id)
            results.append(record)
            count += 1

        self.logger.info("scraper_finished", total_documents=count)
        return results

    def _build_quote(self, meta: dict) -> Optional[str]:
        parts = []
        if meta.get("facility_name"):
            parts.append(f"Facility: {meta['facility_name']}")
        if meta.get("county"):
            parts.append(f"County: {meta['county']}")
        if meta.get("source_type"):
            parts.append(f"Source: {meta['source_type']}")
        if meta.get("volume_str"):
            parts.append(f"Annual withdrawal: {meta['volume_str']}")
        if meta.get("report_year"):
            parts.append(f"Report year: {meta['report_year']}")
        return " | ".join(parts) if parts else None
