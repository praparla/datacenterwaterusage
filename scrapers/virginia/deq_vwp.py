"""Scraper for Virginia DEQ Water Withdrawal Permits (VWP) via ArcGIS.

Queries the DEQ EDMA MapServer for VWP layers:
  - Layer 192: VWP Individual Permits
  - Layer 193: VWP General Permits (by registration)

Virginia requires reporting for all withdrawals >10,000 GPD. In 2023,
1,174 facilities reported. Data centers with their own wells appear here,
as do municipal utilities serving data centers (Loudoun Water, Fairfax Water).

No browser automation needed — pure ArcGIS REST API queries.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import AsyncGenerator, Optional

import structlog

from models.document import DocumentRecord, DocumentSource
from scrapers.base import BaseScraper
from utils.http_client import RateLimitedClient
from utils.matching import get_match_reason

logger = structlog.get_logger()

# DEQ EDMA MapServer base URL
_EDMA_BASE = "https://apps.deq.virginia.gov/arcgis/rest/services/public/EDMA/MapServer"

# VWP Layers on the EDMA MapServer
VWP_LAYERS = {
    "individual": {
        "id": 192,
        "name": "VWP Individual Permits",
        "description": "Surface water and groundwater withdrawal individual permits",
    },
    "general": {
        "id": 193,
        "name": "VWP General Permits (by Registration)",
        "description": "General permits for water withdrawals registered by facility",
    },
}

# Northern Virginia FIPS county codes for filtering
# (Loudoun, Fairfax, Prince William, Arlington, Fauquier — the DC corridor)
NOVA_COUNTIES = {
    "107": "Loudoun",
    "059": "Fairfax",
    "153": "Prince William",
    "013": "Arlington",
    "061": "Fauquier",
}

# Fields commonly available on VWP layers
_VWP_FIELDS = [
    "OBJECTID", "VWP_PMT_NO", "PERMITTEE", "FAC_NAME",
    "COUNTY", "FIPS", "SOURCE_NAME", "SOURCE_TYPE",
    "MAX_WITHDRAW_GPD", "AVG_WITHDRAW_GPD",
    "PERMIT_STATUS", "ISSUE_DATE", "EXPIRE_DATE",
    "LATITUDE", "LONGITUDE",
]


class DEQVWPScraper(BaseScraper):
    """Scraper for Virginia DEQ Water Withdrawal Permits (VWP).

    Queries ArcGIS layers for water withdrawal permits in Northern Virginia
    counties where data center clusters are located.
    """

    @property
    def name(self) -> str:
        return "va_deq_vwp"

    @property
    def source(self) -> DocumentSource:
        return DocumentSource.VA_DEQ_VWP

    async def discover(self, limit: int | None = None) -> AsyncGenerator[dict, None]:
        """Query VWP layers for withdrawal permits in target counties."""
        target_counties = self.config.get("va_vwp_target_counties", list(NOVA_COUNTIES.keys()))
        known_companies = self.config.get("known_companies", [])
        count = 0

        async with RateLimitedClient(
            min_delay=self.config["min_delay"],
            max_delay=self.config["max_delay"],
        ) as client:
            for layer_key, layer_info in VWP_LAYERS.items():
                if limit and count >= limit:
                    return

                layer_url = f"{_EDMA_BASE}/{layer_info['id']}/query"

                self.logger.info(
                    "querying_vwp_layer",
                    layer=layer_info["name"],
                    layer_id=layer_info["id"],
                )

                # Build WHERE clause for target counties
                where_clause = self._build_county_where(target_counties)

                facilities = await self._query_layer(
                    client, layer_url, where_clause
                )

                for fac in facilities:
                    if limit and count >= limit:
                        return

                    attrs = fac.get("attributes", {})
                    permittee = str(attrs.get("PERMITTEE", "")).strip()
                    fac_name = str(attrs.get("FAC_NAME", "")).strip()
                    permit_no = str(attrs.get("VWP_PMT_NO", "")).strip()

                    display_name = fac_name or permittee or "Unknown"

                    # Check for data center company match
                    match_reason = get_match_reason(
                        f"{permittee} {fac_name}", known_companies
                    )

                    # Include all permits (not just DC matches) — water utilities
                    # serving DC areas are equally important
                    doc_id = f"vwp-{layer_key}-{permit_no or attrs.get('OBJECTID', '')}"

                    yield {
                        "url": f"{layer_url}?where=VWP_PMT_NO='{permit_no}'&f=html",
                        "title": (
                            f"VWP {layer_info['name']}: {display_name} — "
                            f"{permit_no}"
                        ),
                        "date": self._parse_date(attrs.get("ISSUE_DATE")),
                        "state": "VA",
                        "agency": "Virginia DEQ",
                        "permit_number": permit_no,
                        "id": doc_id,
                        "match_term": (
                            f"VWP {layer_key} permit — "
                            f"county {attrs.get('COUNTY', 'unknown')}"
                        ),
                        "matched_company": match_reason,
                        "attributes": attrs,
                        "layer": layer_key,
                    }
                    count += 1

        self.logger.info("vwp_discovery_complete", total=count)

    async def fetch_document(self, metadata: dict) -> Optional[str]:
        """No file to download — data comes from the ArcGIS API response."""
        return None

    async def run(self, limit: int | None = None) -> list[DocumentRecord]:
        """Override run to build records directly from API data."""
        self.logger.info("scraper_starting", limit=limit)
        results = []
        count = 0

        async for meta in self.discover(limit=limit):
            doc_id = meta.get("id", "")

            if await self.state_manager.is_fetched(self.name, doc_id):
                self.logger.debug("skipping_already_fetched", doc_id=doc_id)
                continue

            record = self._build_record(meta, None)

            # Enrich with withdrawal data
            attrs = meta.get("attributes", {})
            record.company_llc_name = (
                str(attrs.get("PERMITTEE", "")).strip() or
                str(attrs.get("FAC_NAME", "")).strip() or
                None
            )
            record.extracted_water_metric = self._format_withdrawal_metric(attrs)
            record.match_term = meta.get("match_term")
            record.matched_company = meta.get("matched_company")
            record.extracted_quote = self._build_vwp_quote(attrs)

            await self.state_manager.mark_fetched(self.name, doc_id)
            results.append(record)
            count += 1

        self.logger.info("scraper_finished", total_documents=count)
        return results

    async def _query_layer(
        self,
        client: RateLimitedClient,
        layer_url: str,
        where_clause: str,
    ) -> list[dict]:
        """Query an ArcGIS layer and return feature records."""
        features = []
        offset = 0
        batch_size = 1000

        while True:
            try:
                resp = await client.get(
                    layer_url,
                    params={
                        "where": where_clause,
                        "outFields": ",".join(_VWP_FIELDS),
                        "f": "json",
                        "resultRecordCount": str(batch_size),
                        "resultOffset": str(offset),
                    },
                )
                data = resp.json()
            except Exception as e:
                self.logger.error(
                    "vwp_query_failed",
                    url=layer_url,
                    offset=offset,
                    error=str(e),
                )
                break

            batch = data.get("features", [])
            if not batch:
                break

            features.extend(batch)

            # Check for more results
            if not data.get("exceededTransferLimit", False) or len(batch) < batch_size:
                break
            offset += batch_size

        return features

    def _build_county_where(self, county_fips: list[str]) -> str:
        """Build a WHERE clause to filter by county FIPS codes."""
        if not county_fips:
            return "1=1"

        # Try FIPS field first, fall back to COUNTY name
        fips_values = ", ".join(f"'{c}'" for c in county_fips)
        county_names = [NOVA_COUNTIES.get(c, c) for c in county_fips]
        name_values = ", ".join(f"'{n}'" for n in county_names)

        return (
            f"(FIPS IN ({fips_values}) OR COUNTY IN ({name_values}))"
        )

    def _format_withdrawal_metric(self, attrs: dict) -> Optional[str]:
        """Format withdrawal volumes from VWP attributes."""
        parts = []

        max_gpd = attrs.get("MAX_WITHDRAW_GPD")
        if max_gpd is not None:
            try:
                val = float(max_gpd)
                if val >= 1_000_000:
                    parts.append(f"Max Withdrawal: {val / 1_000_000:.2f} MGD")
                else:
                    parts.append(f"Max Withdrawal: {val:,.0f} GPD")
            except (ValueError, TypeError):
                pass

        avg_gpd = attrs.get("AVG_WITHDRAW_GPD")
        if avg_gpd is not None:
            try:
                val = float(avg_gpd)
                if val >= 1_000_000:
                    parts.append(f"Avg Withdrawal: {val / 1_000_000:.2f} MGD")
                else:
                    parts.append(f"Avg Withdrawal: {val:,.0f} GPD")
            except (ValueError, TypeError):
                pass

        source_type = attrs.get("SOURCE_TYPE", "")
        if source_type:
            parts.append(f"Source: {source_type}")

        source_name = attrs.get("SOURCE_NAME", "")
        if source_name:
            parts.append(f"Source Name: {source_name}")

        return "; ".join(parts) if parts else None

    def _build_vwp_quote(self, attrs: dict) -> Optional[str]:
        """Build a context quote from VWP permit attributes."""
        parts = []
        for key in [
            "VWP_PMT_NO", "PERMITTEE", "FAC_NAME", "COUNTY",
            "SOURCE_NAME", "SOURCE_TYPE", "PERMIT_STATUS",
            "MAX_WITHDRAW_GPD", "AVG_WITHDRAW_GPD",
            "LATITUDE", "LONGITUDE",
        ]:
            val = attrs.get(key)
            if val is not None and str(val).strip():
                parts.append(f"{key}: {val}")

        quote = " | ".join(parts)
        return quote[:500] if quote else None

    def _parse_date(self, date_val) -> Optional[datetime]:
        """Parse date from ArcGIS epoch milliseconds or string."""
        if date_val is None:
            return None

        # ArcGIS often returns dates as epoch milliseconds
        if isinstance(date_val, (int, float)):
            try:
                return datetime.utcfromtimestamp(date_val / 1000)
            except (ValueError, OSError):
                return None

        # String formats
        if isinstance(date_val, str):
            for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"):
                try:
                    return datetime.strptime(date_val, fmt)
                except ValueError:
                    continue

        return None

    @staticmethod
    def gpd_to_mgd(gpd: float) -> float:
        """Convert gallons per day to million gallons per day."""
        return gpd / 1_000_000

    @staticmethod
    def get_nova_counties() -> dict[str, str]:
        """Return the Northern Virginia county FIPS-to-name mapping."""
        return dict(NOVA_COUNTIES)
