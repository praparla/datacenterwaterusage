"""Scraper for EPA ECHO facility discovery by NAICS code.

Queries the EPA ECHO REST API for all facilities classified under NAICS 518210
(Data Processing, Hosting, and Related Services) with any CWA/NPDES regulatory
footprint in target states (Virginia, Ohio).

This builds a registry of known data center facilities with their:
- FRS Registry IDs and geographic coordinates
- NPDES permit numbers and compliance status
- Facility names and addresses

The registry is used to:
1. Map data centers to WWTP service areas
2. Discover new permits to monitor
3. Track the overall regulatory footprint of data centers
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import AsyncGenerator, Optional

import structlog

from models.document import DocumentRecord, DocumentSource
from scrapers.base import BaseScraper
from utils.http_client import RateLimitedClient

logger = structlog.get_logger()

# EPA ECHO Facility search endpoint
ECHO_FACILITIES_URL = "https://ofmpub.epa.gov/echo/echo_rest_services.get_facilities"

# Maximum results per ECHO API call (API limit)
ECHO_MAX_RESULTS = 10000


class EPAEchoNAICSScraper(BaseScraper):
    """Discover data center facilities via EPA ECHO NAICS code search.

    Queries ECHO for NAICS 518210 facilities with CWA permits in VA and OH.
    Extracts facility metadata, permit numbers, and compliance status.
    """

    @property
    def name(self) -> str:
        return "epa_echo_naics"

    @property
    def source(self) -> DocumentSource:
        return DocumentSource.EPA_ECHO_NAICS

    async def discover(self, limit: int | None = None) -> AsyncGenerator[dict, None]:
        """Query ECHO API for data center facilities by NAICS code."""
        naics_code = self.config.get("naics_data_center", "518210")
        target_states = self.config.get("target_states", ["VA", "OH"])
        count = 0

        async with RateLimitedClient(
            min_delay=self.config["min_delay"],
            max_delay=self.config["max_delay"],
        ) as client:
            for state in target_states:
                if limit and count >= limit:
                    return

                self.logger.info(
                    "searching_echo_facilities",
                    state=state,
                    naics=naics_code,
                )

                facilities = await self._search_facilities(
                    client, naics_code, state
                )

                for facility in facilities:
                    if limit and count >= limit:
                        return

                    fac_id = facility.get("SourceID") or facility.get("FacFIPSCode", "")
                    registry_id = facility.get("RegistryID", "")
                    fac_name = facility.get("FacName", "Unknown")
                    permit_ids = facility.get("CWPPermitStatusDesc", "")

                    doc_id = f"echo-naics-{state}-{registry_id or fac_name}"

                    yield {
                        "url": (
                            f"https://echo.epa.gov/detailed-facility-report"
                            f"?fid={registry_id}"
                        ),
                        "title": (
                            f"ECHO Facility: {fac_name} ({state}) — "
                            f"NAICS {naics_code}"
                        ),
                        "date": datetime.utcnow(),
                        "state": state,
                        "agency": "EPA ECHO",
                        "id": doc_id,
                        "match_term": f"NAICS {naics_code} facility search",
                        "matched_company": self._match_known_company(fac_name),
                        "facility_data": facility,
                        "registry_id": registry_id,
                        "document_url": (
                            f"https://echo.epa.gov/detailed-facility-report"
                            f"?fid={registry_id}"
                        ),
                    }
                    count += 1

        self.logger.info("naics_discovery_complete", total_facilities=count)

    async def fetch_document(self, metadata: dict) -> Optional[str]:
        """No file to download — data comes from the API response."""
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
            facility = meta.get("facility_data", {})

            # Enrich record with facility data
            record.company_llc_name = facility.get("FacName")
            record.extracted_water_metric = self._format_facility_metric(facility)
            record.match_term = meta.get("match_term")
            record.matched_company = meta.get("matched_company")
            record.extracted_quote = self._build_facility_quote(facility)

            await self.state_manager.mark_fetched(self.name, doc_id)
            results.append(record)
            count += 1

        self.logger.info("scraper_finished", total_facilities=count)
        return results

    async def _search_facilities(
        self,
        client: RateLimitedClient,
        naics_code: str,
        state: str,
    ) -> list[dict]:
        """Query ECHO REST API for facilities by NAICS and state.

        Returns parsed facility records from the JSON response.
        """
        facilities = []

        try:
            # First call to get the query ID (QID)
            resp = await client.get(
                ECHO_FACILITIES_URL,
                params={
                    "p_naic": naics_code,
                    "p_st": state,
                    "p_med": "CWA",  # Clean Water Act programs only
                    "output": "JSON",
                    "responseset": ECHO_MAX_RESULTS,
                },
            )

            data = resp.json()

            # ECHO returns results nested under Results.Facilities
            results = data.get("Results", {})
            facility_list = results.get("Facilities", [])

            if not facility_list:
                self.logger.info(
                    "no_facilities_found",
                    state=state,
                    naics=naics_code,
                )
                return []

            # Validate each facility record
            for fac in facility_list:
                if not isinstance(fac, dict):
                    continue
                # Ensure required fields
                if not fac.get("FacName"):
                    continue
                facilities.append(fac)

            self.logger.info(
                "facilities_found",
                state=state,
                count=len(facilities),
            )

        except Exception as e:
            self.logger.error(
                "echo_facility_search_failed",
                state=state,
                naics=naics_code,
                error=str(e),
            )

        return facilities

    def _format_facility_metric(self, facility: dict) -> Optional[str]:
        """Format key facility data as a readable metric string."""
        parts = []

        name = facility.get("FacName", "")
        if name:
            parts.append(f"Facility: {name}")

        # NPDES permit info
        npdes = facility.get("CWPPermitStatusDesc", "")
        if npdes:
            parts.append(f"NPDES Status: {npdes}")

        # Compliance status
        compliance = facility.get("CWPComplianceStatus", "")
        if compliance:
            parts.append(f"Compliance: {compliance}")

        # SIC/NAICS
        naics = facility.get("NAICSCodes", "")
        if naics:
            parts.append(f"NAICS: {naics}")

        return "; ".join(parts) if parts else None

    def _build_facility_quote(self, facility: dict) -> Optional[str]:
        """Build a context quote from facility data."""
        parts = []

        for key in [
            "FacName", "FacStreet", "FacCity", "FacState", "FacZip",
            "RegistryID", "CWPPermitStatusDesc", "CWPComplianceStatus",
            "FacLat", "FacLong", "NAICSCodes",
        ]:
            val = facility.get(key)
            if val and str(val).strip():
                parts.append(f"{key}: {val}")

        quote = " | ".join(parts)
        return quote[:500] if quote else None

    def _match_known_company(self, facility_name: str) -> Optional[str]:
        """Check if facility name matches a known data center company."""
        if not facility_name:
            return None

        try:
            from utils.matching import is_facility_match

            known = self.config.get("known_companies", [])
            for company in known:
                if is_facility_match(facility_name, company):
                    return company
        except ImportError:
            pass

        return None
