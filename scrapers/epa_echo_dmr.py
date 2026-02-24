"""Scraper for EPA ECHO (Enforcement & Compliance History Online) DMR data.

Fetches Discharge Monitoring Report data from the federal EPA ECHO REST API.
Data centers discharge cooling water blowdown to municipal sewer systems,
so we track the receiving wastewater treatment plants (WWTPs) that hold
NPDES permits with actual flow measurements.

No browser automation needed — pure REST API with JSON/CSV responses.
"""

from __future__ import annotations

import csv
import io
from datetime import datetime
from typing import AsyncGenerator, Optional

import structlog

from models.document import DocumentRecord, DocumentSource
from scrapers.base import BaseScraper
from utils.http_client import RateLimitedClient

logger = structlog.get_logger()

# EPA ECHO API endpoints
ECHO_SUMMARY_URL = "https://echodata.epa.gov/echo/eff_rest_services.get_summary_chart"
ECHO_CSV_URL = "https://echodata.epa.gov/echo/eff_rest_services.download_effluent_chart"

# EPA ECHO parameter code for flow measurement
FLOW_PARAM_CODE = "50050"
FLOW_PARAM_DESC = "Flow, in conduit or thru treatment plant"


class EPAEchoDMRScraper(BaseScraper):
    """Scraper for EPA ECHO DMR effluent data.

    Queries the ECHO Effluent REST API for discharge monitoring reports
    from wastewater treatment plants that serve data center clusters.
    Extracts flow (MGD) measurements and other water quality parameters.
    """

    @property
    def name(self) -> str:
        return "epa_echo_dmr"

    @property
    def source(self) -> DocumentSource:
        return DocumentSource.EPA_ECHO_DMR

    async def discover(self, limit: int | None = None) -> AsyncGenerator[dict, None]:
        """Query EPA ECHO for DMR data from target WWTP permits."""
        target_permits = self.config.get("epa_echo_target_permits", [])
        if not target_permits:
            self.logger.warning("no_target_permits_configured")
            return

        count = 0

        async with RateLimitedClient(
            min_delay=self.config["min_delay"],
            max_delay=self.config["max_delay"],
        ) as client:
            for permit_id in target_permits:
                if limit and count >= limit:
                    return

                # Step 1: Get facility info for this permit
                facility_info = await self._get_facility_info(client, permit_id)
                facility_name = facility_info.get("name", permit_id)
                state = permit_id[:2]

                # Step 2: Get DMR effluent data (flow records first)
                dmr_records = await self._get_dmr_data(client, permit_id)

                # Sort: flow records (50050) first, then other water params
                dmr_records = self._prioritize_flow_records(dmr_records)

                for dmr in dmr_records:
                    if limit and count >= limit:
                        return

                    monitoring_period = dmr.get("monitoring_period_end_date", "")
                    param_desc = dmr.get("parameter_desc", "")
                    param_code = dmr.get("parameter_code", "")
                    outfall = dmr.get("perm_feature_nmbr", "")

                    # Build a unique ID per DMR record
                    doc_id = (
                        f"echo-{permit_id}-{outfall}-{param_code}-{monitoring_period}"
                    )

                    # Format the water metric
                    metric = self._format_metric(dmr)

                    yield {
                        "url": f"https://echo.epa.gov/detailed-facility-report?fid={permit_id}",
                        "title": (
                            f"DMR: {facility_name} — {param_desc} "
                            f"(Outfall {outfall}, {monitoring_period})"
                        ),
                        "date": self._parse_date(monitoring_period),
                        "state": state,
                        "agency": "EPA ECHO",
                        "permit_number": permit_id,
                        "facility_name": facility_name,
                        "id": doc_id,
                        "match_term": f"EPA ECHO target permit: {permit_id}",
                        "matched_company": None,
                        "dmr_data": dmr,
                        "water_metric": metric,
                        "document_url": (
                            f"https://echo.epa.gov/detailed-facility-report"
                            f"?fid={permit_id}#cwa-discharge-monitoring"
                        ),
                    }
                    count += 1

        self.logger.info("echo_discovery_complete", total_records=count)

    async def fetch_document(self, metadata: dict) -> Optional[str]:
        """No file to download — data comes from the API response."""
        return None

    async def run(self, limit: int | None = None) -> list:
        """Override run() since this scraper produces records directly from API data."""
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
            record.extracted_water_metric = meta.get("water_metric")
            record.match_term = meta.get("match_term")

            # Build a context quote from the DMR data
            dmr = meta.get("dmr_data", {})
            record.extracted_quote = self._build_quote(dmr, meta.get("facility_name", ""))

            await self.state_manager.mark_fetched(self.name, doc_id)
            results.append(record)
            count += 1

        self.logger.info("scraper_finished", total_documents=count)
        return results

    async def _get_facility_info(self, client: RateLimitedClient, permit_id: str) -> dict:
        """Get facility name and metadata from ECHO summary chart endpoint."""
        try:
            resp = await client.get(
                ECHO_SUMMARY_URL,
                params={"p_id": permit_id, "output": "JSON"},
            )
            data = resp.json()
            results = data.get("Results", {})
            name = results.get("CWPName", permit_id)
            return {
                "name": name,
                "city": results.get("CWPCity", ""),
                "state": results.get("CWPState", ""),
                "permit_status": results.get("CWPPermitStatusDesc", ""),
            }
        except Exception as e:
            self.logger.warning("facility_info_failed", permit_id=permit_id, error=str(e))

        return {"name": permit_id}

    async def _get_dmr_data(self, client: RateLimitedClient, permit_id: str) -> list[dict]:
        """Fetch DMR effluent records from EPA ECHO CSV download endpoint.

        Uses download_effluent_chart which returns complete DMR data as CSV
        including flow measurements, pollutant limits, and actual values.
        """
        records = []

        try:
            resp = await client.get(
                ECHO_CSV_URL,
                params={"p_id": permit_id},
            )
            reader = csv.DictReader(io.StringIO(resp.text))
            records = list(reader)

            self.logger.info(
                "dmr_data_fetched",
                permit_id=permit_id,
                record_count=len(records),
            )

        except Exception as e:
            self.logger.error("dmr_fetch_failed", permit_id=permit_id, error=str(e))

        return records

    def _prioritize_flow_records(self, records: list[dict]) -> list[dict]:
        """Sort DMR records so flow measurements (50050) come first.

        Flow data is the most relevant for tracking water usage at
        treatment plants serving data centers.
        """
        # Priority tiers: flow > other water-related > everything else
        flow_codes = {FLOW_PARAM_CODE}
        water_codes = {
            "00010",  # Temperature
            "00300",  # Dissolved Oxygen
            "00400",  # pH
            "00530",  # Total Suspended Solids
            "00600",  # Total Nitrogen
            "00665",  # Total Phosphorus
        }

        def sort_key(r):
            code = r.get("parameter_code", "")
            if code in flow_codes:
                return 0
            if code in water_codes:
                return 1
            return 2

        return sorted(records, key=sort_key)

    def _format_metric(self, dmr: dict) -> Optional[str]:
        """Format a DMR record into a human-readable water metric string."""
        parts = []

        param_desc = dmr.get("parameter_desc", dmr.get("ParameterDesc", ""))
        param_code = dmr.get("parameter_code", dmr.get("ParameterCode", ""))

        # Look for various value fields in ECHO response
        value_fields = [
            ("dmr_value_nmbr", "DMRValueNmbr"),
            ("quantity_avg", "QuantityAvg"),
            ("quantity_max", "QuantityMax"),
            ("concentration_avg", "ConcentrationAvg"),
            ("concentration_max", "ConcentrationMax"),
            ("statistical_base_monthly_avg", "StatisticalBaseMonthlyAvg"),
        ]

        for snake_key, camel_key in value_fields:
            val = dmr.get(snake_key) or dmr.get(camel_key)
            if val and str(val).strip() and str(val).strip() != "None":
                units = (
                    dmr.get("standard_unit_desc")
                    or dmr.get("StandardUnitDesc")
                    or ""
                )
                label = snake_key.replace("_", " ").title()
                parts.append(f"{label}: {val} {units}".strip())

        if not parts:
            return None

        prefix = param_desc or f"Parameter {param_code}"
        return f"{prefix}: {'; '.join(parts)}"

    def _build_quote(self, dmr: dict, facility_name: str) -> Optional[str]:
        """Build a context quote from DMR data for the extracted_quote field."""
        parts = [f"Facility: {facility_name}"]

        period = (
            dmr.get("monitoring_period_end_date")
            or dmr.get("MonitoringPeriodEndDate")
            or ""
        )
        if period:
            parts.append(f"Monitoring Period: {period}")

        param = dmr.get("parameter_desc") or dmr.get("ParameterDesc") or ""
        if param:
            parts.append(f"Parameter: {param}")

        outfall = dmr.get("perm_feature_nmbr") or dmr.get("PermFeatureNmbr") or ""
        if outfall:
            parts.append(f"Outfall: {outfall}")

        # Add limit info if available
        limit_val = (
            dmr.get("limit_value_nmbr")
            or dmr.get("LimitValueNmbr")
            or ""
        )
        if limit_val and str(limit_val).strip():
            limit_type = (
                dmr.get("limit_value_type_code")
                or dmr.get("LimitValueTypeCode")
                or ""
            )
            parts.append(f"Permit Limit ({limit_type}): {limit_val}")

        quote = " | ".join(parts)
        return quote[:500]

    def _parse_date(self, date_str: str | None) -> Optional[datetime]:
        if not date_str:
            return None
        try:
            # ECHO dates come in various formats
            for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"):
                try:
                    return datetime.strptime(date_str, fmt)
                except ValueError:
                    continue
            return datetime.fromisoformat(date_str)
        except (ValueError, TypeError):
            return None
