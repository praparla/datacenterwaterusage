"""Scraper for Ohio EPA Data Center General Permit (OHD000001).

Ohio EPA has drafted the first-ever NPDES general permit specifically for data
center wastewater discharges (cooling tower blowdown, non-contact cooling water).
Once finalized (expected 2026), data centers that obtain coverage will file
Notices of Intent (NOIs) and report discharge monitoring data (DMR).

This scraper:
1. Monitors the Ohio EPA general permits list for OHD000001 status changes
2. Downloads the draft permit PDF and fact sheet for text extraction
3. Tracks facilities that file NOIs for coverage (once available)
4. Documents the monitoring parameters (flow, pH, TDS, chlorine, temperature)

This is a "tracker" scraper — it checks status and captures metadata rather
than scraping live data. Once the permit is finalized and NOIs start appearing,
the facilities will be added to epa_echo_dmr.py's target list automatically.
"""

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import AsyncGenerator, Optional

import structlog

from models.document import DocumentRecord, DocumentSource
from scrapers.base import BaseScraper
from utils.http_client import RateLimitedClient

logger = structlog.get_logger()

# Known monitoring parameters in the draft general permit
DRAFT_PERMIT_PARAMETERS = [
    {"code": "50050", "name": "Flow", "unit": "MGD", "frequency": "Monthly"},
    {"code": "00400", "name": "pH", "unit": "S.U.", "frequency": "Monthly"},
    {"code": "70295", "name": "TDS (Total Dissolved Solids)", "unit": "mg/L", "frequency": "Quarterly"},
    {"code": "50060", "name": "Total Residual Chlorine", "unit": "µg/L", "frequency": "Monthly"},
    {"code": "00010", "name": "Temperature", "unit": "°F", "frequency": "Monthly"},
    {"code": "00530", "name": "TSS (Total Suspended Solids)", "unit": "mg/L", "frequency": "Monthly"},
    {"code": "00556", "name": "Oil & Grease", "unit": "mg/L", "frequency": "Monthly"},
]

# Status tracking for the general permit
PERMIT_STATUS_LABELS = {
    "draft": "Draft — public comment period",
    "final": "Final — effective, NOIs accepted",
    "expired": "Expired",
    "pending": "Pending — under development",
}


class OhioEPAGeneralPermitScraper(BaseScraper):
    """Track the Ohio EPA Data Center General Permit (OHD000001).

    Monitors permit status, downloads draft/final documents, and will
    track facility NOIs once the permit is finalized.
    """

    @property
    def name(self) -> str:
        return "oh_epa_general_permit"

    @property
    def source(self) -> DocumentSource:
        return DocumentSource.OH_EPA_GENERAL_PERMIT

    async def discover(self, limit: int | None = None) -> AsyncGenerator[dict, None]:
        """Discover documents related to OHD000001.

        Yields metadata for:
        1. The general permits list page (status check)
        2. The draft permit PDF
        3. The fact sheet PDF
        4. Any NOI/coverage list (once available)
        """
        count = 0

        async with RateLimitedClient(
            min_delay=self.config["min_delay"],
            max_delay=self.config["max_delay"],
        ) as client:
            # 1. Check general permits list page for status
            status_info = await self._check_permit_status(client)

            yield {
                "url": self.config.get(
                    "oh_epa_general_permits_url",
                    "https://www.epa.state.oh.us/dsw/permits/gplist",
                ),
                "title": (
                    f"Ohio EPA General Permit OHD000001 — Data Center Wastewater "
                    f"(Status: {status_info.get('status', 'unknown')})"
                ),
                "date": datetime.utcnow(),
                "state": "OH",
                "agency": "Ohio EPA",
                "id": f"oh-gp-ohd000001-status-{datetime.utcnow().strftime('%Y%m')}",
                "match_term": "Ohio EPA data center general permit OHD000001",
                "matched_company": None,
                "status_info": status_info,
                "document_type": "status_check",
            }
            count += 1
            if limit and count >= limit:
                return

            # 2. Draft permit PDF
            draft_url = self.config.get("oh_epa_dc_permit_draft_pdf")
            if draft_url:
                yield {
                    "url": draft_url,
                    "document_url": draft_url,
                    "title": "Ohio EPA OHD000001 Draft General Permit — Data Center Wastewater",
                    "date": None,
                    "state": "OH",
                    "agency": "Ohio EPA",
                    "id": "oh-gp-ohd000001-draft-permit",
                    "match_term": "Ohio EPA data center general permit draft",
                    "matched_company": None,
                    "document_type": "draft_permit",
                    "filename": "OHD000001_Draft.pdf",
                    "parameters": DRAFT_PERMIT_PARAMETERS,
                }
                count += 1
                if limit and count >= limit:
                    return

            # 3. Fact sheet PDF
            fact_sheet_url = self.config.get("oh_epa_dc_permit_fact_sheet_pdf")
            if fact_sheet_url:
                yield {
                    "url": fact_sheet_url,
                    "document_url": fact_sheet_url,
                    "title": "Ohio EPA OHD000001 Draft Fact Sheet — Data Center Wastewater",
                    "date": None,
                    "state": "OH",
                    "agency": "Ohio EPA",
                    "id": "oh-gp-ohd000001-fact-sheet",
                    "match_term": "Ohio EPA data center general permit fact sheet",
                    "matched_company": None,
                    "document_type": "fact_sheet",
                    "filename": "OHD000001_Draft_FactSheet.pdf",
                }
                count += 1
                if limit and count >= limit:
                    return

            # 4. Check for NOI list (facilities that filed Notice of Intent)
            noi_facilities = await self._check_noi_list(client)
            for fac in noi_facilities:
                if limit and count >= limit:
                    return

                yield {
                    "url": fac.get("url", ""),
                    "title": (
                        f"OHD000001 NOI: {fac.get('name', 'Unknown')} — "
                        f"Data Center Wastewater"
                    ),
                    "date": fac.get("noi_date"),
                    "state": "OH",
                    "agency": "Ohio EPA",
                    "id": f"oh-gp-ohd000001-noi-{fac.get('permit_id', 'unknown')}",
                    "match_term": "Ohio EPA OHD000001 NOI coverage",
                    "matched_company": fac.get("matched_company"),
                    "document_type": "noi",
                    "facility_data": fac,
                }
                count += 1

        self.logger.info("general_permit_discovery_complete", total=count)

    async def fetch_document(self, metadata: dict) -> Optional[str]:
        """Download PDF documents (draft permit, fact sheet)."""
        doc_url = metadata.get("document_url")
        doc_type = metadata.get("document_type", "")

        # Status checks and NOIs don't have files to download
        if doc_type in ("status_check", "noi") or not doc_url:
            return None

        filename = metadata.get("filename", f"ohd000001_{doc_type}.pdf")
        local_path = self.file_store.get_path("ohio", "epa_general_permit", filename)

        if Path(local_path).exists() and Path(local_path).stat().st_size > 0:
            self.logger.info("permit_pdf_already_downloaded", path=local_path)
            return local_path

        async with RateLimitedClient(
            min_delay=self.config["min_delay"],
            max_delay=self.config["max_delay"],
        ) as client:
            try:
                return await client.download_file(doc_url, local_path)
            except Exception as e:
                self.logger.error(
                    "permit_pdf_download_failed",
                    url=doc_url,
                    error=str(e),
                )
                return None

    async def _check_permit_status(self, client: RateLimitedClient) -> dict:
        """Check the Ohio EPA general permits list for OHD000001 status.

        Returns a dict with status, effective_date, expiration_date, etc.
        """
        gp_url = self.config.get(
            "oh_epa_general_permits_url",
            "https://www.epa.state.oh.us/dsw/permits/gplist",
        )

        status_info = {
            "permit_id": "OHD000001",
            "status": "draft",
            "description": "General NPDES Permit for Data Center Wastewater Discharges",
            "parameters": DRAFT_PERMIT_PARAMETERS,
            "checked_at": datetime.utcnow().isoformat(),
        }

        try:
            resp = await client.get(gp_url)
            html = resp.text.lower()

            # Look for OHD000001 on the page
            if "ohd000001" in html:
                status_info["found_on_page"] = True

                # Check for finalization indicators
                if any(
                    kw in html
                    for kw in ["effective", "final", "noi", "notice of intent"]
                ):
                    if "ohd000001" in html and "effective" in html:
                        status_info["status"] = "final"
                        self.logger.info(
                            "permit_status_change_detected",
                            permit="OHD000001",
                            new_status="final",
                        )
                elif "draft" in html or "public comment" in html:
                    status_info["status"] = "draft"
                elif "expired" in html:
                    status_info["status"] = "expired"
            else:
                status_info["found_on_page"] = False
                # Permit not yet listed — still in development
                status_info["status"] = "pending"

        except Exception as e:
            self.logger.warning(
                "permit_status_check_failed",
                url=gp_url,
                error=str(e),
            )
            status_info["status"] = "unknown"
            status_info["error"] = str(e)

        return status_info

    async def _check_noi_list(self, client: RateLimitedClient) -> list[dict]:
        """Check for facilities that have filed NOIs for OHD000001 coverage.

        Returns empty list until the permit is finalized and NOIs are filed.
        This is a placeholder that will be updated when the permit goes final.
        """
        # Once the permit is finalized, Ohio EPA will publish a list of
        # facilities with NOI coverage. The URL pattern is typically:
        # https://epa.ohio.gov/dsw/permits/gp_noi_[permit_id]
        #
        # For now, return empty — no NOIs exist for a draft permit.
        return []

    @staticmethod
    def get_monitoring_parameters() -> list[dict]:
        """Return the known monitoring parameters from the draft permit.

        These parameters will be required in DMR reporting once the permit
        is finalized. Useful for validation and display purposes.
        """
        return list(DRAFT_PERMIT_PARAMETERS)

    @staticmethod
    def format_permit_summary(status_info: dict) -> str:
        """Format permit status info into a human-readable summary."""
        parts = [
            f"Permit: {status_info.get('permit_id', 'OHD000001')}",
            f"Status: {status_info.get('status', 'unknown')}",
            f"Description: {status_info.get('description', '')}",
        ]

        params = status_info.get("parameters", [])
        if params:
            param_names = [p["name"] for p in params]
            parts.append(f"Required Parameters: {', '.join(param_names)}")

        return " | ".join(parts)
