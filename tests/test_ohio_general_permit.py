"""Tests for the Ohio EPA General Permit (OHD000001) scraper.

Tests cover:
- Monitoring parameter definitions
- Permit summary formatting
- Permit status detection logic
- Document type routing (status vs PDF vs NOI)
- Discovery yields correct metadata structure
- Run pipeline with fake data
"""

from __future__ import annotations

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
import pytest_asyncio

from scrapers.ohio.epa_general_permit import (
    OhioEPAGeneralPermitScraper,
    DRAFT_PERMIT_PARAMETERS,
    PERMIT_STATUS_LABELS,
)


# --- Fixtures ---


class FakeStateManager:
    """In-memory state manager for testing."""

    def __init__(self):
        self._fetched = set()

    async def initialize(self):
        pass

    async def is_fetched(self, scraper_name: str, doc_id: str) -> bool:
        return (scraper_name, doc_id) in self._fetched

    async def mark_fetched(self, scraper_name: str, doc_id: str, local_path=None):
        self._fetched.add((scraper_name, doc_id))

    async def mark_processed(self, scraper_name: str, doc_id: str):
        pass


class FakeFileStore:
    def get_path(self, state, agency, filename):
        return f"/tmp/{state}/{agency}/{filename}"

    def exists(self, state, agency, filename):
        return False


@pytest.fixture
def config():
    return {
        "min_delay": 0.0,
        "max_delay": 0.0,
        "downloads_dir": "/tmp/downloads",
        "oh_epa_general_permits_url": "https://www.epa.state.oh.us/dsw/permits/gplist",
        "oh_epa_dc_general_permit_id": "OHD000001",
        "oh_epa_dc_permit_draft_pdf": (
            "https://dam.assets.ohio.gov/image/upload/epa.ohio.gov/"
            "Portals/35/permits/Data_Centers/OHD000001_Draft.pdf"
        ),
        "oh_epa_dc_permit_fact_sheet_pdf": (
            "https://dam.assets.ohio.gov/image/upload/epa.ohio.gov/"
            "Portals/35/permits/Data_Centers/OHD000001_Draft.fs.pdf"
        ),
    }


@pytest.fixture
def scraper(config):
    return OhioEPAGeneralPermitScraper(
        config=config,
        state_manager=FakeStateManager(),
        file_store=FakeFileStore(),
        browser=None,
    )


# --- Tests for monitoring parameters ---


class TestMonitoringParameters:
    def test_parameters_are_defined(self):
        """Draft permit has known monitoring parameters."""
        params = OhioEPAGeneralPermitScraper.get_monitoring_parameters()
        assert len(params) == 7

    def test_flow_parameter_present(self):
        """Flow (50050) is a required parameter."""
        params = OhioEPAGeneralPermitScraper.get_monitoring_parameters()
        flow_params = [p for p in params if p["code"] == "50050"]
        assert len(flow_params) == 1
        assert flow_params[0]["name"] == "Flow"
        assert flow_params[0]["unit"] == "MGD"

    def test_ph_parameter_present(self):
        """pH (00400) is a required parameter."""
        params = OhioEPAGeneralPermitScraper.get_monitoring_parameters()
        ph_params = [p for p in params if p["code"] == "00400"]
        assert len(ph_params) == 1
        assert ph_params[0]["name"] == "pH"

    def test_tds_parameter_present(self):
        """TDS (70295) is a required parameter."""
        params = OhioEPAGeneralPermitScraper.get_monitoring_parameters()
        tds_params = [p for p in params if p["code"] == "70295"]
        assert len(tds_params) == 1
        assert "TDS" in tds_params[0]["name"]

    def test_chlorine_parameter_present(self):
        """Total Residual Chlorine (50060) is a required parameter."""
        params = OhioEPAGeneralPermitScraper.get_monitoring_parameters()
        cl_params = [p for p in params if p["code"] == "50060"]
        assert len(cl_params) == 1

    def test_all_parameters_have_required_fields(self):
        """Each parameter has code, name, unit, and frequency."""
        for param in DRAFT_PERMIT_PARAMETERS:
            assert "code" in param
            assert "name" in param
            assert "unit" in param
            assert "frequency" in param
            assert len(param["code"]) == 5  # EPA parameter codes are 5 digits


# --- Tests for permit summary formatting ---


class TestFormatPermitSummary:
    def test_basic_summary(self):
        status_info = {
            "permit_id": "OHD000001",
            "status": "draft",
            "description": "General NPDES Permit for Data Center Wastewater Discharges",
            "parameters": DRAFT_PERMIT_PARAMETERS,
        }
        result = OhioEPAGeneralPermitScraper.format_permit_summary(status_info)
        assert "OHD000001" in result
        assert "draft" in result
        assert "Flow" in result
        assert "pH" in result
        assert "TDS" in result

    def test_summary_without_parameters(self):
        status_info = {
            "permit_id": "OHD000001",
            "status": "pending",
            "description": "Test",
        }
        result = OhioEPAGeneralPermitScraper.format_permit_summary(status_info)
        assert "OHD000001" in result
        assert "pending" in result

    def test_summary_with_defaults(self):
        result = OhioEPAGeneralPermitScraper.format_permit_summary({})
        assert "OHD000001" in result  # Default permit_id
        assert "unknown" in result  # Default status


# --- Tests for permit status labels ---


class TestPermitStatusLabels:
    def test_draft_label(self):
        assert "Draft" in PERMIT_STATUS_LABELS["draft"]

    def test_final_label(self):
        assert "Final" in PERMIT_STATUS_LABELS["final"]

    def test_all_statuses_have_labels(self):
        for status in ["draft", "final", "expired", "pending"]:
            assert status in PERMIT_STATUS_LABELS


# --- Scraper properties ---


class TestScraperProperties:
    def test_name(self, scraper):
        assert scraper.name == "oh_epa_general_permit"

    def test_source(self, scraper):
        from models.document import DocumentSource
        assert scraper.source == DocumentSource.OH_EPA_GENERAL_PERMIT


# --- Discovery tests ---


class TestDiscovery:
    @pytest.mark.asyncio
    async def test_discover_yields_status_check(self, config):
        """Discovery should always yield at least a status check record."""
        scraper = OhioEPAGeneralPermitScraper(
            config=config,
            state_manager=FakeStateManager(),
            file_store=FakeFileStore(),
            browser=None,
        )

        # Override _check_permit_status to avoid network calls
        async def fake_status(client):
            return {
                "permit_id": "OHD000001",
                "status": "draft",
                "found_on_page": True,
                "checked_at": "2026-03-15T00:00:00",
            }

        scraper._check_permit_status = fake_status

        # Override _check_noi_list to return empty
        async def fake_noi(client):
            return []

        scraper._check_noi_list = fake_noi

        records = []
        async for meta in scraper.discover(limit=10):
            records.append(meta)

        # Should get: status_check, draft_permit, fact_sheet = 3 records
        assert len(records) == 3
        assert records[0]["document_type"] == "status_check"
        assert "OHD000001" in records[0]["title"]
        assert "draft" in records[0]["title"]

    @pytest.mark.asyncio
    async def test_discover_includes_draft_pdf(self, config):
        """Discovery should yield draft permit PDF metadata."""
        scraper = OhioEPAGeneralPermitScraper(
            config=config,
            state_manager=FakeStateManager(),
            file_store=FakeFileStore(),
            browser=None,
        )

        async def fake_status(client):
            return {"permit_id": "OHD000001", "status": "draft"}

        scraper._check_permit_status = fake_status

        async def fake_noi(client):
            return []

        scraper._check_noi_list = fake_noi

        records = []
        async for meta in scraper.discover(limit=10):
            records.append(meta)

        draft_records = [r for r in records if r.get("document_type") == "draft_permit"]
        assert len(draft_records) == 1
        assert "OHD000001_Draft.pdf" in draft_records[0]["filename"]

    @pytest.mark.asyncio
    async def test_discover_respects_limit(self, config):
        """Limit of 1 should only yield the status check."""
        scraper = OhioEPAGeneralPermitScraper(
            config=config,
            state_manager=FakeStateManager(),
            file_store=FakeFileStore(),
            browser=None,
        )

        async def fake_status(client):
            return {"permit_id": "OHD000001", "status": "draft"}

        scraper._check_permit_status = fake_status

        async def fake_noi(client):
            return []

        scraper._check_noi_list = fake_noi

        records = []
        async for meta in scraper.discover(limit=1):
            records.append(meta)

        assert len(records) == 1
        assert records[0]["document_type"] == "status_check"


class TestRunPipeline:
    @pytest.mark.asyncio
    async def test_run_processes_status_and_pdfs(self, config):
        """Run should process all discovered documents."""
        scraper = OhioEPAGeneralPermitScraper(
            config=config,
            state_manager=FakeStateManager(),
            file_store=FakeFileStore(),
            browser=None,
        )

        # Override discover with fake data
        async def fake_discover(limit=None):
            count = 0
            yield {
                "url": "https://www.epa.state.oh.us/dsw/permits/gplist",
                "title": "Ohio EPA General Permit OHD000001 — Status: draft",
                "date": None,
                "state": "OH",
                "agency": "Ohio EPA",
                "id": "oh-gp-ohd000001-status-202603",
                "match_term": "Ohio EPA data center general permit OHD000001",
                "matched_company": None,
                "document_type": "status_check",
            }
            count += 1
            if limit and count >= limit:
                return

        scraper.discover = fake_discover
        results = await scraper.run(limit=10)
        assert len(results) == 1
        rec = results[0]
        assert rec.state == "OH"
        assert rec.municipality_agency == "Ohio EPA"
        assert rec.source_portal.value == "oh_epa_general_permit"

    @pytest.mark.asyncio
    async def test_run_skips_already_fetched(self, config):
        """Already-tracked status checks are skipped."""
        state_mgr = FakeStateManager()
        # BaseScraper.run() uses meta.get("id") or meta.get("url") as doc_id
        await state_mgr.mark_fetched(
            "oh_epa_general_permit",
            "oh-gp-ohd000001-status-202603",
        )

        scraper = OhioEPAGeneralPermitScraper(
            config=config,
            state_manager=state_mgr,
            file_store=FakeFileStore(),
            browser=None,
        )

        async def fake_discover(limit=None):
            yield {
                "url": "https://www.epa.state.oh.us/dsw/permits/gplist",
                "title": "Ohio EPA General Permit OHD000001 — Status: draft",
                "date": None,
                "state": "OH",
                "agency": "Ohio EPA",
                "id": "oh-gp-ohd000001-status-202603",
                "match_term": "test",
                "matched_company": None,
                "document_type": "status_check",
            }

        scraper.discover = fake_discover
        results = await scraper.run(limit=10)
        assert len(results) == 0


# --- Data validation tests ---


class TestDataValidation:
    def test_parameter_codes_are_valid_epa_format(self):
        """EPA parameter codes should be exactly 5 digits."""
        for param in DRAFT_PERMIT_PARAMETERS:
            assert param["code"].isdigit(), f"Code {param['code']} is not numeric"
            assert len(param["code"]) == 5, f"Code {param['code']} is not 5 digits"

    def test_parameter_units_are_non_empty(self):
        """All parameters must have a unit."""
        for param in DRAFT_PERMIT_PARAMETERS:
            assert len(param["unit"]) > 0, f"Parameter {param['name']} has empty unit"

    def test_parameter_frequencies_are_valid(self):
        """Frequencies should be recognized monitoring intervals."""
        valid_frequencies = {"Monthly", "Quarterly", "Annual", "Daily", "Weekly"}
        for param in DRAFT_PERMIT_PARAMETERS:
            assert param["frequency"] in valid_frequencies, (
                f"Parameter {param['name']} has unexpected frequency: {param['frequency']}"
            )
