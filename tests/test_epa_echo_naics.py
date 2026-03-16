"""Tests for the EPA ECHO NAICS facility discovery scraper.

Tests cover:
- Facility metric formatting
- Facility quote building
- Known company matching
- Scraper properties
- Discovery pipeline with fake data
- State-based deduplication
"""

from __future__ import annotations

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
import pytest_asyncio

from scrapers.epa_echo_naics import EPAEchoNAICSScraper


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
        "naics_data_center": "518210",
        "target_states": ["VA", "OH"],
        "known_companies": [
            "Amazon", "AWS", "Microsoft", "Google", "Meta",
            "QTS", "Equinix", "Digital Realty",
        ],
    }


@pytest.fixture
def scraper(config):
    return EPAEchoNAICSScraper(
        config=config,
        state_manager=FakeStateManager(),
        file_store=FakeFileStore(),
        browser=None,
    )


# --- Sample facility data ---


SAMPLE_FACILITY = {
    "FacName": "AMAZON DATA SERVICES INC",
    "FacStreet": "123 Cloud Drive",
    "FacCity": "Ashburn",
    "FacState": "VA",
    "FacZip": "20147",
    "RegistryID": "110123456789",
    "CWPPermitStatusDesc": "Effective",
    "CWPComplianceStatus": "No Violation",
    "FacLat": "39.0438",
    "FacLong": "-77.4874",
    "NAICSCodes": "518210",
    "SourceID": "VAR052461",
}

SAMPLE_FACILITY_NO_PERMIT = {
    "FacName": "SMALL DATA CO",
    "FacCity": "Columbus",
    "FacState": "OH",
    "RegistryID": "110987654321",
    "NAICSCodes": "518210",
}


# --- Unit tests for _format_facility_metric ---


class TestFormatFacilityMetric:
    def test_full_facility_data(self, scraper):
        result = scraper._format_facility_metric(SAMPLE_FACILITY)
        assert result is not None
        assert "AMAZON DATA SERVICES INC" in result
        assert "Effective" in result
        assert "No Violation" in result
        assert "518210" in result

    def test_minimal_facility_data(self, scraper):
        result = scraper._format_facility_metric(SAMPLE_FACILITY_NO_PERMIT)
        assert result is not None
        assert "SMALL DATA CO" in result

    def test_empty_facility(self, scraper):
        result = scraper._format_facility_metric({})
        assert result is None


# --- Unit tests for _build_facility_quote ---


class TestBuildFacilityQuote:
    def test_full_quote(self, scraper):
        result = scraper._build_facility_quote(SAMPLE_FACILITY)
        assert result is not None
        assert "AMAZON DATA SERVICES INC" in result
        assert "Ashburn" in result
        assert "VA" in result
        assert "110123456789" in result
        assert "39.0438" in result

    def test_quote_truncated(self, scraper):
        # Build a facility with very long values to test truncation
        big_fac = {"FacName": "A" * 600}
        result = scraper._build_facility_quote(big_fac)
        assert len(result) <= 500

    def test_empty_quote(self, scraper):
        result = scraper._build_facility_quote({})
        assert result is None


# --- Unit tests for _match_known_company ---


class TestMatchKnownCompany:
    def test_amazon_match(self, scraper):
        result = scraper._match_known_company("AMAZON DATA SERVICES INC")
        assert result == "Amazon"

    def test_aws_match(self, scraper):
        result = scraper._match_known_company("AWS US EAST LLC")
        assert result == "AWS"

    def test_google_match(self, scraper):
        result = scraper._match_known_company("GOOGLE LLC")
        assert result == "Google"

    def test_no_match(self, scraper):
        result = scraper._match_known_company("RANDOM COMPANY INC")
        assert result is None

    def test_empty_name(self, scraper):
        result = scraper._match_known_company("")
        assert result is None

    def test_none_name(self, scraper):
        result = scraper._match_known_company(None)
        assert result is None


# --- Scraper properties ---


class TestScraperProperties:
    def test_name(self, scraper):
        assert scraper.name == "epa_echo_naics"

    def test_source(self, scraper):
        from models.document import DocumentSource
        assert scraper.source == DocumentSource.EPA_ECHO_NAICS

    def test_fetch_document_returns_none(self, scraper):
        import asyncio
        result = asyncio.get_event_loop().run_until_complete(
            scraper.fetch_document({"url": "https://example.com"})
        )
        assert result is None


# --- Integration-style tests ---


class TestRunPipeline:
    @pytest.mark.asyncio
    async def test_run_processes_facilities(self, config):
        """New facilities are processed and returned as records."""
        scraper = EPAEchoNAICSScraper(
            config=config,
            state_manager=FakeStateManager(),
            file_store=FakeFileStore(),
            browser=None,
        )

        async def fake_discover(limit=None):
            yield {
                "url": "https://echo.epa.gov/detailed-facility-report?fid=110123456789",
                "title": "ECHO Facility: AMAZON DATA SERVICES INC (VA) — NAICS 518210",
                "date": None,
                "state": "VA",
                "agency": "EPA ECHO",
                "id": "echo-naics-VA-110123456789",
                "match_term": "NAICS 518210 facility search",
                "matched_company": "Amazon",
                "facility_data": SAMPLE_FACILITY,
                "registry_id": "110123456789",
                "document_url": "https://echo.epa.gov/detailed-facility-report?fid=110123456789",
            }

        scraper.discover = fake_discover
        results = await scraper.run(limit=10)
        assert len(results) == 1

        rec = results[0]
        assert rec.state == "VA"
        assert rec.municipality_agency == "EPA ECHO"
        assert rec.company_llc_name == "AMAZON DATA SERVICES INC"
        assert rec.matched_company == "Amazon"
        assert rec.source_portal.value == "epa_echo_naics"
        assert "518210" in (rec.extracted_water_metric or "")
        assert rec.extracted_quote is not None

    @pytest.mark.asyncio
    async def test_run_skips_already_fetched(self, config):
        """Already-fetched facilities are skipped."""
        state_mgr = FakeStateManager()
        await state_mgr.mark_fetched("epa_echo_naics", "echo-naics-VA-110123456789")

        scraper = EPAEchoNAICSScraper(
            config=config,
            state_manager=state_mgr,
            file_store=FakeFileStore(),
            browser=None,
        )

        async def fake_discover(limit=None):
            yield {
                "url": "https://echo.epa.gov/detailed-facility-report?fid=110123456789",
                "title": "Test",
                "date": None,
                "state": "VA",
                "agency": "EPA ECHO",
                "id": "echo-naics-VA-110123456789",
                "match_term": "test",
                "matched_company": None,
                "facility_data": SAMPLE_FACILITY,
                "registry_id": "110123456789",
                "document_url": "https://echo.epa.gov/...",
            }

        scraper.discover = fake_discover
        results = await scraper.run(limit=10)
        assert len(results) == 0

    @pytest.mark.asyncio
    async def test_run_respects_limit(self, config):
        """Limit parameter caps the number of records."""
        scraper = EPAEchoNAICSScraper(
            config=config,
            state_manager=FakeStateManager(),
            file_store=FakeFileStore(),
            browser=None,
        )

        async def fake_discover(limit=None):
            for i in range(10):
                if limit and i >= limit:
                    return
                yield {
                    "url": f"https://echo.epa.gov/fid={i}",
                    "title": f"Facility {i}",
                    "date": None,
                    "state": "VA",
                    "agency": "EPA ECHO",
                    "id": f"echo-naics-VA-{i}",
                    "match_term": "test",
                    "matched_company": None,
                    "facility_data": {"FacName": f"Facility {i}"},
                    "registry_id": str(i),
                    "document_url": "",
                }

        scraper.discover = fake_discover
        results = await scraper.run(limit=3)
        assert len(results) == 3

    @pytest.mark.asyncio
    async def test_empty_states_config(self):
        """No target states produces no results."""
        config = {
            "min_delay": 0.0,
            "max_delay": 0.0,
            "downloads_dir": "/tmp",
            "naics_data_center": "518210",
            "target_states": [],
        }
        scraper = EPAEchoNAICSScraper(
            config=config,
            state_manager=FakeStateManager(),
            file_store=FakeFileStore(),
            browser=None,
        )
        results = await scraper.run()
        assert len(results) == 0
