"""Tests for the EPA ECHO DMR scraper.

Tests cover:
- Metric formatting from DMR records
- Quote building from DMR data
- Date parsing for various EPA date formats
- Discovery yields correct metadata structure
- Facility info parsing
- Run method skips already-fetched records
"""

from __future__ import annotations

import pytest
import pytest_asyncio

from scrapers.epa_echo_dmr import EPAEchoDMRScraper


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
        "epa_echo_target_permits": ["VA0091383", "OH0024651"],
    }


@pytest.fixture
def scraper(config):
    return EPAEchoDMRScraper(
        config=config,
        state_manager=FakeStateManager(),
        file_store=FakeFileStore(),
        browser=None,
    )


# --- Unit tests for _format_metric ---


class TestFormatMetric:
    def test_format_metric_with_values(self, scraper):
        dmr = {
            "parameter_desc": "Flow, in conduit or thru treatment plant",
            "parameter_code": "50050",
            "dmr_value_nmbr": "12.5",
            "standard_unit_desc": "MGD",
        }
        result = scraper._format_metric(dmr)
        assert result is not None
        assert "12.5" in result
        assert "MGD" in result
        assert "Flow" in result

    def test_format_metric_with_camel_case_keys(self, scraper):
        dmr = {
            "ParameterDesc": "Total Suspended Solids",
            "ParameterCode": "00530",
            "QuantityAvg": "45.0",
            "StandardUnitDesc": "mg/L",
        }
        result = scraper._format_metric(dmr)
        assert result is not None
        assert "45.0" in result
        assert "Total Suspended Solids" in result

    def test_format_metric_with_multiple_values(self, scraper):
        dmr = {
            "parameter_desc": "Flow",
            "quantity_avg": "10.0",
            "quantity_max": "15.0",
            "standard_unit_desc": "MGD",
        }
        result = scraper._format_metric(dmr)
        assert "10.0" in result
        assert "15.0" in result

    def test_format_metric_empty_record(self, scraper):
        dmr = {"parameter_desc": "Flow"}
        result = scraper._format_metric(dmr)
        assert result is None

    def test_format_metric_none_values_skipped(self, scraper):
        dmr = {
            "parameter_desc": "Flow",
            "dmr_value_nmbr": None,
            "quantity_avg": "5.0",
            "standard_unit_desc": "MGD",
        }
        result = scraper._format_metric(dmr)
        assert result is not None
        assert "5.0" in result


# --- Unit tests for _build_quote ---


class TestBuildQuote:
    def test_build_quote_full_data(self, scraper):
        dmr = {
            "monitoring_period_end_date": "03/31/2025",
            "parameter_desc": "Flow, in conduit or thru treatment plant",
            "perm_feature_nmbr": "001",
            "limit_value_nmbr": "16.5",
            "limit_value_type_code": "ENF",
        }
        result = scraper._build_quote(dmr, "Broad Run WRF")
        assert "Broad Run WRF" in result
        assert "03/31/2025" in result
        assert "Flow" in result
        assert "001" in result
        assert "16.5" in result

    def test_build_quote_partial_data(self, scraper):
        dmr = {"parameter_desc": "Flow"}
        result = scraper._build_quote(dmr, "Test Facility")
        assert "Test Facility" in result
        assert "Flow" in result

    def test_build_quote_truncated_at_500(self, scraper):
        dmr = {
            "parameter_desc": "A" * 600,
        }
        result = scraper._build_quote(dmr, "Test")
        assert len(result) <= 500


# --- Unit tests for _parse_date ---


class TestParseDate:
    def test_parse_mm_dd_yyyy(self, scraper):
        result = scraper._parse_date("03/31/2025")
        assert result is not None
        assert result.year == 2025
        assert result.month == 3
        assert result.day == 31

    def test_parse_iso_date(self, scraper):
        result = scraper._parse_date("2025-03-31")
        assert result is not None
        assert result.year == 2025

    def test_parse_iso_datetime(self, scraper):
        result = scraper._parse_date("2025-03-31T00:00:00")
        assert result is not None
        assert result.year == 2025

    def test_parse_none(self, scraper):
        assert scraper._parse_date(None) is None

    def test_parse_invalid(self, scraper):
        assert scraper._parse_date("not-a-date") is None

    def test_parse_empty_string(self, scraper):
        assert scraper._parse_date("") is None


# --- Tests for scraper properties ---


class TestScraperProperties:
    def test_name(self, scraper):
        assert scraper.name == "epa_echo_dmr"

    def test_source(self, scraper):
        from models.document import DocumentSource
        assert scraper.source == DocumentSource.EPA_ECHO_DMR

    def test_fetch_document_returns_none(self, scraper):
        import asyncio
        result = asyncio.get_event_loop().run_until_complete(
            scraper.fetch_document({"url": "https://example.com"})
        )
        assert result is None


# --- Integration-style tests with mocked HTTP ---


class TestDiscovery:
    """Tests that verify the discover/run pipeline logic with fake data."""

    @pytest.mark.asyncio
    async def test_run_skips_already_fetched(self, config):
        """Records already in state DB are skipped."""
        state_mgr = FakeStateManager()
        # Pre-mark a record as fetched
        await state_mgr.mark_fetched(
            "epa_echo_dmr",
            "echo-VA0091383-001-50050-03/31/2025",
        )

        scraper = EPAEchoDMRScraper(
            config=config,
            state_manager=state_mgr,
            file_store=FakeFileStore(),
            browser=None,
        )

        # Override discover to yield one record that should be skipped
        original_discover = scraper.discover

        async def fake_discover(limit=None):
            yield {
                "url": "https://echo.epa.gov/detailed-facility-report?fid=VA0091383",
                "title": "DMR: Broad Run — Flow (Outfall 001, 03/31/2025)",
                "date": None,
                "state": "VA",
                "agency": "EPA ECHO",
                "permit_number": "VA0091383",
                "facility_name": "Broad Run WRF",
                "id": "echo-VA0091383-001-50050-03/31/2025",
                "match_term": "EPA ECHO target permit: VA0091383",
                "matched_company": None,
                "dmr_data": {},
                "water_metric": "Flow: 12.5 MGD",
                "document_url": "https://echo.epa.gov/...",
            }

        scraper.discover = fake_discover
        results = await scraper.run(limit=10)
        assert len(results) == 0  # Should be skipped

    @pytest.mark.asyncio
    async def test_run_processes_new_records(self, config):
        """New records are processed and returned."""
        scraper = EPAEchoDMRScraper(
            config=config,
            state_manager=FakeStateManager(),
            file_store=FakeFileStore(),
            browser=None,
        )

        async def fake_discover(limit=None):
            yield {
                "url": "https://echo.epa.gov/detailed-facility-report?fid=VA0091383",
                "title": "DMR: Broad Run WRF — Flow (Outfall 001, 03/31/2025)",
                "date": None,
                "state": "VA",
                "agency": "EPA ECHO",
                "permit_number": "VA0091383",
                "facility_name": "Broad Run WRF",
                "id": "echo-VA0091383-001-50050-03/31/2025",
                "match_term": "EPA ECHO target permit: VA0091383",
                "matched_company": None,
                "dmr_data": {
                    "parameter_desc": "Flow",
                    "monitoring_period_end_date": "03/31/2025",
                    "perm_feature_nmbr": "001",
                },
                "water_metric": "Flow: Dmr Value Nmbr: 12.5 MGD",
                "document_url": "https://echo.epa.gov/...",
            }

        scraper.discover = fake_discover
        results = await scraper.run(limit=10)
        assert len(results) == 1

        rec = results[0]
        assert rec.state == "VA"
        assert rec.municipality_agency == "EPA ECHO"
        assert rec.permit_number == "VA0091383"
        assert rec.company_llc_name == "Broad Run WRF"
        assert rec.extracted_water_metric == "Flow: Dmr Value Nmbr: 12.5 MGD"
        assert rec.source_portal.value == "epa_echo_dmr"
        assert "Broad Run WRF" in rec.extracted_quote

    @pytest.mark.asyncio
    async def test_run_respects_limit(self, config):
        """Limit parameter caps the number of records."""
        scraper = EPAEchoDMRScraper(
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
                    "url": f"https://echo.epa.gov/fid=VA0091383",
                    "title": f"DMR Record {i}",
                    "date": None,
                    "state": "VA",
                    "agency": "EPA ECHO",
                    "permit_number": "VA0091383",
                    "facility_name": "Test",
                    "id": f"echo-test-{i}",
                    "match_term": "test",
                    "matched_company": None,
                    "dmr_data": {},
                    "water_metric": None,
                    "document_url": "",
                }

        scraper.discover = fake_discover
        results = await scraper.run(limit=3)
        assert len(results) == 3

    @pytest.mark.asyncio
    async def test_empty_target_permits(self):
        """No target permits produces no results."""
        config = {
            "min_delay": 0.0,
            "max_delay": 0.0,
            "downloads_dir": "/tmp",
            "epa_echo_target_permits": [],
        }
        scraper = EPAEchoDMRScraper(
            config=config,
            state_manager=FakeStateManager(),
            file_store=FakeFileStore(),
            browser=None,
        )
        results = await scraper.run()
        assert len(results) == 0
