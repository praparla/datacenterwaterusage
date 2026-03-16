"""Tests for the Virginia DEQ VWP (Water Withdrawal Permits) scraper.

Tests cover:
- Withdrawal metric formatting
- VWP quote building
- Date parsing (epoch ms and string formats)
- County WHERE clause building
- GPD to MGD conversion
- NoVA counties mapping
- Discovery pipeline with fake data
- Skip-already-fetched deduplication
"""

from __future__ import annotations

from datetime import datetime

import pytest

from scrapers.virginia.deq_vwp import DEQVWPScraper, VWP_LAYERS, NOVA_COUNTIES


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
        "known_companies": [
            "Amazon", "AWS", "Microsoft", "Google", "Meta",
            "Loudoun Water", "Fairfax Water",
        ],
    }


@pytest.fixture
def scraper(config):
    return DEQVWPScraper(
        config=config,
        state_manager=FakeStateManager(),
        file_store=FakeFileStore(),
        browser=None,
    )


# --- Sample data ---


SAMPLE_ATTRS_FULL = {
    "VWP_PMT_NO": "VWP-2023-0042",
    "PERMITTEE": "LOUDOUN WATER",
    "FAC_NAME": "Broad Run Withdrawal",
    "COUNTY": "Loudoun",
    "FIPS": "107",
    "SOURCE_NAME": "Broad Run",
    "SOURCE_TYPE": "Surface Water",
    "MAX_WITHDRAW_GPD": 5000000,
    "AVG_WITHDRAW_GPD": 3200000,
    "PERMIT_STATUS": "Active",
    "ISSUE_DATE": "01/15/2023",
    "EXPIRE_DATE": "01/15/2033",
    "LATITUDE": 39.0438,
    "LONGITUDE": -77.4874,
}

SAMPLE_ATTRS_MINIMAL = {
    "VWP_PMT_NO": "VWP-2022-0100",
    "PERMITTEE": "VADATA INC",
    "COUNTY": "Prince William",
}

SAMPLE_ATTRS_GPD_SMALL = {
    "VWP_PMT_NO": "VWP-2021-0055",
    "FAC_NAME": "Test Well",
    "MAX_WITHDRAW_GPD": 50000,
    "AVG_WITHDRAW_GPD": 30000,
    "SOURCE_TYPE": "Groundwater",
}


# --- Withdrawal metric formatting ---


class TestFormatWithdrawalMetric:
    def test_full_data_mgd(self, scraper):
        """Large GPD values display as MGD."""
        result = scraper._format_withdrawal_metric(SAMPLE_ATTRS_FULL)
        assert result is not None
        assert "5.00 MGD" in result
        assert "3.20 MGD" in result
        assert "Surface Water" in result
        assert "Broad Run" in result

    def test_small_gpd_values(self, scraper):
        """Small GPD values stay in GPD units."""
        result = scraper._format_withdrawal_metric(SAMPLE_ATTRS_GPD_SMALL)
        assert result is not None
        assert "50,000 GPD" in result
        assert "30,000 GPD" in result
        assert "Groundwater" in result

    def test_minimal_data(self, scraper):
        """Minimal attrs without GPD return None."""
        result = scraper._format_withdrawal_metric(SAMPLE_ATTRS_MINIMAL)
        assert result is None

    def test_empty_attrs(self, scraper):
        result = scraper._format_withdrawal_metric({})
        assert result is None


# --- VWP quote building ---


class TestBuildVWPQuote:
    def test_full_quote(self, scraper):
        result = scraper._build_vwp_quote(SAMPLE_ATTRS_FULL)
        assert result is not None
        assert "VWP-2023-0042" in result
        assert "LOUDOUN WATER" in result
        assert "Loudoun" in result
        assert "Surface Water" in result

    def test_minimal_quote(self, scraper):
        result = scraper._build_vwp_quote(SAMPLE_ATTRS_MINIMAL)
        assert result is not None
        assert "VADATA INC" in result

    def test_empty_quote(self, scraper):
        result = scraper._build_vwp_quote({})
        assert result is None

    def test_quote_truncated_at_500(self, scraper):
        long_attrs = {"PERMITTEE": "A" * 600}
        result = scraper._build_vwp_quote(long_attrs)
        assert len(result) <= 500


# --- Date parsing ---


class TestParseDate:
    def test_epoch_ms(self, scraper):
        # 2023-01-15T00:00:00Z in milliseconds
        epoch_ms = 1673740800000
        result = scraper._parse_date(epoch_ms)
        assert result is not None
        assert result.year == 2023
        assert result.month == 1

    def test_string_mm_dd_yyyy(self, scraper):
        result = scraper._parse_date("01/15/2023")
        assert result is not None
        assert result.year == 2023
        assert result.month == 1
        assert result.day == 15

    def test_string_iso(self, scraper):
        result = scraper._parse_date("2023-01-15")
        assert result is not None
        assert result.year == 2023

    def test_string_iso_datetime(self, scraper):
        result = scraper._parse_date("2023-01-15T00:00:00")
        assert result is not None
        assert result.year == 2023

    def test_none(self, scraper):
        assert scraper._parse_date(None) is None

    def test_invalid_string(self, scraper):
        assert scraper._parse_date("not-a-date") is None


# --- County WHERE clause ---


class TestBuildCountyWhere:
    def test_single_county(self, scraper):
        result = scraper._build_county_where(["107"])
        assert "107" in result
        assert "Loudoun" in result

    def test_multiple_counties(self, scraper):
        result = scraper._build_county_where(["107", "059", "153"])
        assert "107" in result
        assert "059" in result
        assert "153" in result
        assert "Loudoun" in result
        assert "Fairfax" in result
        assert "Prince William" in result

    def test_empty_counties(self, scraper):
        result = scraper._build_county_where([])
        assert result == "1=1"


# --- Static methods ---


class TestStaticMethods:
    def test_gpd_to_mgd(self):
        assert DEQVWPScraper.gpd_to_mgd(1_000_000) == 1.0
        assert DEQVWPScraper.gpd_to_mgd(5_000_000) == 5.0
        assert DEQVWPScraper.gpd_to_mgd(500_000) == 0.5

    def test_nova_counties(self):
        counties = DEQVWPScraper.get_nova_counties()
        assert "107" in counties
        assert counties["107"] == "Loudoun"
        assert "059" in counties
        assert counties["059"] == "Fairfax"
        assert len(counties) == 5


# --- VWP layers ---


class TestVWPLayers:
    def test_layers_defined(self):
        assert "individual" in VWP_LAYERS
        assert "general" in VWP_LAYERS

    def test_layer_ids(self):
        assert VWP_LAYERS["individual"]["id"] == 192
        assert VWP_LAYERS["general"]["id"] == 193

    def test_layer_names_non_empty(self):
        for key, layer in VWP_LAYERS.items():
            assert len(layer["name"]) > 0
            assert len(layer["description"]) > 0


# --- Scraper properties ---


class TestScraperProperties:
    def test_name(self, scraper):
        assert scraper.name == "va_deq_vwp"

    def test_source(self, scraper):
        from models.document import DocumentSource
        assert scraper.source == DocumentSource.VA_DEQ_VWP

    def test_fetch_document_returns_none(self, scraper):
        import asyncio
        result = asyncio.get_event_loop().run_until_complete(
            scraper.fetch_document({"url": "https://example.com"})
        )
        assert result is None


# --- Pipeline tests ---


class TestRunPipeline:
    @pytest.mark.asyncio
    async def test_run_processes_permits(self, config):
        """VWP permits are processed and returned as records."""
        scraper = DEQVWPScraper(
            config=config,
            state_manager=FakeStateManager(),
            file_store=FakeFileStore(),
            browser=None,
        )

        async def fake_discover(limit=None):
            yield {
                "url": "https://apps.deq.virginia.gov/arcgis/rest/services/public/EDMA/MapServer/192/query?where=VWP_PMT_NO='VWP-2023-0042'&f=html",
                "title": "VWP Individual Permits: Broad Run Withdrawal — VWP-2023-0042",
                "date": None,
                "state": "VA",
                "agency": "Virginia DEQ",
                "permit_number": "VWP-2023-0042",
                "id": "vwp-individual-VWP-2023-0042",
                "match_term": "VWP individual permit — county Loudoun",
                "matched_company": "Loudoun Water",
                "attributes": SAMPLE_ATTRS_FULL,
                "layer": "individual",
            }

        scraper.discover = fake_discover
        results = await scraper.run(limit=10)
        assert len(results) == 1

        rec = results[0]
        assert rec.state == "VA"
        assert rec.municipality_agency == "Virginia DEQ"
        assert rec.company_llc_name == "LOUDOUN WATER"
        assert rec.matched_company == "Loudoun Water"
        assert rec.source_portal.value == "va_deq_vwp"
        assert "5.00 MGD" in (rec.extracted_water_metric or "")
        assert rec.extracted_quote is not None
        assert "VWP-2023-0042" in rec.extracted_quote

    @pytest.mark.asyncio
    async def test_run_skips_already_fetched(self, config):
        """Already-fetched permits are skipped."""
        state_mgr = FakeStateManager()
        await state_mgr.mark_fetched(
            "va_deq_vwp", "vwp-individual-VWP-2023-0042"
        )

        scraper = DEQVWPScraper(
            config=config,
            state_manager=state_mgr,
            file_store=FakeFileStore(),
            browser=None,
        )

        async def fake_discover(limit=None):
            yield {
                "url": "https://example.com",
                "title": "Test VWP",
                "date": None,
                "state": "VA",
                "agency": "Virginia DEQ",
                "permit_number": "VWP-2023-0042",
                "id": "vwp-individual-VWP-2023-0042",
                "match_term": "test",
                "matched_company": None,
                "attributes": SAMPLE_ATTRS_FULL,
                "layer": "individual",
            }

        scraper.discover = fake_discover
        results = await scraper.run(limit=10)
        assert len(results) == 0

    @pytest.mark.asyncio
    async def test_run_respects_limit(self, config):
        """Limit parameter caps the number of records."""
        scraper = DEQVWPScraper(
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
                    "url": f"https://example.com/vwp-{i}",
                    "title": f"VWP Permit {i}",
                    "date": None,
                    "state": "VA",
                    "agency": "Virginia DEQ",
                    "permit_number": f"VWP-{i}",
                    "id": f"vwp-individual-{i}",
                    "match_term": "test",
                    "matched_company": None,
                    "attributes": {"PERMITTEE": f"Facility {i}"},
                    "layer": "individual",
                }

        scraper.discover = fake_discover
        results = await scraper.run(limit=3)
        assert len(results) == 3


# --- NOVA counties data validation ---


class TestNOVACounties:
    def test_loudoun_in_nova(self):
        assert "107" in NOVA_COUNTIES
        assert NOVA_COUNTIES["107"] == "Loudoun"

    def test_fairfax_in_nova(self):
        assert "059" in NOVA_COUNTIES
        assert NOVA_COUNTIES["059"] == "Fairfax"

    def test_prince_william_in_nova(self):
        assert "153" in NOVA_COUNTIES
        assert NOVA_COUNTIES["153"] == "Prince William"

    def test_all_counties_have_names(self):
        for fips, name in NOVA_COUNTIES.items():
            assert len(fips) == 3, f"FIPS {fips} should be 3 digits"
            assert len(name) > 0, f"County name for FIPS {fips} should not be empty"
