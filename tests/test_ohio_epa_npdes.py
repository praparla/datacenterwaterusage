"""Tests for the Ohio EPA NPDES ArcGIS scraper.

Tests cover:
- _feature_to_meta() parses GeoJSON features correctly
- SIC 7374 features get correct match_term
- County-based features get correct match_term
- Missing permit_id returns None from _feature_to_meta
- _summarize_permit() produces readable metric strings
- _parse_epoch_ms() handles timestamps and None
- run() skips already-fetched records
- run() populates record fields correctly
"""

from __future__ import annotations

import pytest

from scrapers.ohio.epa_npdes_arcgis import OhioEPANPDESArcGISScraper


# --- Shared fixtures ---

class FakeStateManager:
    def __init__(self):
        self._fetched = set()

    async def initialize(self):
        pass

    async def is_fetched(self, scraper_name, doc_id):
        return (scraper_name, doc_id) in self._fetched

    async def mark_fetched(self, scraper_name, doc_id, local_path=None):
        self._fetched.add((scraper_name, doc_id))

    async def mark_processed(self, scraper_name, doc_id):
        pass


class FakeFileStore:
    pass


@pytest.fixture
def config():
    return {
        "min_delay": 0.0,
        "max_delay": 0.0,
        "downloads_dir": "/tmp",
        "oh_epa_npdes_arcgis_page": "https://data-oepa.opendata.arcgis.com/datasets/npdes-individual-permits",
    }


@pytest.fixture
def scraper(config):
    return OhioEPANPDESArcGISScraper(
        config=config,
        state_manager=FakeStateManager(),
        file_store=FakeFileStore(),
        browser=None,
    )


# --- Sample GeoJSON features ---

SAMPLE_DC_FEATURE = {
    "properties": {
        "PERMIT_ID": "OHC000012345",
        "FACILITY_NAME": "AmazonCloud Data Center LLC",
        "COUNTY_NAME": "Licking",
        "SIC_CODE": "7374",
        "PERMIT_STATUS": "Active",
        "ISSUE_DATE": 1609459200000,  # 2021-01-01 in epoch ms
        "EXPIRY_DATE": 1735689600000,  # ~2025-01-01
    },
}

SAMPLE_COUNTY_FEATURE = {
    "properties": {
        "PERMIT_ID": "OHC000099999",
        "FACILITY_NAME": "Franklin County WWTP",
        "COUNTY_NAME": "Franklin",
        "SIC_CODE": "4952",
        "PERMIT_STATUS": "Active",
        "ISSUE_DATE": None,
        "EXPIRY_DATE": None,
    },
}

SAMPLE_NO_PERMIT = {
    "properties": {
        "FACILITY_NAME": "Missing Permit ID Facility",
        "COUNTY_NAME": "Franklin",
    },
}


# --- Unit tests: _feature_to_meta ---

class TestFeatureToMeta:
    def test_dc_sic_feature(self, scraper):
        meta = scraper._feature_to_meta(SAMPLE_DC_FEATURE)
        assert meta is not None
        assert meta["permit_number"] == "OHC000012345"
        assert meta["facility_name"] == "AmazonCloud Data Center LLC"
        assert meta["county"] == "Licking"
        assert meta["sic_code"] == "7374"
        assert meta["state"] == "OH"
        assert meta["id"] == "oh-npdes-OHC000012345"
        assert "SIC 7374" in meta["match_term"]

    def test_county_feature(self, scraper):
        meta = scraper._feature_to_meta(SAMPLE_COUNTY_FEATURE)
        assert meta is not None
        assert meta["permit_number"] == "OHC000099999"
        assert "Franklin" in meta["match_term"]

    def test_missing_permit_returns_none(self, scraper):
        meta = scraper._feature_to_meta(SAMPLE_NO_PERMIT)
        assert meta is None

    def test_empty_feature_returns_none(self, scraper):
        assert scraper._feature_to_meta({}) is None

    def test_date_parsed_from_epoch_ms(self, scraper):
        meta = scraper._feature_to_meta(SAMPLE_DC_FEATURE)
        assert meta["date"] is not None
        assert meta["date"].year == 2021

    def test_none_date_handled(self, scraper):
        meta = scraper._feature_to_meta(SAMPLE_COUNTY_FEATURE)
        assert meta["date"] is None


# --- Unit tests: _parse_epoch_ms ---

class TestParseEpochMs:
    def test_valid_epoch(self, scraper):
        dt = scraper._parse_epoch_ms(1609459200000)
        assert dt is not None
        assert dt.year == 2021

    def test_none(self, scraper):
        assert scraper._parse_epoch_ms(None) is None

    def test_invalid_string(self, scraper):
        assert scraper._parse_epoch_ms("not-a-number") is None

    def test_zero(self, scraper):
        dt = scraper._parse_epoch_ms(0)
        assert dt is not None
        assert dt.year == 1970


# --- Unit tests: _summarize_permit ---

class TestSummarizePermit:
    def test_dc_sic_summary(self, scraper):
        meta = scraper._feature_to_meta(SAMPLE_DC_FEATURE)
        summary = scraper._summarize_permit(meta)
        assert "SIC 7374" in summary
        assert "Active" in summary
        assert "Licking" in summary

    def test_county_summary_no_sic(self, scraper):
        meta = scraper._feature_to_meta(SAMPLE_COUNTY_FEATURE)
        summary = scraper._summarize_permit(meta)
        assert "Active" in summary
        assert "Franklin" in summary


# --- Scraper properties ---

class TestScraperProperties:
    def test_name(self, scraper):
        assert scraper.name == "oh_epa_npdes_arcgis"

    def test_source(self, scraper):
        from models.document import DocumentSource
        assert scraper.source == DocumentSource.OH_EPA_NPDES_ARCGIS

    def test_fetch_document_returns_none(self, scraper):
        import asyncio
        result = asyncio.get_event_loop().run_until_complete(
            scraper.fetch_document({"url": "https://example.com"})
        )
        assert result is None


# --- Integration-style run tests ---

class TestRunPipeline:
    @pytest.mark.asyncio
    async def test_run_skips_already_fetched(self, config):
        state_mgr = FakeStateManager()
        await state_mgr.mark_fetched("oh_epa_npdes_arcgis", "oh-npdes-OHC000012345")

        scraper = OhioEPANPDESArcGISScraper(
            config=config,
            state_manager=state_mgr,
            file_store=FakeFileStore(),
            browser=None,
        )

        async def fake_discover(limit=None):
            yield scraper._feature_to_meta(SAMPLE_DC_FEATURE)

        scraper.discover = fake_discover
        results = await scraper.run()
        assert len(results) == 0

    @pytest.mark.asyncio
    async def test_run_processes_dc_permit(self, config):
        scraper = OhioEPANPDESArcGISScraper(
            config=config,
            state_manager=FakeStateManager(),
            file_store=FakeFileStore(),
            browser=None,
        )

        async def fake_discover(limit=None):
            yield scraper._feature_to_meta(SAMPLE_DC_FEATURE)

        scraper.discover = fake_discover
        results = await scraper.run()
        assert len(results) == 1

        rec = results[0]
        assert rec.state == "OH"
        assert rec.municipality_agency == "Ohio EPA"
        assert rec.permit_number == "OHC000012345"
        assert rec.company_llc_name == "AmazonCloud Data Center LLC"
        assert rec.source_portal.value == "oh_epa_npdes_arcgis"
        assert "SIC 7374" in rec.extracted_water_metric
        assert "AmazonCloud" in rec.extracted_quote
