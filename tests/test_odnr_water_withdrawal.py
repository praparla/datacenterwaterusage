"""Tests for the ODNR Water Withdrawal scraper.

Tests cover:
- _feature_to_meta() parses ArcGIS attributes correctly
- Annual volume is converted from gallons to MGD average
- Missing facility ID returns None
- _build_quote() includes key fields
- run() skips already-fetched records
- run() populates record fields (volume, county, facility)
- Scraper properties (name, source)
"""

from __future__ import annotations

import pytest

from scrapers.ohio.odnr_water_withdrawal import ODNRWaterWithdrawalScraper


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
        "oh_odnr_water_withdrawal_viewer": (
            "https://experience.arcgis.com/experience/0605c2eaf8fe458481ac323404b4ab36"
        ),
        "oh_odnr_water_withdrawal_service": "https://services.arcgis.com/fake/FeatureServer/0/query",
        "oh_odnr_target_counties": ["Franklin", "Licking"],
    }


@pytest.fixture
def scraper(config):
    return ODNRWaterWithdrawalScraper(
        config=config,
        state_manager=FakeStateManager(),
        file_store=FakeFileStore(),
        browser=None,
    )


# --- Sample ArcGIS features ---

SAMPLE_FEATURE_FULL = {
    "attributes": {
        "FACILITY_ID": "OH-WW-12345",
        "FACILITY_NAME": "City of Columbus Public Water System",
        "COUNTY": "Franklin",
        "WATER_SOURCE_TYPE": "Surface Water",
        "ANNUAL_WITHDRAWAL_VOLUME": 36500000000,  # 36.5B gallons/yr ≈ 100 MGD
        "REPORT_YEAR": 2023,
    },
}

SAMPLE_FEATURE_MINIMAL = {
    "attributes": {
        "FACILITY_ID": "OH-WW-99999",
        "FACILITY_NAME": "Small County Well",
        "COUNTY": "Licking",
        "WATER_SOURCE_TYPE": "Groundwater",
        "ANNUAL_WITHDRAWAL_VOLUME": None,
        "REPORT_YEAR": None,
    },
}

SAMPLE_FEATURE_NO_ID = {
    "attributes": {
        "FACILITY_NAME": "Mystery Facility",
        "COUNTY": "Franklin",
    },
}


# --- Unit tests: _feature_to_meta ---

class TestFeatureToMeta:
    def test_full_feature_parsed(self, scraper):
        viewer_url = config()["oh_odnr_water_withdrawal_viewer"] if False else (
            "https://experience.arcgis.com/experience/0605c2eaf8fe458481ac323404b4ab36"
        )
        meta = scraper._feature_to_meta(SAMPLE_FEATURE_FULL, viewer_url)
        assert meta is not None
        assert meta["id"] == "odnr-withdrawal-OH-WW-12345"
        assert meta["facility_name"] == "City of Columbus Public Water System"
        assert meta["county"] == "Franklin"
        assert meta["source_type"] == "Surface Water"
        assert meta["annual_volume"] == 36500000000
        assert meta["report_year"] == 2023
        assert meta["state"] == "OH"
        assert meta["agency"] == "Ohio ODNR"

    def test_volume_converted_to_mgd(self, scraper):
        viewer_url = "https://experience.arcgis.com/test"
        meta = scraper._feature_to_meta(SAMPLE_FEATURE_FULL, viewer_url)
        # 36.5B gal / 365 days / 1M = 100 MGD
        assert "100.00 MGD" in meta["volume_str"]
        assert "36,500,000,000 gal/yr" in meta["volume_str"]

    def test_none_volume_handled(self, scraper):
        viewer_url = "https://experience.arcgis.com/test"
        meta = scraper._feature_to_meta(SAMPLE_FEATURE_MINIMAL, viewer_url)
        assert meta is not None
        assert meta["volume_str"] is None

    def test_missing_facility_id_returns_none(self, scraper):
        viewer_url = "https://experience.arcgis.com/test"
        meta = scraper._feature_to_meta(SAMPLE_FEATURE_NO_ID, viewer_url)
        assert meta is None

    def test_empty_feature_returns_none(self, scraper):
        assert scraper._feature_to_meta({}, "https://example.com") is None

    def test_match_term_includes_county(self, scraper):
        viewer_url = "https://experience.arcgis.com/test"
        meta = scraper._feature_to_meta(SAMPLE_FEATURE_FULL, viewer_url)
        assert "Franklin" in meta["match_term"]


# --- Unit tests: _build_quote ---

class TestBuildQuote:
    def test_full_meta_quote(self, scraper):
        viewer_url = "https://experience.arcgis.com/test"
        meta = scraper._feature_to_meta(SAMPLE_FEATURE_FULL, viewer_url)
        quote = scraper._build_quote(meta)
        assert "City of Columbus Public Water System" in quote
        assert "Franklin" in quote
        assert "Surface Water" in quote
        assert "100.00 MGD" in quote
        assert "2023" in quote

    def test_minimal_meta_quote(self, scraper):
        viewer_url = "https://experience.arcgis.com/test"
        meta = scraper._feature_to_meta(SAMPLE_FEATURE_MINIMAL, viewer_url)
        quote = scraper._build_quote(meta)
        assert "Small County Well" in quote
        assert "Licking" in quote


# --- Scraper properties ---

class TestScraperProperties:
    def test_name(self, scraper):
        assert scraper.name == "oh_odnr_water_withdrawal"

    def test_source(self, scraper):
        from models.document import DocumentSource
        assert scraper.source == DocumentSource.OH_ODNR_WATER_WITHDRAWAL

    def test_fetch_document_returns_none(self, scraper):
        import asyncio
        result = asyncio.get_event_loop().run_until_complete(
            scraper.fetch_document({})
        )
        assert result is None


# --- Integration-style run tests ---

class TestRunPipeline:
    @pytest.mark.asyncio
    async def test_run_skips_already_fetched(self, config):
        state_mgr = FakeStateManager()
        await state_mgr.mark_fetched("oh_odnr_water_withdrawal", "odnr-withdrawal-OH-WW-12345")

        scraper = ODNRWaterWithdrawalScraper(
            config=config,
            state_manager=state_mgr,
            file_store=FakeFileStore(),
            browser=None,
        )

        viewer_url = config["oh_odnr_water_withdrawal_viewer"]

        async def fake_discover(limit=None):
            yield scraper._feature_to_meta(SAMPLE_FEATURE_FULL, viewer_url)

        scraper.discover = fake_discover
        results = await scraper.run()
        assert len(results) == 0

    @pytest.mark.asyncio
    async def test_run_processes_facility(self, config):
        scraper = ODNRWaterWithdrawalScraper(
            config=config,
            state_manager=FakeStateManager(),
            file_store=FakeFileStore(),
            browser=None,
        )
        viewer_url = config["oh_odnr_water_withdrawal_viewer"]

        async def fake_discover(limit=None):
            yield scraper._feature_to_meta(SAMPLE_FEATURE_FULL, viewer_url)

        scraper.discover = fake_discover
        results = await scraper.run()
        assert len(results) == 1

        rec = results[0]
        assert rec.state == "OH"
        assert rec.municipality_agency == "Ohio ODNR"
        assert rec.company_llc_name == "City of Columbus Public Water System"
        assert rec.source_portal.value == "oh_odnr_water_withdrawal"
        assert "100.00 MGD" in rec.extracted_water_metric
        assert "Franklin" in rec.extracted_quote
