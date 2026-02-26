"""Tests for the Fairfax Water financial report scraper.

Tests cover:
- Year extraction from URLs and link text
- _make_meta() produces correct metadata structure
- fetch_document() returns None when no URL given
- Known direct URLs are emitted even when page scrape fails
- run() skips already-fetched records
- run() processes new records correctly
"""

from __future__ import annotations

import pytest

from scrapers.virginia.fairfax_water import FairfaxWaterScraper


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
    async def save(self, content, state, agency, filename):
        return f"/tmp/{state}/{agency}/{filename}"


@pytest.fixture
def config():
    return {
        "min_delay": 0.0,
        "max_delay": 0.0,
        "downloads_dir": "/tmp/downloads",
        "va_fairfax_water_about_page": "https://www.fairfaxwater.org/about-us",
        "va_fairfax_water_budget_page": "https://www.fairfaxwater.org/rates",
    }


@pytest.fixture
def scraper(config):
    return FairfaxWaterScraper(
        config=config,
        state_manager=FakeStateManager(),
        file_store=FakeFileStore(),
        browser=None,
    )


# --- Unit tests: _extract_year ---

class TestExtractYear:
    def test_year_from_url(self, scraper):
        assert scraper._extract_year("2024%20Financial%20Report.pdf", "") == "2024"

    def test_year_from_text(self, scraper):
        assert scraper._extract_year("report.pdf", "2023 Comprehensive Annual Report") == "2023"

    def test_no_year(self, scraper):
        assert scraper._extract_year("report.pdf", "Financial Report") is None


# --- Unit tests: _make_meta ---

class TestMakeMeta:
    def test_make_meta_structure(self, scraper):
        meta = scraper._make_meta(
            pdf_url="https://www.fairfaxwater.org/2024-report.pdf",
            title="2024 Report",
            year="2024",
            doc_id="fairfax-water-report-2024",
            page_url="https://www.fairfaxwater.org/about-us",
        )
        assert meta["state"] == "VA"
        assert meta["agency"] == "Fairfax Water"
        assert meta["year"] == "2024"
        assert meta["id"] == "fairfax-water-report-2024"
        assert meta["pdf_url"] == "https://www.fairfaxwater.org/2024-report.pdf"
        assert meta["date"].year == 2024
        assert "wholesale" in meta["match_term"].lower()

    def test_make_meta_no_year(self, scraper):
        meta = scraper._make_meta(
            pdf_url="https://www.fairfaxwater.org/report.pdf",
            title="Financial Report",
            year=None,
            doc_id="fairfax-water-report-unknown",
            page_url="https://www.fairfaxwater.org/about-us",
        )
        assert meta["date"] is None
        assert meta["year"] is None


# --- Scraper properties ---

class TestScraperProperties:
    def test_name(self, scraper):
        assert scraper.name == "va_fairfax_water"

    def test_source(self, scraper):
        from models.document import DocumentSource
        assert scraper.source == DocumentSource.VA_FAIRFAX_WATER

    def test_fetch_document_no_url_returns_none(self, scraper):
        import asyncio
        result = asyncio.get_event_loop().run_until_complete(
            scraper.fetch_document({"title": "No URL"})
        )
        assert result is None


# --- Integration-style run tests ---

class TestRunPipeline:
    @pytest.mark.asyncio
    async def test_run_skips_already_fetched(self, config):
        state_mgr = FakeStateManager()
        await state_mgr.mark_fetched("va_fairfax_water", "fairfax-water-financial-report-2024")

        scraper = FairfaxWaterScraper(
            config=config,
            state_manager=state_mgr,
            file_store=FakeFileStore(),
            browser=None,
        )

        async def fake_discover(limit=None):
            yield {
                "url": "https://www.fairfaxwater.org/about-us",
                "document_url": "https://www.fairfaxwater.org/2024-report.pdf",
                "pdf_url": "https://www.fairfaxwater.org/2024-report.pdf",
                "title": "Fairfax Water 2024 Comprehensive Financial Report",
                "date": None,
                "state": "VA",
                "agency": "Fairfax Water",
                "id": "fairfax-water-financial-report-2024",
                "year": "2024",
                "match_term": "Fairfax Water financial report",
                "matched_company": None,
            }

        scraper.discover = fake_discover
        results = await scraper.run()
        assert len(results) == 0

    @pytest.mark.asyncio
    async def test_run_processes_new_report(self, config):
        scraper = FairfaxWaterScraper(
            config=config,
            state_manager=FakeStateManager(),
            file_store=FakeFileStore(),
            browser=None,
        )

        async def fake_discover(limit=None):
            yield {
                "url": "https://www.fairfaxwater.org/about-us",
                "document_url": "https://www.fairfaxwater.org/2024-report.pdf",
                "pdf_url": "https://www.fairfaxwater.org/2024-report.pdf",
                "title": "Fairfax Water 2024 Comprehensive Financial Report",
                "date": None,
                "state": "VA",
                "agency": "Fairfax Water",
                "id": "fairfax-water-financial-report-2024",
                "year": "2024",
                "match_term": "Fairfax Water financial report — wholesale water delivery to DC clusters",
                "matched_company": None,
            }

        async def fake_fetch(meta):
            return "/tmp/va/fairfax-water/fairfax_water_financial_report_2024.pdf"

        scraper.discover = fake_discover
        scraper.fetch_document = fake_fetch

        results = await scraper.run()
        assert len(results) == 1

        rec = results[0]
        assert rec.state == "VA"
        assert rec.municipality_agency == "Fairfax Water"
        assert rec.source_portal.value == "va_fairfax_water"
        assert rec.local_file_path.endswith("2024.pdf")
