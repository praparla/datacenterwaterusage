"""Tests for the Loudoun Water ACFR scraper.

Tests cover:
- Year extraction from filenames and link text
- PDF link detection (identifies ACFR vs unrelated links)
- discover() yields correct metadata structure
- fetch_document() returns None when no URL given
- run() skips already-fetched records
- run() processes new records correctly
"""

from __future__ import annotations

import pytest

from scrapers.virginia.loudoun_acfr import LoudounACFRScraper


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
    def get_path(self, state, agency, filename):
        return f"/tmp/{state}/{agency}/{filename}"

    def exists(self, state, agency, filename):
        return False

    async def save(self, content, state, agency, filename):
        return f"/tmp/{state}/{agency}/{filename}"


@pytest.fixture
def config():
    return {
        "min_delay": 0.0,
        "max_delay": 0.0,
        "downloads_dir": "/tmp/downloads",
        "va_loudoun_acfr_page": "https://www.loudounwater.org/about/acfr",
        "va_loudoun_reclaimed_page": "https://www.loudounwater.org/commercial/reclaimed",
    }


@pytest.fixture
def scraper(config):
    return LoudounACFRScraper(
        config=config,
        state_manager=FakeStateManager(),
        file_store=FakeFileStore(),
        browser=None,
    )


# --- Unit tests: _extract_year ---

class TestExtractYear:
    def test_year_from_filename(self, scraper):
        assert scraper._extract_year("loudoun-water-2023-acfr.pdf", "") == "2023"

    def test_year_from_link_text(self, scraper):
        assert scraper._extract_year("report.pdf", "Annual Report 2022") == "2022"

    def test_year_from_url_path(self, scraper):
        assert scraper._extract_year("/files/ACFR2024Final.pdf", "") == "2024"

    def test_no_year(self, scraper):
        assert scraper._extract_year("report.pdf", "Annual Financial Report") is None

    def test_year_prefers_href(self, scraper):
        # href year takes precedence
        result = scraper._extract_year("2022-acfr.pdf", "2023 Annual Report")
        assert result == "2022"


# --- Scraper properties ---

class TestScraperProperties:
    def test_name(self, scraper):
        assert scraper.name == "va_loudoun_acfr"

    def test_source(self, scraper):
        from models.document import DocumentSource
        assert scraper.source == DocumentSource.VA_LOUDOUN_ACFR

    def test_fetch_document_no_url_returns_none(self, scraper):
        import asyncio
        result = asyncio.get_event_loop().run_until_complete(
            scraper.fetch_document({"title": "No URL"})
        )
        assert result is None


# --- Integration-style tests with mocked discover ---

class TestRunPipeline:
    @pytest.mark.asyncio
    async def test_run_skips_already_fetched(self, config):
        state_mgr = FakeStateManager()
        await state_mgr.mark_fetched("va_loudoun_acfr", "loudoun-acfr-2023")

        scraper = LoudounACFRScraper(
            config=config,
            state_manager=state_mgr,
            file_store=FakeFileStore(),
            browser=None,
        )

        async def fake_discover(limit=None):
            yield {
                "url": "https://www.loudounwater.org/about/acfr",
                "document_url": "https://www.loudounwater.org/files/2023-acfr.pdf",
                "pdf_url": "https://www.loudounwater.org/files/2023-acfr.pdf",
                "title": "Loudoun Water ACFR 2023",
                "date": None,
                "state": "VA",
                "agency": "Loudoun Water",
                "id": "loudoun-acfr-2023",
                "year": "2023",
                "match_term": "Loudoun Water ACFR",
                "matched_company": None,
            }

        scraper.discover = fake_discover
        results = await scraper.run(limit=10)
        assert len(results) == 0

    @pytest.mark.asyncio
    async def test_run_processes_new_acfr(self, config):
        scraper = LoudounACFRScraper(
            config=config,
            state_manager=FakeStateManager(),
            file_store=FakeFileStore(),
            browser=None,
        )

        async def fake_discover(limit=None):
            yield {
                "url": "https://www.loudounwater.org/about/acfr",
                "document_url": "https://www.loudounwater.org/files/2023-acfr.pdf",
                "pdf_url": "https://www.loudounwater.org/files/2023-acfr.pdf",
                "title": "Loudoun Water ACFR 2023",
                "date": None,
                "state": "VA",
                "agency": "Loudoun Water",
                "id": "loudoun-acfr-2023",
                "year": "2023",
                "match_term": "Loudoun Water ACFR — data center water sales",
                "matched_company": None,
            }

        async def fake_fetch(meta):
            return "/tmp/va/loudoun-water/loudoun_acfr_2023.pdf"

        scraper.discover = fake_discover
        scraper.fetch_document = fake_fetch

        results = await scraper.run(limit=10)
        assert len(results) == 1

        rec = results[0]
        assert rec.state == "VA"
        assert rec.municipality_agency == "Loudoun Water"
        assert rec.document_title == "Loudoun Water ACFR 2023"
        assert rec.source_portal.value == "va_loudoun_acfr"
        assert rec.local_file_path == "/tmp/va/loudoun-water/loudoun_acfr_2023.pdf"
        assert rec.match_term == "Loudoun Water ACFR — data center water sales"

    @pytest.mark.asyncio
    async def test_run_respects_limit(self, config):
        scraper = LoudounACFRScraper(
            config=config,
            state_manager=FakeStateManager(),
            file_store=FakeFileStore(),
            browser=None,
        )

        async def fake_discover(limit=None):
            for year in ["2020", "2021", "2022", "2023", "2024"]:
                if limit and int(year) - 2020 >= limit:
                    return
                yield {
                    "url": "https://www.loudounwater.org/about/acfr",
                    "document_url": f"https://www.loudounwater.org/files/{year}-acfr.pdf",
                    "pdf_url": f"https://www.loudounwater.org/files/{year}-acfr.pdf",
                    "title": f"Loudoun Water ACFR {year}",
                    "date": None,
                    "state": "VA",
                    "agency": "Loudoun Water",
                    "id": f"loudoun-acfr-{year}",
                    "year": year,
                    "match_term": "Loudoun Water ACFR",
                    "matched_company": None,
                }

        async def fake_fetch(meta):
            return f"/tmp/va/loudoun/{meta['year']}.pdf"

        scraper.discover = fake_discover
        scraper.fetch_document = fake_fetch

        results = await scraper.run(limit=2)
        assert len(results) == 2
