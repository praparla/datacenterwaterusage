"""Tests for the Prince William Water Industrial User Survey scraper.

Tests cover:
- ERU value extraction from text
- GPD value extraction
- MGD value extraction
- Data center count extraction
- Percent demand extraction
- ERU to GPD/monthly conversion
- Discovery pipeline with fake data
- Skip-already-fetched deduplication
"""

from __future__ import annotations

import pytest

from scrapers.virginia.pwc_ius import PWCIUSScraper


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
    }


@pytest.fixture
def scraper(config):
    return PWCIUSScraper(
        config=config,
        state_manager=FakeStateManager(),
        file_store=FakeFileStore(),
        browser=None,
    )


# --- ERU extraction tests ---


class TestExtractERU:
    def test_simple_eru(self):
        text = "The facility has been allocated 150 ERU for cooling water."
        results = PWCIUSScraper.extract_eru_values(text)
        assert len(results) == 1
        assert results[0]["value"] == 150

    def test_comma_separated_eru(self):
        text = "Data center capacity: 1,500 ERU allocated."
        results = PWCIUSScraper.extract_eru_values(text)
        assert len(results) == 1
        assert results[0]["value"] == 1500

    def test_multiple_eru_values(self):
        text = "Phase 1: 200 ERU; Phase 2: 350 ERU; Total: 550 ERU."
        results = PWCIUSScraper.extract_eru_values(text)
        assert len(results) == 3
        values = [r["value"] for r in results]
        assert 200 in values
        assert 350 in values
        assert 550 in values

    def test_no_eru(self):
        text = "No ERU allocations found in this document."
        # "ERU" appears but not preceded by a number
        results = PWCIUSScraper.extract_eru_values(text)
        assert len(results) == 0

    def test_eru_context_captured(self):
        text = "AWS data center has been allocated 500 ERU for cooling."
        results = PWCIUSScraper.extract_eru_values(text)
        assert len(results) == 1
        assert "AWS" in results[0]["context"]
        assert "cooling" in results[0]["context"]


# --- GPD extraction tests ---


class TestExtractGPD:
    def test_simple_gpd(self):
        text = "Average daily flow: 200,000 GPD"
        results = PWCIUSScraper.extract_gpd_values(text)
        assert len(results) == 1
        assert results[0]["value"] == 200000
        assert results[0]["unit"] == "GPD"

    def test_gallons_per_day_spelled_out(self):
        text = "The facility uses approximately 50,000 gallons per day."
        results = PWCIUSScraper.extract_gpd_values(text)
        assert len(results) == 1
        assert results[0]["value"] == 50000

    def test_no_gpd(self):
        text = "Total water usage is measured in other units."
        results = PWCIUSScraper.extract_gpd_values(text)
        assert len(results) == 0

    def test_decimal_gpd(self):
        text = "Discharge: 1,234.5 GPD"
        results = PWCIUSScraper.extract_gpd_values(text)
        assert len(results) == 1
        assert results[0]["value"] == 1234.5


# --- MGD extraction tests ---


class TestExtractMGD:
    def test_simple_mgd(self):
        text = "Plant capacity: 12.5 MGD"
        results = PWCIUSScraper.extract_mgd_values(text)
        assert len(results) == 1
        assert results[0]["value"] == 12.5
        assert results[0]["unit"] == "MGD"

    def test_integer_mgd(self):
        text = "Designed for 3 MGD throughput."
        results = PWCIUSScraper.extract_mgd_values(text)
        assert len(results) == 1
        assert results[0]["value"] == 3.0

    def test_no_mgd(self):
        text = "No million gallons per day data available."
        results = PWCIUSScraper.extract_mgd_values(text)
        assert len(results) == 0


# --- Data center count extraction ---


class TestExtractDataCenterCount:
    def test_count_found(self):
        text = "Prince William County hosts 56 data centers."
        result = PWCIUSScraper.extract_data_center_count(text)
        assert result == 56

    def test_count_not_found(self):
        text = "Several facilities operate in the area."
        result = PWCIUSScraper.extract_data_center_count(text)
        assert result is None

    def test_count_singular(self):
        text = "Amazon operates 1 data center in the district."
        result = PWCIUSScraper.extract_data_center_count(text)
        assert result == 1


# --- Percent demand extraction ---


class TestExtractPercentDemand:
    def test_average_demand(self):
        text = "Data centers consumed 2.7% of average daily demand."
        results = PWCIUSScraper.extract_percent_demand(text)
        assert len(results) == 1
        assert results[0]["percent"] == 2.7
        assert results[0]["demand_type"] == "average"

    def test_max_demand(self):
        text = "Peak: 5.3% of max daily demand in summer."
        results = PWCIUSScraper.extract_percent_demand(text)
        assert len(results) == 1
        assert results[0]["percent"] == 5.3
        assert results[0]["demand_type"] == "max"

    def test_multiple_percents(self):
        text = "2.7% of average demand and 5.3% of max demand."
        results = PWCIUSScraper.extract_percent_demand(text)
        assert len(results) == 2

    def test_no_percent(self):
        text = "Data center water usage is growing rapidly."
        results = PWCIUSScraper.extract_percent_demand(text)
        assert len(results) == 0


# --- Unit conversions ---


class TestERUConversions:
    def test_eru_to_gpd(self):
        assert PWCIUSScraper.eru_to_gpd(1) == 400
        assert PWCIUSScraper.eru_to_gpd(100) == 40_000
        assert PWCIUSScraper.eru_to_gpd(1500) == 600_000

    def test_eru_to_monthly(self):
        assert PWCIUSScraper.eru_to_monthly_gallons(1) == 10_000
        assert PWCIUSScraper.eru_to_monthly_gallons(100) == 1_000_000
        assert PWCIUSScraper.eru_to_monthly_gallons(56) == 560_000


# --- Scraper properties ---


class TestScraperProperties:
    def test_name(self, scraper):
        assert scraper.name == "va_pwc_ius"

    def test_source(self, scraper):
        from models.document import DocumentSource
        assert scraper.source == DocumentSource.VA_PWC_IUS


# --- Pipeline tests ---


class TestRunPipeline:
    @pytest.mark.asyncio
    async def test_run_processes_ius_report(self, config):
        """IUS reports are processed and returned as records."""
        scraper = PWCIUSScraper(
            config=config,
            state_manager=FakeStateManager(),
            file_store=FakeFileStore(),
            browser=None,
        )

        async def fake_discover(limit=None):
            yield {
                "url": "https://princewilliamwater.org/sites/default/files/IUS_March%202024.pdf",
                "document_url": "https://princewilliamwater.org/sites/default/files/IUS_March%202024.pdf",
                "title": "Prince William Water Industrial User Survey — March 2024",
                "date": None,
                "state": "VA",
                "agency": "Prince William Water",
                "id": "pwc-ius-2024",
                "year": "2024",
                "match_term": "Prince William Water IUS",
                "matched_company": None,
                "filename": "pwc_ius_2024.pdf",
            }

        scraper.discover = fake_discover
        results = await scraper.run(limit=10)
        assert len(results) == 1

        rec = results[0]
        assert rec.state == "VA"
        assert rec.municipality_agency == "Prince William Water"
        assert rec.source_portal.value == "va_pwc_ius"

    @pytest.mark.asyncio
    async def test_run_skips_already_fetched(self, config):
        """Already-fetched IUS reports are skipped."""
        state_mgr = FakeStateManager()
        await state_mgr.mark_fetched("va_pwc_ius", "pwc-ius-2024")

        scraper = PWCIUSScraper(
            config=config,
            state_manager=state_mgr,
            file_store=FakeFileStore(),
            browser=None,
        )

        async def fake_discover(limit=None):
            yield {
                "url": "https://princewilliamwater.org/sites/default/files/IUS_March%202024.pdf",
                "document_url": "https://princewilliamwater.org/...",
                "title": "IUS 2024",
                "date": None,
                "state": "VA",
                "agency": "Prince William Water",
                "id": "pwc-ius-2024",
                "year": "2024",
                "match_term": "test",
                "matched_company": None,
                "filename": "pwc_ius_2024.pdf",
            }

        scraper.discover = fake_discover
        results = await scraper.run(limit=10)
        assert len(results) == 0

    @pytest.mark.asyncio
    async def test_run_respects_limit(self, config):
        """Limit parameter caps the number of records."""
        scraper = PWCIUSScraper(
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
                    "url": f"https://example.com/ius-{i}.pdf",
                    "document_url": f"https://example.com/ius-{i}.pdf",
                    "title": f"IUS {i}",
                    "date": None,
                    "state": "VA",
                    "agency": "Prince William Water",
                    "id": f"pwc-ius-{i}",
                    "match_term": "test",
                    "matched_company": None,
                    "filename": f"ius_{i}.pdf",
                }

        scraper.discover = fake_discover
        results = await scraper.run(limit=3)
        assert len(results) == 3
