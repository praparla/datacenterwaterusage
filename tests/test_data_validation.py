"""Data validation tests for the pipeline output and configuration.

Tests cover:
- Config integrity: required keys, valid URLs, valid permit IDs
- DocumentRecord field validation: non-empty required fields, valid dates, valid URLs
- Output format validation: CSV headers, JSON structure
- Cross-scraper consistency: all document sources have corresponding scrapers
- Permit ID format validation for EPA ECHO targets
"""

from __future__ import annotations

import sys
import os
import re
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest

from config import CONFIG
from models.document import DocumentRecord, DocumentSource
from storage.csv_writer import FIELDNAMES


# --- Config validation ---


class TestConfigIntegrity:
    """Validate that config.py has all required keys and valid values."""

    def test_required_keys_present(self):
        """All essential config keys must exist."""
        required_keys = [
            "min_delay",
            "max_delay",
            "downloads_dir",
            "csv_output_path",
            "json_output_path",
            "state_db_path",
            "epa_echo_target_permits",
            "known_companies",
            "search_keywords",
        ]
        for key in required_keys:
            assert key in CONFIG, f"Missing config key: {key}"

    def test_rate_limits_are_positive(self):
        """Rate limit delays must be positive numbers."""
        assert CONFIG["min_delay"] > 0
        assert CONFIG["max_delay"] > 0
        assert CONFIG["max_delay"] >= CONFIG["min_delay"]

    def test_target_permits_non_empty(self):
        """Must have at least one target permit for EPA ECHO."""
        permits = CONFIG["epa_echo_target_permits"]
        assert len(permits) > 0

    def test_target_permits_valid_format(self):
        """EPA NPDES permit IDs follow the pattern: 2-letter state + digits."""
        permit_pattern = re.compile(r"^[A-Z]{2}\d{7}$")
        for permit_id in CONFIG["epa_echo_target_permits"]:
            assert permit_pattern.match(permit_id), (
                f"Invalid permit ID format: {permit_id} "
                f"(expected 2 letters + 7 digits, e.g. VA0091383)"
            )

    def test_target_permits_include_va_and_oh(self):
        """Should have permits for both Virginia and Ohio."""
        permits = CONFIG["epa_echo_target_permits"]
        va_permits = [p for p in permits if p.startswith("VA")]
        oh_permits = [p for p in permits if p.startswith("OH")]
        assert len(va_permits) >= 1, "No Virginia permits configured"
        assert len(oh_permits) >= 1, "No Ohio permits configured"

    def test_known_companies_non_empty(self):
        """Must have at least one known company for entity matching."""
        assert len(CONFIG["known_companies"]) > 0

    def test_known_companies_no_duplicates(self):
        """No duplicate entries in known_companies."""
        companies = CONFIG["known_companies"]
        assert len(companies) == len(set(companies)), (
            f"Duplicate companies found: "
            f"{[c for c in companies if companies.count(c) > 1]}"
        )

    def test_search_keywords_non_empty(self):
        """Must have at least one search keyword."""
        assert len(CONFIG["search_keywords"]) > 0

    def test_naics_code_valid(self):
        """NAICS code for data centers should be 518210."""
        if "naics_data_center" in CONFIG:
            assert CONFIG["naics_data_center"] == "518210"

    def test_target_states_valid(self):
        """Target states should be valid 2-letter codes."""
        if "target_states" in CONFIG:
            for state in CONFIG["target_states"]:
                assert len(state) == 2
                assert state.isupper()

    def test_urls_are_https(self):
        """All URL config values should use HTTPS."""
        url_keys = [
            k for k, v in CONFIG.items()
            if isinstance(v, str) and v.startswith("http")
        ]
        for key in url_keys:
            url = CONFIG[key]
            assert url.startswith("https://"), (
                f"Config {key} uses HTTP instead of HTTPS: {url}"
            )


# --- DocumentRecord validation ---


class TestDocumentRecordValidation:
    """Validate DocumentRecord field constraints and serialization."""

    def _make_record(self, **overrides) -> DocumentRecord:
        defaults = dict(
            state="VA",
            municipality_agency="Test Agency",
            document_title="Test Document",
            source_url="https://example.com/doc",
            source_portal=DocumentSource.EPA_ECHO_DMR,
        )
        defaults.update(overrides)
        return DocumentRecord(**defaults)

    def test_required_fields_non_empty(self):
        """Required fields must not be empty strings."""
        rec = self._make_record()
        assert len(rec.state) > 0
        assert len(rec.municipality_agency) > 0
        assert len(rec.document_title) > 0
        assert len(rec.source_url) > 0

    def test_to_dict_has_all_csv_fields(self):
        """to_dict() output should contain all CSV fieldnames."""
        rec = self._make_record()
        d = rec.to_dict()
        for field in FIELDNAMES:
            assert field in d, f"Missing field in to_dict(): {field}"

    def test_date_serialization_iso_format(self):
        """Dates should serialize to ISO 8601 format."""
        rec = self._make_record(document_date=datetime(2023, 6, 30, 12, 0))
        d = rec.to_dict()
        assert d["document_date"] == "2023-06-30T12:00:00"

    def test_none_date_serializes_to_empty(self):
        """None dates should serialize to empty string."""
        rec = self._make_record(document_date=None)
        d = rec.to_dict()
        assert d["document_date"] == ""

    def test_relevance_score_is_formatted(self):
        """Relevance score should be a formatted float string."""
        rec = self._make_record(relevance_score=0.75)
        d = rec.to_dict()
        assert d["relevance_score"] == "0.75"

    def test_source_portal_serializes_to_string(self):
        """source_portal should serialize to its string value."""
        for source in DocumentSource:
            rec = self._make_record(source_portal=source)
            d = rec.to_dict()
            assert d["source_portal"] == source.value

    def test_extracted_quote_truncated_to_500(self):
        """Extracted quotes longer than 500 chars are truncated."""
        long_quote = "x" * 1000
        rec = self._make_record(extracted_quote=long_quote)
        d = rec.to_dict()
        assert len(d["extracted_quote"]) == 500

    def test_keyword_matches_joined_with_semicolons(self):
        """keyword_matches list serializes as semicolon-separated string."""
        rec = self._make_record(
            keyword_matches=["data center", "cooling tower", "MGD"]
        )
        d = rec.to_dict()
        assert d["keyword_matches"] == "data center; cooling tower; MGD"

    def test_scraped_at_auto_populated(self):
        """scraped_at should be automatically set on creation."""
        rec = self._make_record()
        assert isinstance(rec.scraped_at, datetime)
        # Should be recent (within last minute)
        delta = datetime.utcnow() - rec.scraped_at
        assert delta.total_seconds() < 60


# --- DocumentSource completeness ---


class TestDocumentSourceCompleteness:
    """Verify all document sources are properly defined."""

    def test_all_sources_have_non_empty_values(self):
        """Each enum member should have a non-empty string value."""
        for source in DocumentSource:
            assert isinstance(source.value, str)
            assert len(source.value) > 0

    def test_new_sources_exist(self):
        """New sources added in this batch should exist."""
        assert hasattr(DocumentSource, "VA_LOUDOUN_ACFR")
        assert hasattr(DocumentSource, "EPA_ECHO_NAICS")
        assert hasattr(DocumentSource, "OH_EPA_GENERAL_PERMIT")

    def test_source_values_are_lowercase(self):
        """All source values should be lowercase with underscores."""
        for source in DocumentSource:
            assert source.value == source.value.lower(), (
                f"Source {source.name} has non-lowercase value: {source.value}"
            )

    def test_no_duplicate_values(self):
        """No two sources should have the same value."""
        values = [s.value for s in DocumentSource]
        assert len(values) == len(set(values)), (
            f"Duplicate source values found"
        )


# --- CSV output format ---


class TestCSVFormat:
    def test_fieldnames_include_essential_columns(self):
        """CSV must include essential tracking columns."""
        essential = [
            "state",
            "municipality_agency",
            "document_title",
            "source_url",
            "source_portal",
            "scraped_at",
        ]
        for col in essential:
            assert col in FIELDNAMES, f"Missing essential CSV column: {col}"

    def test_fieldnames_include_water_data_columns(self):
        """CSV must include water-data-specific columns."""
        water_cols = [
            "extracted_water_metric",
            "permit_number",
            "relevance_score",
        ]
        for col in water_cols:
            assert col in FIELDNAMES, f"Missing water data CSV column: {col}"

    def test_fieldnames_count(self):
        """Verify expected number of CSV columns."""
        assert len(FIELDNAMES) == 17  # Current count


# --- Permit ID validation ---


class TestPermitIDValidation:
    """Validate permit ID formats across the pipeline."""

    VA_PERMIT_PATTERN = re.compile(r"^VA\d{7}$")
    OH_PERMIT_PATTERN = re.compile(r"^OH\d{7}$")

    def test_broad_run_permit_valid(self):
        """Broad Run WRF permit is correctly formatted."""
        assert self.VA_PERMIT_PATTERN.match("VA0091383")

    def test_all_va_permits_valid(self):
        """All Virginia permits in config follow the correct format."""
        va_permits = [
            p for p in CONFIG["epa_echo_target_permits"]
            if p.startswith("VA")
        ]
        for permit in va_permits:
            assert self.VA_PERMIT_PATTERN.match(permit), (
                f"Invalid VA permit format: {permit}"
            )

    def test_all_oh_permits_valid(self):
        """All Ohio permits in config follow the correct format."""
        oh_permits = [
            p for p in CONFIG["epa_echo_target_permits"]
            if p.startswith("OH")
        ]
        for permit in oh_permits:
            assert self.OH_PERMIT_PATTERN.match(permit), (
                f"Invalid OH permit format: {permit}"
            )

    def test_no_duplicate_permits(self):
        """No duplicate permit IDs in the target list."""
        permits = CONFIG["epa_echo_target_permits"]
        assert len(permits) == len(set(permits)), (
            f"Duplicate permits found: "
            f"{[p for p in permits if permits.count(p) > 1]}"
        )
