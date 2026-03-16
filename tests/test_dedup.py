"""Tests for the deduplication engine (utils/dedup.py).

Tests cover:
- Title normalization and similarity
- Completeness scoring
- Exact URL dedup
- Permit + date dedup across sources
- Fuzzy title dedup within same permit
- Edge cases (empty df, single row, no permit)
- Source merging across portals
"""

from __future__ import annotations

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import pytest

from utils.dedup import (
    _normalize_title,
    title_similarity,
    _completeness_score,
    _pick_best_row,
    deduplicate,
)


# --- Title normalization ---


class TestNormalizeTitle:
    def test_lowercase(self):
        assert _normalize_title("VPDES Outfall Report") == "vpdes outfall report"

    def test_strips_punctuation(self):
        assert _normalize_title("Flow: 6.4 MGD!") == "flow 64 mgd"

    def test_collapses_whitespace(self):
        assert _normalize_title("  flow   data  ") == "flow data"

    def test_none_input(self):
        assert _normalize_title(None) == ""

    def test_non_string_input(self):
        assert _normalize_title(42) == ""


class TestTitleSimilarity:
    def test_identical_titles(self):
        assert title_similarity("VPDES Report 2024", "VPDES Report 2024") == 1.0

    def test_very_similar(self):
        sim = title_similarity(
            "VPDES Outfall: Amazon - VAR052461",
            "VPDES Outfall: Amazon - VAR052461 (Outfall 001)",
        )
        assert sim > 0.7

    def test_very_different(self):
        sim = title_similarity("Water usage report", "Zoning variance application")
        assert sim < 0.3

    def test_empty_strings(self):
        assert title_similarity("", "") == 0.0

    def test_one_empty(self):
        assert title_similarity("some title", "") == 0.0


# --- Completeness scoring ---


class TestCompletenessScore:
    def test_full_record(self):
        row = pd.Series(
            {
                "extracted_water_metric": "6.4 MGD",
                "extracted_quote": "Some quote",
                "permit_number": "VA0091383",
                "company_llc_name": "Broad Run WRF",
                "document_date": "2024-01-01",
                "document_url": "https://example.com",
                "local_file_path": "/data/file.pdf",
            }
        )
        assert _completeness_score(row) == 7

    def test_sparse_record(self):
        row = pd.Series(
            {
                "extracted_water_metric": "",
                "extracted_quote": "",
                "permit_number": "VA0091383",
                "company_llc_name": "",
                "document_date": "",
                "document_url": "",
                "local_file_path": "",
            }
        )
        assert _completeness_score(row) == 1  # only permit_number

    def test_empty_record(self):
        row = pd.Series(
            {
                "extracted_water_metric": "",
                "permit_number": "",
            }
        )
        assert _completeness_score(row) == 0


# --- Pick best row ---


class TestPickBestRow:
    def test_single_row_returns_itself(self):
        df = pd.DataFrame(
            [
                {
                    "source_portal": "epa_echo_dmr",
                    "permit_number": "VA0091383",
                    "scraped_at": "2026-01-01",
                }
            ]
        )
        best = _pick_best_row(df)
        assert best["permit_number"] == "VA0091383"
        assert best["sources"] == "epa_echo_dmr"

    def test_picks_most_complete(self):
        df = pd.DataFrame(
            [
                {
                    "source_portal": "va_deq_arcgis",
                    "permit_number": "VA0091383",
                    "extracted_water_metric": "",
                    "company_llc_name": "",
                    "scraped_at": "2026-03-01",
                },
                {
                    "source_portal": "epa_echo_dmr",
                    "permit_number": "VA0091383",
                    "extracted_water_metric": "6.4 MGD",
                    "company_llc_name": "Broad Run WRF",
                    "scraped_at": "2026-02-01",
                },
            ]
        )
        best = _pick_best_row(df)
        assert best["extracted_water_metric"] == "6.4 MGD"
        assert "epa_echo_dmr" in best["sources"]
        assert "va_deq_arcgis" in best["sources"]

    def test_merges_fields_from_others(self):
        df = pd.DataFrame(
            [
                {
                    "source_portal": "epa_echo_dmr",
                    "permit_number": "VA0091383",
                    "extracted_water_metric": "6.4 MGD",
                    "document_url": "",
                    "scraped_at": "2026-02-01",
                },
                {
                    "source_portal": "va_deq_arcgis",
                    "permit_number": "VA0091383",
                    "extracted_water_metric": "",
                    "document_url": "https://deq.virginia.gov/report.pdf",
                    "scraped_at": "2026-01-01",
                },
            ]
        )
        best = _pick_best_row(df)
        # Best row had the metric; missing URL filled from other
        assert best["extracted_water_metric"] == "6.4 MGD"
        assert best["document_url"] == "https://deq.virginia.gov/report.pdf"


# --- Full deduplicate function ---


class TestDeduplicate:
    def test_empty_dataframe(self):
        df = pd.DataFrame(
            columns=["source_url", "permit_number", "document_date", "source_portal", "scraped_at"]
        )
        result = deduplicate(df)
        assert len(result) == 0
        assert "sources" in result.columns

    def test_single_record(self):
        df = pd.DataFrame(
            [
                {
                    "source_url": "https://example.com/a",
                    "permit_number": "VA0091383",
                    "document_date": "2024-01-01",
                    "document_title": "Flow Report",
                    "source_portal": "epa_echo_dmr",
                    "scraped_at": "2026-01-01",
                }
            ]
        )
        result = deduplicate(df)
        assert len(result) == 1
        assert result.iloc[0]["permit_number"] == "VA0091383"

    def test_exact_url_dedup(self):
        """Two records with the same source_url should merge into one."""
        df = pd.DataFrame(
            [
                {
                    "source_url": "https://example.com/same",
                    "permit_number": "VA0091383",
                    "document_date": "2024-01-01",
                    "document_title": "Report",
                    "source_portal": "epa_echo_dmr",
                    "extracted_water_metric": "6.4 MGD",
                    "scraped_at": "2026-01-01",
                },
                {
                    "source_url": "https://example.com/same",
                    "permit_number": "VA0091383",
                    "document_date": "2024-01-01",
                    "document_title": "Report",
                    "source_portal": "epa_echo_dmr",
                    "extracted_water_metric": "6.4 MGD",
                    "scraped_at": "2026-02-01",
                },
            ]
        )
        result = deduplicate(df)
        assert len(result) == 1

    def test_permit_date_cross_source_dedup(self):
        """Same permit+date from different portals should merge."""
        df = pd.DataFrame(
            [
                {
                    "source_url": "https://epa.gov/echo/VA0091383",
                    "permit_number": "VA0091383",
                    "document_date": "2024-01-15",
                    "document_title": "DMR Report",
                    "source_portal": "epa_echo_dmr",
                    "extracted_water_metric": "6.4 MGD",
                    "scraped_at": "2026-01-01",
                },
                {
                    "source_url": "https://deq.virginia.gov/VA0091383",
                    "permit_number": "VA0091383",
                    "document_date": "2024-01-20",
                    "document_title": "DEQ Permit Record",
                    "source_portal": "va_deq_arcgis",
                    "extracted_water_metric": "",
                    "scraped_at": "2026-02-01",
                },
            ]
        )
        result = deduplicate(df)
        # Same permit + same month -> merged
        assert len(result) == 1
        assert "epa_echo_dmr" in result.iloc[0]["sources"]
        assert "va_deq_arcgis" in result.iloc[0]["sources"]

    def test_different_permits_not_merged(self):
        """Records with different permit numbers should stay separate."""
        df = pd.DataFrame(
            [
                {
                    "source_url": "https://example.com/a",
                    "permit_number": "VA0091383",
                    "document_date": "2024-01-01",
                    "document_title": "Report A",
                    "source_portal": "epa_echo_dmr",
                    "scraped_at": "2026-01-01",
                },
                {
                    "source_url": "https://example.com/b",
                    "permit_number": "VA0024988",
                    "document_date": "2024-01-01",
                    "document_title": "Report B",
                    "source_portal": "epa_echo_dmr",
                    "scraped_at": "2026-01-01",
                },
            ]
        )
        result = deduplicate(df)
        assert len(result) == 2

    def test_different_months_not_merged(self):
        """Same permit but different months should stay separate."""
        df = pd.DataFrame(
            [
                {
                    "source_url": "https://example.com/jan",
                    "permit_number": "VA0091383",
                    "document_date": "2024-01-15",
                    "document_title": "Jan Report",
                    "source_portal": "epa_echo_dmr",
                    "scraped_at": "2026-01-01",
                },
                {
                    "source_url": "https://example.com/feb",
                    "permit_number": "VA0091383",
                    "document_date": "2024-02-15",
                    "document_title": "Feb Report",
                    "source_portal": "epa_echo_dmr",
                    "scraped_at": "2026-01-01",
                },
            ]
        )
        result = deduplicate(df)
        assert len(result) == 2

    def test_no_permit_records_preserved(self):
        """Records without permit numbers should not be lost."""
        df = pd.DataFrame(
            [
                {
                    "source_url": "https://example.com/a",
                    "permit_number": "",
                    "document_date": "2024-01-01",
                    "document_title": "General Report",
                    "source_portal": "oh_central_water_study",
                    "scraped_at": "2026-01-01",
                },
                {
                    "source_url": "https://example.com/b",
                    "permit_number": "",
                    "document_date": "2024-01-01",
                    "document_title": "Another Report",
                    "source_portal": "va_fairfax_water",
                    "scraped_at": "2026-01-01",
                },
            ]
        )
        result = deduplicate(df)
        assert len(result) == 2

    def test_fuzzy_title_dedup(self):
        """Very similar titles for the same permit should merge."""
        df = pd.DataFrame(
            [
                {
                    "source_url": "https://example.com/a",
                    "permit_number": "VA0091383",
                    "document_date": "",
                    "document_title": "VPDES Outfall: Amazon DDC4 - VAR052461 (Outfall 001)",
                    "source_portal": "va_deq_arcgis",
                    "scraped_at": "2026-01-01",
                },
                {
                    "source_url": "https://example.com/b",
                    "permit_number": "VA0091383",
                    "document_date": "",
                    "document_title": "VPDES Outfall: Amazon DDC4 - VAR052461 (Outfall 002)",
                    "source_portal": "va_deq_arcgis",
                    "scraped_at": "2026-02-01",
                },
            ]
        )
        result = deduplicate(df, title_threshold=0.85)
        assert len(result) == 1

    def test_sources_column_always_present(self):
        """Output should always have a 'sources' column."""
        df = pd.DataFrame(
            [
                {
                    "source_url": "x",
                    "permit_number": "P1",
                    "document_date": "",
                    "document_title": "T1",
                    "source_portal": "sp1",
                    "scraped_at": "2026-01-01",
                }
            ]
        )
        result = deduplicate(df)
        assert "sources" in result.columns

    def test_preserves_most_recent_scrape(self):
        """When merging, the most recent scraped_at should win."""
        df = pd.DataFrame(
            [
                {
                    "source_url": "https://example.com/same",
                    "permit_number": "VA0091383",
                    "document_date": "2024-01-01",
                    "document_title": "Report",
                    "source_portal": "epa_echo_dmr",
                    "extracted_water_metric": "5.0 MGD",
                    "scraped_at": "2026-01-01",
                },
                {
                    "source_url": "https://example.com/same",
                    "permit_number": "VA0091383",
                    "document_date": "2024-01-01",
                    "document_title": "Report",
                    "source_portal": "epa_echo_dmr",
                    "extracted_water_metric": "6.4 MGD",
                    "scraped_at": "2026-03-01",
                },
            ]
        )
        result = deduplicate(df)
        assert len(result) == 1
        # Both have same completeness; more recent one should win
        assert result.iloc[0]["extracted_water_metric"] == "6.4 MGD"
