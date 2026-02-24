"""Tests for the DocumentRecord model and DocumentSource enum."""

import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models.document import DocumentRecord, DocumentSource


class TestDocumentSource:

    def test_all_sources_have_string_values(self):
        """Every DocumentSource member should have a non-empty string value."""
        for source in DocumentSource:
            assert isinstance(source.value, str)
            assert len(source.value) > 0

    def test_known_sources_exist(self):
        """Key sources used by scrapers should be defined."""
        assert DocumentSource.VA_DEQ_VPDES_EXCEL.value == "va_deq_vpdes_excel"
        assert DocumentSource.VA_DEQ_ARCGIS.value == "va_deq_arcgis"
        assert DocumentSource.OH_EPA_EDOCUMENT.value == "oh_epa_edocument"


class TestDocumentRecord:

    def _make_record(self, **overrides) -> DocumentRecord:
        defaults = dict(
            state="Virginia",
            municipality_agency="DEQ",
            document_title="Test Document",
            source_url="https://example.com/doc",
            source_portal=DocumentSource.VA_DEQ_VPDES_EXCEL,
        )
        defaults.update(overrides)
        return DocumentRecord(**defaults)

    def test_to_dict_required_fields(self):
        """to_dict should include all required fields with correct values."""
        rec = self._make_record()
        d = rec.to_dict()
        assert d["state"] == "Virginia"
        assert d["municipality_agency"] == "DEQ"
        assert d["document_title"] == "Test Document"
        assert d["source_url"] == "https://example.com/doc"
        assert d["source_portal"] == "va_deq_vpdes_excel"

    def test_to_dict_optional_fields_default_empty(self):
        """Optional fields should default to empty strings in the dict."""
        rec = self._make_record()
        d = rec.to_dict()
        assert d["company_llc_name"] == ""
        assert d["extracted_water_metric"] == ""
        assert d["permit_number"] == ""

    def test_to_dict_with_date(self):
        """document_date should serialize as ISO string."""
        rec = self._make_record(document_date=datetime(2026, 3, 15))
        d = rec.to_dict()
        assert d["document_date"] == "2026-03-15T00:00:00"

    def test_to_dict_truncates_long_quotes(self):
        """extracted_quote should be truncated to 500 chars."""
        long_quote = "x" * 1000
        rec = self._make_record(extracted_quote=long_quote)
        d = rec.to_dict()
        assert len(d["extracted_quote"]) == 500

    def test_to_dict_keyword_matches_joined(self):
        """keyword_matches list should be semicolon-joined in dict."""
        rec = self._make_record(keyword_matches=["data center", "MGD", "cooling tower"])
        d = rec.to_dict()
        assert d["keyword_matches"] == "data center; MGD; cooling tower"

    def test_to_dict_relevance_score_formatted(self):
        """relevance_score should be formatted to 2 decimal places."""
        rec = self._make_record(relevance_score=0.85)
        d = rec.to_dict()
        assert d["relevance_score"] == "0.85"

    def test_scraped_at_auto_set(self):
        """scraped_at should be automatically set on creation."""
        rec = self._make_record()
        assert isinstance(rec.scraped_at, datetime)
