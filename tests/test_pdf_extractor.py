"""Tests for PDF extraction module.

Note: These tests require actual PDF files. For unit testing without external
dependencies, we test the module's interface and error handling.
Integration tests with real PDFs should be run separately.
"""

import sys
import os
import tempfile
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from extractors.pdf_extractor import PDFExtractor


class TestPDFExtractor:

    def setup_method(self):
        self.extractor = PDFExtractor()

    def test_extract_text_nonexistent_file(self):
        """Should return empty string for a file that doesn't exist."""
        text = self.extractor.extract_text("/nonexistent/path/file.pdf")
        assert text == ""

    def test_extract_text_empty_file(self):
        """Should handle an empty/corrupt file gracefully."""
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as f:
            f.write(b"not a real pdf")
            tmp_path = f.name

        try:
            text = self.extractor.extract_text(tmp_path)
            # Should not crash, may return empty string
            assert isinstance(text, str)
        finally:
            os.unlink(tmp_path)

    def test_extract_tables_nonexistent_file(self):
        """Should return empty list for missing file."""
        tables = self.extractor.extract_tables("/nonexistent/path/file.pdf")
        assert tables == []

    def test_extract_tables_returns_list(self):
        """Table extraction should always return a list."""
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as f:
            f.write(b"not a real pdf")
            tmp_path = f.name

        try:
            tables = self.extractor.extract_tables(tmp_path)
            assert isinstance(tables, list)
        finally:
            os.unlink(tmp_path)
