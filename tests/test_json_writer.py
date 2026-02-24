"""Tests for the JSONWriter append-only output module."""

import sys
import os
import json
import tempfile
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from storage.json_writer import JSONWriter
from models.document import DocumentRecord, DocumentSource


def _make_record(title="Test Doc", url="https://example.com/doc") -> DocumentRecord:
    return DocumentRecord(
        state="Virginia",
        municipality_agency="DEQ",
        document_title=title,
        source_url=url,
        source_portal=DocumentSource.VA_DEQ_VPDES_EXCEL,
        scraped_at=datetime(2026, 1, 1),
    )


class TestJSONWriter:

    def setup_method(self):
        self.tmpdir = tempfile.mkdtemp()
        self.output_path = os.path.join(self.tmpdir, "results.json")
        self.writer = JSONWriter(self.output_path)

    def test_write_to_new_file(self):
        """Should create a new JSON file with records."""
        records = [_make_record()]
        self.writer.write(records)

        with open(self.output_path) as f:
            data = json.load(f)
        assert len(data) == 1
        assert data[0]["document_title"] == "Test Doc"
        assert data[0]["source_portal"] == "va_deq_vpdes_excel"

    def test_append_to_existing(self):
        """Should append new records to an existing file."""
        self.writer.write([_make_record(title="First", url="https://example.com/1")])
        self.writer.write([_make_record(title="Second", url="https://example.com/2")])

        with open(self.output_path) as f:
            data = json.load(f)
        assert len(data) == 2
        titles = [d["document_title"] for d in data]
        assert "First" in titles
        assert "Second" in titles

    def test_dedup_replaces_existing(self):
        """Duplicate source_url + title should replace the old record."""
        self.writer.write([_make_record(title="Doc A", url="https://example.com/a")])
        # Write again with same key — should replace, not duplicate
        self.writer.write([_make_record(title="Doc A", url="https://example.com/a")])

        with open(self.output_path) as f:
            data = json.load(f)
        assert len(data) == 1

    def test_handles_corrupt_existing_file(self):
        """Should recover gracefully if existing JSON is corrupted."""
        with open(self.output_path, "w") as f:
            f.write("{bad json")

        self.writer.write([_make_record()])
        with open(self.output_path) as f:
            data = json.load(f)
        assert len(data) == 1

    def test_creates_parent_directories(self):
        """Should create parent dirs if they don't exist."""
        nested_path = os.path.join(self.tmpdir, "sub", "dir", "results.json")
        writer = JSONWriter(nested_path)
        writer.write([_make_record()])
        assert os.path.exists(nested_path)
