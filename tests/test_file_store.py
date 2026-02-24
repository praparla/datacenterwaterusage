"""Tests for the FileStore local file management module."""

import sys
import os
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from storage.file_store import FileStore


class TestFileStore:

    def setup_method(self):
        self.tmpdir = tempfile.mkdtemp()
        self.store = FileStore(self.tmpdir)

    def test_get_path_creates_directory(self):
        """get_path should create intermediate directories."""
        path = self.store.get_path("virginia", "DEQ", "test.pdf")
        assert os.path.isdir(os.path.dirname(path))
        assert path.endswith("test.pdf")

    def test_get_path_sanitizes_names(self):
        """Special characters in agency name should be replaced."""
        path = self.store.get_path("ohio", "EPA / Division", "report (1).pdf")
        assert "/" not in os.path.basename(os.path.dirname(path))
        assert "(" not in os.path.basename(path)

    def test_exists_false_for_missing_file(self):
        """exists() should return False for a file that hasn't been created."""
        assert not self.store.exists("virginia", "DEQ", "nonexistent.pdf")

    def test_exists_true_for_real_file(self):
        """exists() should return True for a file with content."""
        path = self.store.get_path("virginia", "DEQ", "real.pdf")
        with open(path, "w") as f:
            f.write("content")
        assert self.store.exists("virginia", "DEQ", "real.pdf")

    def test_exists_false_for_empty_file(self):
        """exists() should return False for a zero-byte file."""
        path = self.store.get_path("virginia", "DEQ", "empty.pdf")
        with open(path, "w") as f:
            pass  # empty file
        assert not self.store.exists("virginia", "DEQ", "empty.pdf")

    def test_list_files_empty_dir(self):
        """list_files should return empty list for nonexistent agency."""
        files = self.store.list_files("virginia", "NONEXISTENT")
        assert files == []

    def test_list_files_returns_files(self):
        """list_files should return paths of files in the directory."""
        path1 = self.store.get_path("ohio", "EPA", "file1.pdf")
        path2 = self.store.get_path("ohio", "EPA", "file2.xlsx")
        for p in [path1, path2]:
            with open(p, "w") as f:
                f.write("data")
        files = self.store.list_files("ohio", "EPA")
        assert len(files) == 2
        basenames = [os.path.basename(f) for f in files]
        assert "file1.pdf" in basenames
        assert "file2.xlsx" in basenames
