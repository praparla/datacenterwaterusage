import os
import re
from pathlib import Path


class FileStore:
    """Manage local file storage for downloaded documents."""

    def __init__(self, downloads_dir: str):
        self.downloads_dir = Path(downloads_dir)

    def get_path(self, state: str, agency: str, filename: str) -> str:
        """Build a local file path: downloads_dir/state/agency/filename."""
        safe_agency = re.sub(r'[^\w\-.]', '_', agency.lower())
        dest_dir = self.downloads_dir / state.lower() / safe_agency
        dest_dir.mkdir(parents=True, exist_ok=True)
        safe_filename = re.sub(r'[^\w\-.]', '_', filename)
        return str(dest_dir / safe_filename)

    def exists(self, state: str, agency: str, filename: str) -> bool:
        path = self.get_path(state, agency, filename)
        return os.path.exists(path) and os.path.getsize(path) > 0

    def list_files(self, state: str, agency: str) -> list[str]:
        safe_agency = re.sub(r'[^\w\-.]', '_', agency.lower())
        dest_dir = self.downloads_dir / state.lower() / safe_agency
        if not dest_dir.exists():
            return []
        return [str(p) for p in dest_dir.iterdir() if p.is_file()]
