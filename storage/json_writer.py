import json
from pathlib import Path

from models.document import DocumentRecord


class JSONWriter:
    """Append DocumentRecords to a JSON file (as a JSON array)."""

    def __init__(self, output_path: str):
        self.output_path = Path(output_path)

    def write(self, records: list[DocumentRecord]):
        """Append records to JSON. Loads existing data first to preserve it."""
        self.output_path.parent.mkdir(parents=True, exist_ok=True)

        existing = []
        if self.output_path.exists() and self.output_path.stat().st_size > 0:
            try:
                with open(self.output_path, "r", encoding="utf-8") as f:
                    existing = json.load(f)
            except (json.JSONDecodeError, IOError):
                existing = []

        # Build set of existing source_url + title for dedup
        existing_keys = set()
        for entry in existing:
            key = (entry.get("source_url", ""), entry.get("document_title", ""))
            existing_keys.add(key)

        new_records = []
        for rec in records:
            d = rec.to_dict()
            key = (d.get("source_url", ""), d.get("document_title", ""))
            if key in existing_keys:
                # Conflict: replace with newer version (append-only with adjudication)
                existing = [
                    e for e in existing
                    if (e.get("source_url", ""), e.get("document_title", "")) != key
                ]
                existing_keys.discard(key)
            new_records.append(d)
            existing_keys.add(key)

        all_data = existing + new_records

        with open(self.output_path, "w", encoding="utf-8") as f:
            json.dump(all_data, f, indent=2, ensure_ascii=False)
