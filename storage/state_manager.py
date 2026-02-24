from datetime import datetime
from pathlib import Path

import aiosqlite


class StateManager:
    """Track scraper progress in SQLite for resumability."""

    def __init__(self, db_path: str):
        self.db_path = db_path

    async def initialize(self):
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS scraper_state (
                    scraper_name TEXT,
                    document_id TEXT,
                    status TEXT DEFAULT 'fetched',
                    local_path TEXT,
                    fetched_at TEXT,
                    processed_at TEXT,
                    PRIMARY KEY (scraper_name, document_id)
                )
            """)
            await db.commit()

    async def is_processed(self, scraper_name: str, doc_id: str) -> bool:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "SELECT status FROM scraper_state WHERE scraper_name=? AND document_id=?",
                (scraper_name, doc_id),
            )
            row = await cursor.fetchone()
            return row is not None and row[0] == "processed"

    async def is_fetched(self, scraper_name: str, doc_id: str) -> bool:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "SELECT status FROM scraper_state WHERE scraper_name=? AND document_id=?",
                (scraper_name, doc_id),
            )
            row = await cursor.fetchone()
            return row is not None

    async def mark_fetched(self, scraper_name: str, doc_id: str, local_path: str | None = None):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """INSERT OR REPLACE INTO scraper_state
                   (scraper_name, document_id, status, local_path, fetched_at)
                   VALUES (?, ?, 'fetched', ?, ?)""",
                (scraper_name, doc_id, local_path, datetime.utcnow().isoformat()),
            )
            await db.commit()

    async def mark_processed(self, scraper_name: str, doc_id: str):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """UPDATE scraper_state SET status='processed', processed_at=?
                   WHERE scraper_name=? AND document_id=?""",
                (datetime.utcnow().isoformat(), scraper_name, doc_id),
            )
            await db.commit()
