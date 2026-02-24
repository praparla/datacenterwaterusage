from __future__ import annotations

import abc
import asyncio
import random
from typing import AsyncGenerator, Optional

import structlog

from models.document import DocumentRecord, DocumentSource
from storage.state_manager import StateManager
from storage.file_store import FileStore


class BaseScraper(abc.ABC):
    """Abstract base for all portal scrapers.

    Subclasses implement discover() and fetch_document().
    The run() method orchestrates: discover -> check state -> fetch -> rate limit -> build record.
    """

    def __init__(self, config: dict, state_manager: StateManager, file_store: FileStore, browser=None):
        self.config = config
        self.state_manager = state_manager
        self.file_store = file_store
        self.browser = browser
        self.logger = structlog.get_logger(scraper=self.name)

    @property
    @abc.abstractmethod
    def name(self) -> str:
        """Unique identifier for this scraper."""
        ...

    @property
    @abc.abstractmethod
    def source(self) -> DocumentSource:
        """Which DocumentSource enum value this scraper produces."""
        ...

    @abc.abstractmethod
    async def discover(self, limit: int | None = None) -> AsyncGenerator[dict, None]:
        """Yield metadata dicts for each document found.

        Each dict should have at minimum: {"url": ..., "title": ..., "date": ...}
        Optional keys: "state", "agency", "permit_number", "filename"

        Args:
            limit: Max number of documents to discover (None = no limit).
        """
        ...

    @abc.abstractmethod
    async def fetch_document(self, metadata: dict) -> Optional[str]:
        """Download a single document. Returns local file path or None on failure."""
        ...

    async def run(self, limit: int | None = None) -> list[DocumentRecord]:
        """Main entry point: discover, fetch, and return records.

        Args:
            limit: Max documents to process (for testing with small batches).
        """
        self.logger.info("scraper_starting", limit=limit)
        results = []
        count = 0

        try:
            async for meta in self.discover(limit=limit):
                doc_id = meta.get("url") or meta.get("id", "")

                # Skip already-processed docs (resumability)
                if await self.state_manager.is_fetched(self.name, doc_id):
                    self.logger.debug("skipping_already_fetched", doc_id=doc_id)
                    continue

                try:
                    local_path = await self.fetch_document(meta)
                    await self._rate_limit_delay()

                    await self.state_manager.mark_fetched(self.name, doc_id, local_path)
                    results.append(self._build_record(meta, local_path))
                    count += 1
                    self.logger.info("document_fetched", doc_id=doc_id, count=count)

                except Exception as e:
                    self.logger.error("document_fetch_failed", doc_id=doc_id, error=str(e))
                    continue

        except Exception as e:
            self.logger.error("scraper_discover_failed", error=str(e))

        self.logger.info("scraper_finished", total_documents=count)
        return results

    async def _rate_limit_delay(self):
        delay = random.uniform(
            self.config.get("min_delay", 2.0),
            self.config.get("max_delay", 5.0),
        )
        await asyncio.sleep(delay)

    def _build_record(self, meta: dict, local_path: Optional[str]) -> DocumentRecord:
        """Construct a DocumentRecord from scraper metadata."""
        date_val = meta.get("date")
        return DocumentRecord(
            state=meta.get("state", ""),
            municipality_agency=meta.get("agency", ""),
            document_title=meta.get("title", ""),
            document_date=date_val,
            source_url=meta.get("url", ""),
            document_url=meta.get("document_url") or meta.get("pdf_url"),
            local_file_path=local_path,
            source_portal=self.source,
            permit_number=meta.get("permit_number"),
            match_term=meta.get("match_term"),
            matched_company=meta.get("matched_company"),
        )
