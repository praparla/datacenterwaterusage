# Data Center Water Use Tracker — Project Guide

## Project Overview

Python-based scraping and data extraction pipeline that finds documents related to data center water usage from public utility and environmental portals in Virginia and Ohio. Extracts water consumption metrics into structured CSV/JSON output.

## Core Principles

### 1. Resource-Efficient Scraping
- **Start small, test, iterate.** When building or running a scraper, test against a single page or a handful of documents first. Confirm the data looks correct before scaling up to full runs.
- Use `--limit N` flags or similar mechanisms to cap document fetches during development.
- Respect government servers: randomized 2-5 second delays between requests, never parallel-blast a single host.
- If a scraper doesn't need Playwright (e.g., direct file download or REST API), don't use Playwright.

### 2. Caching and Data Reuse
- All downloaded files are stored locally in `data/downloads/` organized by state and agency.
- The SQLite state database (`data/state/scraper_state.db`) tracks every fetched document by (scraper_name, document_id). Re-runs skip already-fetched documents automatically.
- Before downloading a file, check if it already exists at the expected local path. Only re-download if the remote version is newer or the local copy is corrupted.
- Extracted results are cached — don't re-extract text from a PDF that hasn't changed.

### 3. Append-Only Data, No Overwrites
- When writing to `results.csv` or `results.json`, **append** new records rather than overwriting the file.
- If a new scrape produces a record that conflicts with an existing one (same document_id/source_url), adjudicate: keep the **newer** version since government portals update documents over time.
- The state database tracks timestamps for this purpose. Use `scraped_at` to determine recency.
- Never delete raw downloaded files unless explicitly asked to clean up.

### 4. Source Attribution
- Every `DocumentRecord` must include the `source_url` pointing to the original document or portal page.
- PDF files stored locally must have their `local_file_path` recorded in the output.
- When extracting quotes or metrics, include enough context to trace back to the source section.
- The `source_portal` enum identifies which scraper produced the record.

### 5. Git Discipline
- Initialize git at project start. Commit at natural checkpoints:
  - After creating the project structure and foundation modules
  - After each scraper is implemented and tested
  - After the extraction pipeline is working end-to-end
  - After fixing bugs or errors
- Write descriptive commit messages that explain *what* and *why*.
- Don't commit `data/downloads/` (large binary files) — add to `.gitignore`. Do commit `data/output/` samples if they're small.

### 6. Error Logging
- When errors occur during scraping, extraction, or testing, log them to `errors.md` with:
  - Date/time
  - Which scraper/module failed
  - Error message and traceback summary
  - Resolution status (open / fixed)
- When an error is fixed or a failing test passes, update the corresponding entry in `errors.md` with the resolution.
- Use structured logging (`structlog`) in code for runtime errors. `errors.md` is the human-readable audit trail.

### 7. Testing and Validation
- Write tests alongside code, not as an afterthought. Each extractor module should have corresponding tests in `tests/`.
- Test against real sample data when possible (save a sample PDF or Excel snippet in `tests/fixtures/`).
- When a test fails, document it in `errors.md`. When it's fixed, update the entry.
- Validate output data: check that required fields are non-empty, dates parse correctly, URLs are valid.

### 8. Backlog Management
- When ideas come up for improvements, new scrapers, or enhancements, add them to `backlog.md` immediately.
- Each backlog item should include a sample prompt that could be used to generate a plan for implementing it.
- Prioritize backlog items periodically — mark items as low/medium/high priority.

### 9. Living Document
- Update this CLAUDE.md as the project evolves:
  - Add new scrapers to the architecture section below as they're built
  - Document any changed conventions or patterns
  - Record key decisions and their rationale

---

## Architecture

### Tech Stack
- **Browser automation**: playwright (async)
- **HTML parsing**: beautifulsoup4 + lxml
- **HTTP client**: httpx (async, rate-limited via tenacity)
- **PDF extraction**: pdfplumber (tables) + PyMuPDF/fitz (text fallback)
- **Excel parsing**: openpyxl
- **State/resumability**: aiosqlite
- **Logging**: structlog
- **CLI**: click
- **Testing**: pytest + pytest-asyncio

### Key Directories
- `scrapers/` — one module per government portal, organized by state
- `extractors/` — PDF text extraction, keyword matching, entity extraction
- `models/` — dataclasses for DocumentRecord
- `storage/` — CSV/JSON writers, SQLite state manager, file download manager
- `utils/` — HTTP client, Playwright browser manager, user-agent pool, logging config
- `data/downloads/` — raw downloaded files (gitignored)
- `data/output/` — structured CSV/JSON results
- `data/state/` — SQLite database for scraper state

### Scraper Status
| Scraper | Portal | Status |
|---------|--------|--------|
| deq_vpdes_excel | VA DEQ VPDES Excel | Not started |
| deq_arcgis | VA DEQ ArcGIS REST | Not started |
| deq_public_notices | VA DEQ Public Notices | Not started |
| deq_peep_tableau | VA DEQ PEEP/VPT | Not started |
| loudoun_boarddocs | Loudoun Water BoardDocs | Not started |
| loudoun_highbond | Loudoun Water Highbond | Not started |
| pwc_eservices | Prince William County | Not started |
| epa_edocument | Ohio EPA eDocument | Not started |
| columbus_legistar | Columbus Legistar API | Not started |
| columbus_utilities | Columbus Utilities Board | Not started |
| new_albany | New Albany Council | Not started |
