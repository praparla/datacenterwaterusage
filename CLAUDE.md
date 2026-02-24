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
- Commit **often** at natural checkpoints — small, focused commits are better than large monolithic ones:
  - After creating the project structure and foundation modules
  - After each new module or scraper is implemented
  - After writing or updating tests for a module
  - After fixing a bug or resolving a failing test
  - After refactoring or cleanup
  - After updating documentation (CLAUDE.md, errors.md, backlog.md)
- Don't let work accumulate — if you've made a meaningful change, commit it.
- Write descriptive commit messages that explain *what* and *why*.
- Don't commit `data/downloads/` (large binary files) — add to `.gitignore`. Do commit `data/output/` samples if they're small.

### 6. Error Logging
- When errors occur during scraping, extraction, or testing, log them to `errors.md` with:
  - Date/time
  - Which scraper/module failed
  - Error message and traceback summary
  - **Root cause classification**: Is this a **code bug** (logic error in production code) or a **test bug** (incorrect assertion, wrong test setup, stale fixture)?
  - Resolution status (open / fixed)
- When an error is fixed or a failing test passes, **immediately** update the corresponding entry in `errors.md` with:
  - What the fix was
  - Whether it was a code fix or a test fix
  - The commit that resolved it (if applicable)
- Use structured logging (`structlog`) in code for runtime errors. `errors.md` is the human-readable audit trail.
- After every bug fix, check whether the fix needs a new test or an updated test to prevent regression.

### 7. Testing and Validation
- **Write tests alongside code, not as an afterthought.** Every new module, function, or bug fix should include corresponding tests.
  - New module → add `tests/test_<module>.py` in the same session.
  - Bug fix → add a regression test that would have caught the bug.
  - New extractor or scraper → test with sample data.
- Each extractor, storage module, and utility should have corresponding tests in `tests/`.
- Test against real sample data when possible (save a sample PDF or Excel snippet in `tests/fixtures/`).
- When a test fails:
  1. Determine root cause: **code bug** vs. **test bug** (bad assertion, stale fixture, wrong expectation).
  2. Document in `errors.md` with the classification.
  3. Fix the appropriate side (code or test), then update `errors.md` with the resolution.
- Validate output data: check that required fields are non-empty, dates parse correctly, URLs are valid.
- Run the full test suite before committing to catch regressions early.

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
| Scraper | Portal | Status | Notes |
|---------|--------|--------|-------|
| deq_vpdes_excel | VA DEQ VPDES Excel | Built, blocked by WAF | 403 from DEQ site — see errors.md |
| deq_arcgis | VA DEQ ArcGIS REST | Working | Permit metadata only — no flow data in ArcGIS layers |
| deq_public_notices | VA DEQ Public Notices | Built | Playwright-based, needs testing |
| deq_peep_tableau | VA DEQ PEEP/VPT | Built | Power BI scraper, needs testing |
| loudoun_boarddocs | Loudoun Water BoardDocs | Built | BoardDocs JS rendering |
| loudoun_highbond | Loudoun Water Highbond | Built | Needs testing |
| pwc_eservices | Prince William County | Built | Dual HTTP + Playwright |
| epa_edocument | Ohio EPA eDocument | Built | ASP.NET WebForms, needs testing |
| columbus_legistar | Columbus Legistar API | Working | Municipal IT data center — no water data |
| columbus_utilities | Columbus Utilities Board | Built | Needs testing |
| new_albany | New Albany Council | Built | HTTP + Playwright fallback |
| **epa_echo** | **EPA ECHO DMR** | **Working** | **Primary water data source — flow MGD from treatment plants** |

### Key Architecture Decision: Water Data Source Strategy
Data centers discharge cooling water to municipal sewer systems, not directly to surface water.
Individual data center VPDES permits (e.g., Amazon's VAR052xxx) are stormwater-only permits
with no flow measurements. To track actual water usage, the pipeline monitors receiving
wastewater treatment plants via EPA ECHO DMR data. Target permits are configured in
`config.py` under `epa_echo_target_permits`.

### Data Source Tiers (identified Feb 2026)

**Tier 1 — Direct water metrics (highest value):**
- EPA ECHO DMR flow data from receiving WWTPs (currently implemented)
- Loudoun Water ACFRs — aggregate data center water sales (~1.6B gal/yr in 2023)
- Ohio EPA General Permit OHD000001 — once finalized, requires DMR from data centers directly
- Prince William Water Industrial User Survey — data center ERU allocations
- ODNR Water Withdrawal Facility Viewer — annual facility-level withdrawal volumes

**Tier 2 — Permit metadata and facility discovery:**
- EPA ECHO NAICS 518210 search — discover data center regulatory footprints
- EPA FRS cross-referencing — link facilities across 90+ EPA databases
- Ohio EPA ArcGIS NPDES permits — searchable by SIC code 7374
- Virginia DEQ ArcGIS VWP layers 192/193 — water withdrawal permits

**Tier 3 — Context and projections:**
- Central Ohio Regional Water Study (2025) — demand projections to 2050
- JLARC Data Centers in Virginia report (2024)
- EIA Form 923 — power plant cooling water for indirect footprint
- USGS county-level water use estimates (every 5 years)
- Virginia SB 553 (2026) — if enacted, mandates monthly data center water reporting

See `backlog.md` for detailed scraper plans and sample prompts for each source.
