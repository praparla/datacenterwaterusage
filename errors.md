# Error Log

Errors encountered during development, scraping, and testing. Each entry includes the date, module, description, and resolution status.

Format:
```
### [YYYY-MM-DD] Short description
- **Module**: which scraper/extractor/module
- **Error**: error message or traceback summary
- **Context**: what was happening when the error occurred
- **Root cause**: code bug | test bug | infrastructure issue
- **Resolution**: open | fixed (with description of fix)
```

---

### [2026-02-23] test_gpd_match failed — "cooling operations" not in cooling keyword group
- **Module**: tests/test_keyword_matcher.py
- **Error**: `AssertionError: assert 'cooling' in {'water_volume': ['gallons per day']}` — test expected "cooling operations" to match the `cooling` keyword group, but the group only contains "cooling tower", "evaporative cooling", and "chiller".
- **Context**: Initial test run after building the keyword matcher. The test text said "cooling operations" but no keyword pattern matches the generic word "cooling" alone.
- **Root cause**: Test bug — the test used an input ("cooling operations") that doesn't match any keyword pattern. The production code was correct.
- **Resolution**: Fixed — changed test text from "cooling operations" to "cooling tower operations" which correctly matches the `cooling` keyword group. 34/34 tests now pass.

### [2026-02-23] Python 3.9 TypeError with `str | None` union syntax
- **Module**: storage/state_manager.py, scrapers/base.py, utils/browser.py, and 12 other files
- **Error**: `TypeError: unsupported operand type(s) for |: 'type' and 'NoneType'` — Python 3.9 does not support the `X | Y` type union syntax introduced in Python 3.10.
- **Context**: Running `python main.py --help` on macOS with system Python 3.9.6. The `str | None` annotations in function signatures caused import-time failures.
- **Root cause**: Code bug — used Python 3.10+ type syntax without ensuring compatibility with the project's Python 3.9 runtime.
- **Resolution**: Fixed — added `from __future__ import annotations` to all 15 affected files. This makes all annotations strings (PEP 563) and avoids runtime evaluation of the `|` operator. CLI and all 34 tests pass.

### [2026-02-23] False positive company name matching — substring "AWS" found in "LAWSON"
- **Module**: scrapers/virginia/deq_arcgis.py, scrapers/virginia/deq_vpdes_excel.py
- **Error**: ArcGIS scraper returned 5 false positives (e.g., "LAWSON INDUSTRIES" matched "AWS", "METALSMITH CO" matched "META") because the matching used `company in fac_upper` substring checking.
- **Context**: First live pipeline run with `--limit 5` on `va_deq_arcgis`. All 5 returned records were false positives.
- **Root cause**: Code bug — naive substring matching (`company in fac_upper`) has no word boundary awareness.
- **Resolution**: Fixed — created `utils/matching.py` with word-boundary regex (`\bAWS\b`) via `is_facility_match()`. Updated both `deq_arcgis.py` and `deq_vpdes_excel.py` to use it. Also added "Vadata", "Vadata Inc" to known_companies in config.py. Regression tests added in `tests/test_matching.py`.

### [2026-02-24] DEQ VPDES page returns 403 — IP rate-limited by WAF
- **Module**: scrapers/virginia/deq_vpdes_excel.py
- **Error**: `HTTPStatusError: Client error '403 Forbidden'` for `https://www.deq.virginia.gov/permits/water/surface-waters-vpdes`. Both httpx and Playwright headless get blocked.
- **Context**: After multiple requests to the DEQ site during pipeline development/testing, the WAF (web application firewall) started returning 403 for all requests from this IP. Even sync httpx with browser-like headers gets blocked.
- **Root cause**: Infrastructure issue — WAF rate-limiting triggered by repeated requests during development.
- **Resolution**: Open — temporary IP block. The scraper design is correct (HTTP-first with Playwright fallback). Will resolve on its own once the rate limit expires. Consider adding longer backoff delays or proxy support for production use.

### [2026-02-24] No water data in any Columbus Legistar downloaded documents
- **Module**: scrapers/ohio/columbus_legistar.py, data analysis
- **Error**: All 10 downloaded documents (5 PDFs, 4 Excel, 1 Word) from Columbus City Council contained zero water-related terms. Searched for: water, gallon, gpd, gpm, cooling, discharge, effluent, consumption, withdrawal, evaporation, wastewater, stormwater, mgd.
- **Context**: Auditing collected data to find water usage metrics. The Legistar scraper matched on `substringof('data center', MatterTitle)` which returned contracts for Columbus's own municipal IT "Data Center West" — UPS battery replacements, janitorial services, alarm monitoring, server maintenance.
- **Root cause**: Data source mismatch — Columbus Legistar legislation about "data center" refers to the city's internal IT data center, not commercial hyperscale data center facilities. These are municipal IT infrastructure contracts with no relevance to water consumption.
- **Resolution**: Noted — the Columbus Legistar scraper works correctly but this data source does not contain water usage data for commercial data centers. Consider adding more targeted search terms (e.g., "water service agreement", "cooling tower") or deprioritizing this scraper for water-specific queries.

### [2026-02-24] VA DEQ ArcGIS layer 119 contains no water volume/flow data
- **Module**: scrapers/virginia/deq_arcgis.py, API investigation
- **Error**: The ArcGIS VPDES Outfalls layer (MapServer/119) returns only spatial/permit metadata fields (FAC_NAME, VAP_PMT_NO, OUTFALLNO, VAP_TYPE). No numeric flow, discharge, volume, GPD, or MGD fields exist in this layer. The `_extract_metrics_from_attrs()` method always returns None.
- **Context**: Investigated all 230+ layers on the VA DEQ EDMA MapServer for DMR data. Checked layers 119 (VPDES Outfalls), 303 (Stormwater), 188-193 (VWP), 311-312 (VWP SSWD). All are spatial/location layers with permit metadata only.
- **Root cause**: Data source limitation — Virginia DEQ does not expose DMR (Discharge Monitoring Report) flow data through ArcGIS REST services. DMR data lives in DEQ's internal CEDS database and is submitted to EPA's federal ICIS-NPDES system.
- **Resolution**: Fixed — built EPA ECHO DMR scraper (`scrapers/epa_echo_dmr.py`) which accesses the federal ECHO system that receives DMR data from all states. The ArcGIS scraper remains useful for permit discovery and facility location data.

### [2026-02-24] Amazon VA permits are stormwater-only — no flow measurements
- **Module**: scrapers/virginia/deq_arcgis.py, API investigation
- **Error**: All 5 Amazon permits found via ArcGIS (VAR052461, VAR052418, VAR052420, VAR052421, VAR052422) have permit type `SWI_GP` (Stormwater Industrial General Permit). These monitor only Total Suspended Solids, Nitrogen, and Phosphorus — not water flow. No flow parameter (50050) exists on these permits.
- **Context**: Data centers in Virginia typically discharge cooling water blowdown to the municipal sewer system (e.g., Loudoun Water), not directly to surface waters. Individual data center facilities therefore do not hold their own VPDES discharge permits with flow data.
- **Root cause**: Data model insight — tracking water consumption at individual data centers requires monitoring the receiving wastewater treatment plants (e.g., Broad Run WRF, VA0091383) rather than the data center facilities themselves.
- **Resolution**: Fixed — the EPA ECHO DMR scraper targets treatment plant permits that serve data center corridors. Target permits configured in `config.py` under `epa_echo_target_permits`.

### [2026-02-24] EPA ECHO get_effluent and get_facility_info endpoints return HTTP 500
- **Module**: scrapers/epa_echo_dmr.py
- **Error**: `RetryError[HTTPStatusError 500]` from `eff_rest_services.get_effluent` and `cwa_rest_services.get_facility_info` for all tested permit IDs (VA0091383, VA0024988, OH0024651, OH0028061). The `get_qid` endpoint also returns 500.
- **Context**: Initial EPA ECHO scraper used the primary REST API endpoints documented at echo.epa.gov/tools/web-services. These main endpoints appear to have reliability issues or require parameters not documented in the public API.
- **Root cause**: API limitation — the primary ECHO REST endpoints (`get_effluent`, `get_facility_info`, `get_qid`) are unreliable. However, the chart/download endpoints work consistently.
- **Resolution**: Fixed — switched to `eff_rest_services.get_summary_chart` (for facility info) and `eff_rest_services.download_effluent_chart` (for DMR CSV data). These endpoints return complete data reliably. The CSV contains 64 columns per record with full DMR values, limits, and violation data.
