# Error Log

Errors encountered during development, scraping, and testing. Each entry includes the date, module, description, and resolution status.

Format:
```
### [YYYY-MM-DD] Short description
- **Module**: which scraper/extractor/module
- **Error**: error message or traceback summary
- **Context**: what was happening when the error occurred
- **Resolution**: open | fixed (with description of fix)
```

---

### [2026-02-23] test_gpd_match failed — "cooling operations" not in cooling keyword group
- **Module**: tests/test_keyword_matcher.py
- **Error**: `AssertionError: assert 'cooling' in {'water_volume': ['gallons per day']}` — test expected "cooling operations" to match the `cooling` keyword group, but the group only contains "cooling tower", "evaporative cooling", and "chiller".
- **Context**: Initial test run after building the keyword matcher. The test text said "cooling operations" but no keyword pattern matches the generic word "cooling" alone.
- **Resolution**: Fixed — changed test text from "cooling operations" to "cooling tower operations" which correctly matches the `cooling` keyword group. 34/34 tests now pass.

### [2026-02-23] Python 3.9 TypeError with `str | None` union syntax
- **Module**: storage/state_manager.py, scrapers/base.py, utils/browser.py, and 12 other files
- **Error**: `TypeError: unsupported operand type(s) for |: 'type' and 'NoneType'` — Python 3.9 does not support the `X | Y` type union syntax introduced in Python 3.10.
- **Context**: Running `python main.py --help` on macOS with system Python 3.9.6. The `str | None` annotations in function signatures caused import-time failures.
- **Resolution**: Fixed — added `from __future__ import annotations` to all 15 affected files. This makes all annotations strings (PEP 563) and avoids runtime evaluation of the `|` operator. CLI and all 34 tests pass.
