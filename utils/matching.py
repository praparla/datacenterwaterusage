"""Word-boundary matching utilities for facility/company name detection.

Prevents false positives like "LAWSON" matching "AWS" or "METALS" matching "META".
"""

from __future__ import annotations

import re
from functools import lru_cache


@lru_cache(maxsize=64)
def _compile_pattern(company: str) -> re.Pattern:
    """Compile a word-boundary regex for a company name."""
    escaped = re.escape(company)
    return re.compile(rf'\b{escaped}\b', re.IGNORECASE)


def is_facility_match(facility_name: str, known_companies: list[str]) -> bool:
    """Check if a facility name matches any known data center company using word boundaries.

    Also checks for "data center" in the facility name.
    """
    return get_match_reason(facility_name, known_companies) is not None


def get_match_reason(facility_name: str, known_companies: list[str]) -> str | None:
    """Return the term that matched, or None if no match.

    Returns "data center" if matched by that phrase, or the company name
    from known_companies that matched.
    """
    if not facility_name:
        return None

    if re.search(r'\bdata\s+cent(?:er|re)\b', facility_name, re.IGNORECASE):
        return "data center"

    for company in known_companies:
        pattern = _compile_pattern(company)
        if pattern.search(facility_name):
            return company

    return None
