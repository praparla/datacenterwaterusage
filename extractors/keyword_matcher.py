import re


# Keyword groups — all matched case-insensitively
KEYWORD_GROUPS = {
    "data_center": [
        r"data\s+center", r"data\s+centre", r"server\s+farm",
        r"colocation", r"co-location", r"hyperscale",
    ],
    "water_agreement": [
        r"water\s+service\s+agreement", r"water\s+supply\s+agreement",
        r"utility\s+agreement", r"service\s+agreement",
    ],
    "cooling": [
        r"cooling\s+tower", r"evaporative\s+cooling", r"chiller",
        r"heat\s+rejection", r"condenser\s+water",
    ],
    "water_volume": [
        r"gallons\s+per\s+day", r"\bGPD\b", r"million\s+gallons\s+per\s+day",
        r"\bMGD\b", r"gallons\s+per\s+minute", r"\bGPM\b",
        r"acre[\s-]?feet", r"AF/Y",
    ],
    "water_process": [
        r"blowdown", r"reclaimed\s+water", r"consumptive\s+use",
        r"make[\s-]?up\s+water", r"discharge", r"effluent",
        r"non[\s-]?potable", r"potable\s+water",
    ],
    "redaction": [
        r"trade\s+secret", r"redacted", r"confidential\s+business",
        r"proprietary",
    ],
}

# Relevance weights per keyword group
RELEVANCE_WEIGHTS = {
    "data_center": 0.30,
    "water_agreement": 0.25,
    "water_volume": 0.20,
    "cooling": 0.10,
    "water_process": 0.10,
    "redaction": 0.05,
}


class KeywordMatcher:
    """Match text against data-center water usage keyword groups and score relevance."""

    def __init__(self):
        self._compiled: dict[str, list[re.Pattern]] = {}
        for group, patterns in KEYWORD_GROUPS.items():
            self._compiled[group] = [re.compile(p, re.IGNORECASE) for p in patterns]

    def find_matches(self, text: str) -> dict[str, list[str]]:
        """Return {group_name: [matched_strings]} for all keyword groups found in text."""
        results = {}
        for group, regexes in self._compiled.items():
            matches = []
            for rgx in regexes:
                found = rgx.findall(text)
                matches.extend(found)
            if matches:
                results[group] = matches
        return results

    def compute_relevance_score(self, matches: dict[str, list[str]]) -> float:
        """Score 0.0-1.0 based on which keyword groups matched."""
        score = 0.0
        for group, weight in RELEVANCE_WEIGHTS.items():
            if group in matches:
                score += weight
        return min(score, 1.0)

    def is_relevant(self, text: str, threshold: float = 0.3) -> bool:
        """Check if text meets the relevance threshold."""
        matches = self.find_matches(text)
        return self.compute_relevance_score(matches) >= threshold

    def get_all_matched_keywords(self, matches: dict[str, list[str]]) -> list[str]:
        """Flatten all matched strings into a single deduplicated list."""
        seen = set()
        result = []
        for group_matches in matches.values():
            for kw in group_matches:
                lower = kw.lower().strip()
                if lower not in seen:
                    seen.add(lower)
                    result.append(kw.strip())
        return result
