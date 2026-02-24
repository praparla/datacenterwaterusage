import re
from typing import Optional


class EntityExtractor:
    """Extract company names and water usage metrics from document text."""

    # Patterns for water volume mentions
    VOLUME_PATTERNS = [
        re.compile(
            r'([\d,]+(?:\.\d+)?)\s*'
            r'(MGD|GPD|GPM|gallons\s+per\s+day|'
            r'million\s+gallons\s+per\s+day|gallons\s+per\s+minute|'
            r'acre[\s-]?feet(?:\s+per\s+year)?)',
            re.IGNORECASE,
        ),
        re.compile(
            r'([\d,]+(?:\.\d+)?)\s+gallons',
            re.IGNORECASE,
        ),
    ]

    # Pattern for LLC/company names
    COMPANY_PATTERN = re.compile(
        r'([A-Z][A-Za-z0-9\s&,.\'-]+?'
        r'(?:LLC|Inc\.?|Corp\.?|LP|Ltd\.?|Company|Co\.?|'
        r'Partners|Holdings|Ventures|Properties|Realty|'
        r'Data\s+Centers?|Datacenters?))',
        re.MULTILINE,
    )

    def extract_water_metrics(self, text: str) -> list[str]:
        """Find all water volume mentions in text."""
        metrics = []
        for pattern in self.VOLUME_PATTERNS:
            for match in pattern.finditer(text):
                metrics.append(match.group(0).strip())
        return metrics

    def extract_company_names(self, text: str, known_companies: list[str]) -> list[str]:
        """Find company names. Checks known companies first, then regex for LLC/Inc entities."""
        found = []
        text_upper = text.upper()

        # Check known companies
        for company in known_companies:
            if company.upper() in text_upper:
                found.append(company)

        # Regex for LLC/Inc entities not already found
        for match in self.COMPANY_PATTERN.finditer(text):
            name = match.group(1).strip()
            if len(name) > 5 and name not in found:
                # Avoid false positives from common words
                lower = name.lower()
                if not any(skip in lower for skip in ["the state", "the city", "the county", "the board"]):
                    found.append(name)

        return found

    def extract_surrounding_context(
        self, text: str, keyword: str, window: int = 200
    ) -> Optional[str]:
        """Get a text window around a keyword match for the 'extracted_quote' field."""
        idx = text.lower().find(keyword.lower())
        if idx == -1:
            return None
        start = max(0, idx - window)
        end = min(len(text), idx + len(keyword) + window)
        snippet = text[start:end].strip()
        # Clean up whitespace
        snippet = re.sub(r'\s+', ' ', snippet)
        return snippet
