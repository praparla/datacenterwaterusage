from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional


class DocumentSource(Enum):
    VA_DEQ_VPDES_EXCEL = "va_deq_vpdes_excel"
    VA_DEQ_PUBLIC_NOTICES = "va_deq_public_notices"
    VA_DEQ_PEEP = "va_deq_peep"
    VA_DEQ_ARCGIS = "va_deq_arcgis"
    VA_LOUDOUN_BOARDDOCS = "va_loudoun_boarddocs"
    VA_LOUDOUN_HIGHBOND = "va_loudoun_highbond"
    VA_PWC_ESERVICES = "va_pwc_eservices"
    VA_LOUDOUN_ACFR = "va_loudoun_acfr"
    VA_FAIRFAX_WATER = "va_fairfax_water"
    OH_EPA_EDOCUMENT = "oh_epa_edocument"
    OH_COLUMBUS_LEGISTAR = "oh_columbus_legistar"
    OH_COLUMBUS_UTILITIES = "oh_columbus_utilities"
    OH_NEW_ALBANY = "oh_new_albany"
    OH_EPA_NPDES_ARCGIS = "oh_epa_npdes_arcgis"
    OH_ODNR_WATER_WITHDRAWAL = "oh_odnr_water_withdrawal"
    EPA_ECHO_DMR = "epa_echo_dmr"
    OH_CENTRAL_WATER_STUDY = "oh_central_water_study"


@dataclass
class DocumentRecord:
    state: str
    municipality_agency: str
    document_title: str
    source_url: str
    source_portal: DocumentSource
    document_date: Optional[datetime] = None
    company_llc_name: Optional[str] = None
    extracted_water_metric: Optional[str] = None
    extracted_quote: Optional[str] = None
    local_file_path: Optional[str] = None
    permit_number: Optional[str] = None
    match_term: Optional[str] = None
    matched_company: Optional[str] = None
    document_url: Optional[str] = None
    keyword_matches: list[str] = field(default_factory=list)
    relevance_score: float = 0.0
    scraped_at: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> dict:
        return {
            "state": self.state,
            "municipality_agency": self.municipality_agency,
            "document_title": self.document_title,
            "document_date": self.document_date.isoformat() if self.document_date else "",
            "company_llc_name": self.company_llc_name or "",
            "extracted_water_metric": self.extracted_water_metric or "",
            "extracted_quote": (self.extracted_quote or "")[:500],
            "source_url": self.source_url,
            "document_url": self.document_url or "",
            "local_file_path": self.local_file_path or "",
            "source_portal": self.source_portal.value,
            "permit_number": self.permit_number or "",
            "match_term": self.match_term or "",
            "matched_company": self.matched_company or "",
            "keyword_matches": "; ".join(self.keyword_matches),
            "relevance_score": f"{self.relevance_score:.2f}",
            "scraped_at": self.scraped_at.isoformat(),
        }
