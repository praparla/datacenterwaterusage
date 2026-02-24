import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

CONFIG = {
    # Rate limiting
    "min_delay": 2.0,
    "max_delay": 5.0,

    # Paths
    "downloads_dir": os.path.join(BASE_DIR, "data", "downloads"),
    "csv_output_path": os.path.join(BASE_DIR, "data", "output", "results.csv"),
    "json_output_path": os.path.join(BASE_DIR, "data", "output", "results.json"),
    "state_db_path": os.path.join(BASE_DIR, "data", "state", "scraper_state.db"),

    # Virginia DEQ
    "va_deq_vpdes_page": "https://www.deq.virginia.gov/permits/water/surface-waters-vpdes",
    "va_deq_arcgis_vpdes_outfalls": (
        "https://apps.deq.virginia.gov/arcgis/rest/services/public/EDMA/MapServer/119/query"
    ),
    "va_deq_public_notices": (
        "https://www.deq.virginia.gov/permits/public-notices/water/surface-waters-vpdes"
    ),
    "va_deq_peep_search": "https://portal.deq.virginia.gov/reports/tableau/peep-search",

    # Loudoun Water
    "va_loudoun_boarddocs": "https://go.boarddocs.com/va/lwva/Board.nsf/Public",
    "va_loudoun_highbond": "https://loudounwater.community.highbond.com/portal/",

    # Prince William County
    "va_pwc_eservices": "https://eservice.pwcgov.org/",

    # Ohio EPA
    "oh_epa_edocument": "https://edocpub.epa.ohio.gov/publicportal/edochome.aspx",

    # Columbus
    "oh_columbus_legistar_api": "https://webapi.legistar.com/v1/columbus",
    "oh_columbus_utilities": "https://www.columbusutilities.org/board-agendas-minutes/",

    # New Albany
    "oh_new_albany_council": "https://newalbanyohio.org/boards-commissions/city-council-business/",

    # Search keywords for full-text search portals
    "search_keywords": [
        "data center",
        "water service agreement",
        "cooling tower",
        "evaporative cooling",
        "gallons per day",
        "million gallons per day",
        "consumptive use",
        "blowdown",
    ],

    # Known data center companies for entity matching
    "known_companies": [
        "Amazon", "AWS", "Microsoft", "Azure", "Google", "Meta", "Facebook",
        "QTS", "Equinix", "Digital Realty", "CyrusOne", "Vantage",
        "CloudHQ", "Iron Mountain", "CoreSite", "DataBank", "T5",
        "Compass Datacenters", "EdgeCore", "Stream Data Centers",
        "Aligned", "Stack Infrastructure", "Prime Data Centers",
    ],
}
