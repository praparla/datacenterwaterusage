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

    # EPA ECHO (federal Enforcement & Compliance History Online — DMR data)
    "epa_echo_effluent_api": "https://echodata.epa.gov/echo/eff_rest_services.get_effluent",
    "epa_echo_dfr_api": "https://echodata.epa.gov/echo/dfr_rest_services.get_water_quality",
    "epa_echo_permits_api": "https://echodata.epa.gov/echo/cwa_rest_services.get_facility_info",

    # Key wastewater treatment plant permits that receive data center cooling water
    # These facilities serve areas with major data center clusters
    "epa_echo_target_permits": [
        # Virginia — Loudoun County / Northern VA data center corridor
        "VA0091383",  # Broad Run Water Reclamation Facility (Loudoun Water)
        "VA0024988",  # Henrico County Water Reclamation Facility
        # Ohio — Columbus area
        "OH0024651",  # Columbus Southerly WWTP
        "OH0028061",  # Columbus Jackson Pike WWTP
    ],

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
    # NOTE: Short names like "AWS", "Meta", "T5" need word-boundary matching
    # to avoid false positives (e.g. "LAWSON" matching "AWS").
    # Use utils.matching.is_facility_match() for all facility name checks.
    "known_companies": [
        "Amazon", "AWS", "Microsoft", "Azure", "Google", "Meta", "Facebook",
        "QTS", "Equinix", "Digital Realty", "CyrusOne", "Vantage",
        "CloudHQ", "Iron Mountain", "CoreSite", "DataBank", "T5",
        "Compass Datacenters", "EdgeCore", "Stream Data Centers",
        "Aligned", "Stack Infrastructure", "Prime Data Centers",
        "Vadata", "Vadata Inc",
    ],
}
