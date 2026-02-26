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

    # Loudoun Water ACFR — Annual Comprehensive Financial Reports
    # Confirmed live Feb 2026. 2023 data: 899M gal potable + 736M gal reclaimed to DCs.
    "va_loudoun_acfr_page": "https://www.loudounwater.org/about/comprehensive-annual-financial-reports",
    "va_loudoun_reclaimed_page": "https://www.loudounwater.org/commercial-customers/reclaimed-water-program",

    # Fairfax Water Financial Reports
    # Confirmed live Feb 2026. 2024 report PDF at fairfaxwater.org/about-us.
    # Supplies ~18 MGD wholesale to Loudoun Water; 47% of sales to wholesale customers.
    "va_fairfax_water_about_page": "https://www.fairfaxwater.org/about-us",
    "va_fairfax_water_budget_page": "https://www.fairfaxwater.org/your-water/rates-charges",

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
        "VA0026301",  # Upper Occoquan Service Authority (Prince William / Fairfax)
        "VA0026271",  # Noman M. Cole Jr. Pollution Control Plant (Fairfax Water)
        # Ohio — Columbus area (Franklin County)
        "OH0024651",  # Columbus Southerly WWTP
        "OH0028061",  # Columbus Jackson Pike WWTP
        # Ohio — Licking County / New Albany data center cluster
        "OH0020494",  # Newark WWTP (Licking County)
        "OH0068071",  # Southwest Licking Community WWTP (New Albany area)
    ],

    # Ohio EPA
    "oh_epa_edocument": "https://edocpub.epa.ohio.gov/publicportal/edochome.aspx",

    # Ohio EPA ArcGIS Open Data — NPDES Individual Permits
    # Confirmed live Feb 2026, updated nightly. Filter SIC_CODE='7374' for data centers.
    # Dataset ID: 1118cdc038884214ba79a0712b60ece7_0
    "oh_epa_npdes_arcgis_query": (
        "https://opendata.arcgis.com/api/v3/datasets/1118cdc038884214ba79a0712b60ece7_0/"
        "downloads/data?format=geojson&spatialRefId=4326"
    ),
    "oh_epa_npdes_arcgis_page": "https://data-oepa.opendata.arcgis.com/datasets/npdes-individual-permits",
    # Fallback: county-based permit lookup (HTML page)
    "oh_epa_npdes_by_county": "http://wwwapp.epa.ohio.gov/dsw/permits/permit_list.php",

    # ODNR Water Withdrawal Facility Viewer
    # ArcGIS Experience Builder app confirmed live Feb 2026.
    # TODO: Find backing FeatureServer URL by inspecting the app's network requests
    # (GET /arcgis/rest/services/.../FeatureServer/.../query from the experience URL)
    "oh_odnr_water_withdrawal_viewer": (
        "https://experience.arcgis.com/experience/0605c2eaf8fe458481ac323404b4ab36"
        "/page/Water-Withdrawal-Facility-Viewer"
    ),
    # ArcGIS Online item backing the viewer — use to discover the FeatureServer URL
    "oh_odnr_water_withdrawal_item": "0605c2eaf8fe458481ac323404b4ab36",
    # ArcGIS REST query endpoint for the water withdrawal data
    # Derive from item config; placeholder points to ODNR's known ArcGIS org
    "oh_odnr_water_withdrawal_service": (
        "https://services.arcgis.com/OZAiDlLbFTMOZIGD/arcgis/rest/services"
        "/Water_Withdrawal_Registrations/FeatureServer/0/query"
    ),
    # Data center cluster counties in Ohio for ODNR queries
    "oh_odnr_target_counties": ["Franklin", "Licking", "Delaware", "Union"],

    # Columbus
    "oh_columbus_legistar_api": "https://webapi.legistar.com/v1/columbus",
    "oh_columbus_utilities": "https://www.columbusutilities.org/board-agendas-minutes/",

    # New Albany
    "oh_new_albany_council": "https://newalbanyohio.org/boards-commissions/city-council-business/",

    # Central Ohio Regional Water Study (Ohio EPA, March 2025)
    # Confirmed live Feb 2026. Projects data center/Intel industrial demand:
    # ~40 MGD by 2030, ~70 MGD by 2040, ~90 MGD by 2050.
    "oh_central_water_study_pdfs": [
        {
            "url": (
                "https://dam.assets.ohio.gov/image/upload/epa.ohio.gov"
                "/Portals/0/water/CentralWaterStudyOverview.pdf"
            ),
            "title": "Central Ohio Regional Water Study — Overview (March 2025)",
            "id": "central-ohio-water-study-overview-2025",
        },
        {
            "url": (
                "https://dam.assets.ohio.gov/image/upload/epa.ohio.gov"
                "/Portals/0/water/LickingWaterStudy.pdf"
            ),
            "title": "Central Ohio Regional Water Study — Licking County Detail (March 2025)",
            "id": "central-ohio-water-study-licking-2025",
        },
        {
            "url": (
                "https://dam.assets.ohio.gov/image/upload/epa.ohio.gov"
                "/Portals/0/water/PickawayWaterStudy.pdf"
            ),
            "title": "Central Ohio Regional Water Study — Pickaway County Detail (March 2025)",
            "id": "central-ohio-water-study-pickaway-2025",
        },
    ],

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
        "Vadata", "Vadata Inc", "Intel",
    ],
}
