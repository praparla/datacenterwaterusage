# Backlog

Items are ordered by priority (high / medium / low). Each includes a sample prompt for generating an implementation plan.

Last reviewed: 2026-03-16.

---

## Completed (March 2026)

### ✅ EPA ECHO NAICS Facility Discovery (Federal)
**Status**: Built — `scrapers/epa_echo_naics.py` (18 tests)

### ✅ Ohio EPA Data Center General Permit Tracker (OHD000001)
**Status**: Built — `scrapers/ohio/epa_general_permit.py` (20 tests)

### ✅ Loudoun Water ACFR Scraper (Virginia)
**Status**: Built — `scrapers/virginia/loudoun_acfr.py` (26 tests)

### ✅ Expand EPA ECHO DMR Target Permits
**Status**: Done — 8 target permits in config.py (VA0091383, VA0024988, VA0026301, VA0026271, OH0024651, OH0028061, OH0020494, OH0068071)

### ✅ Dashboard / Visualization (Phase 1)
**Status**: Built — `dashboard.py` Streamlit dashboard with flow time series, permit limit overlays, seasonal heatmap, cross-filtering, data download (20 tests). Run with `streamlit run dashboard.py`.

### ✅ Fairfax Water Financial Reports (Virginia)
**Status**: Built — `scrapers/virginia/fairfax_water.py` (tests in test_fairfax_water.py). Downloads ACFR/PAFR PDFs, extracts wholesale delivery volumes.

### ✅ Central Ohio Regional Water Study Analysis
**Status**: Built — `scrapers/federal/central_ohio_water_study.py`. Downloads and processes the 3 study PDFs.

### ✅ Ohio EPA ArcGIS NPDES Permits
**Status**: Built — `scrapers/ohio/epa_npdes_arcgis.py` (tests in test_ohio_epa_npdes.py). Queries Ohio EPA Open Data for NPDES permits by SIC 7374.

### ✅ ODNR Water Withdrawal Facility Viewer (Ohio)
**Status**: Built — `scrapers/ohio/odnr_water_withdrawal.py` (tests in test_odnr_water_withdrawal.py). Queries ArcGIS FeatureServer for withdrawal registrations in central Ohio counties.

### ✅ Prince William Water Industrial User Survey (Virginia)
**Status**: Built — `scrapers/virginia/pwc_ius.py` (tests in test_pwc_ius.py). Downloads IUS PDFs, extracts ERU allocations, GPD/MGD values, data center counts. ERU→GPD conversion (1 ERU = 400 GPD).

### ✅ Virginia DEQ Water Withdrawal Permits (VWP)
**Status**: Built — `scrapers/virginia/deq_vwp.py` (tests in test_deq_vwp.py). Queries ArcGIS EDMA MapServer layers 192 (individual) and 193 (general) for water withdrawal permits in Northern Virginia counties.

---

## High Priority

### EPA FRS Cross-Reference Module (Federal)
The EPA Facility Registry Service links facilities across 90+ EPA databases. Query FRS for all NAICS 518210 facilities in VA/OH to get FRS Registry IDs, then cross-reference against NPDES, RCRA, TRI, and other programs. API: `https://enviro.epa.gov/enviro/efservice/FRS_NAICS/NAICS_CODE/518210/rows/0:99/JSON`

**Data status:** Confirmed. FRS REST API is documented and available (exchangenetwork.net). The ER_NAICS dataset is also available as an Esri REST API endpoint on data.gov (`catalog.data.gov/dataset/epa-facility-registry-service-frs-er_naics7`). NAICS codes in FRS are represented as first 3 digits (i.e., query `518`), not full 6-digit code.

**Sample prompt:**
> Build a utility module `utils/frs_lookup.py` that queries the EPA Envirofacts REST API for facilities by NAICS code and state. For each facility, retrieve the FRS Registry ID, geographic coordinates, and linked program IDs (NPDES, RCRA, TRI, CAA). Use this to build a mapping of data center facilities to their regulatory footprints across EPA databases. Integrate with the existing scraper pipeline to auto-discover which WWTP service areas data center facilities fall within.

### Virginia Legislative Tracker (SB 553) (elevated from Low)
Virginia SB 553 (2026 session) would require water providers to report data center water consumption to the State Water Control Board monthly. If enacted, this creates a new mandatory reporting data source.

**URL:** `https://legiscan.com/VA/bill/SB553/2026`

**Data status:** Active — very close to passage. Passed Senate 25-15 on February 9, 2026 (sponsor: Sen. Kannan Srinivasan, D-Loudoun). Currently in House Committee on Agriculture, Chesapeake and Natural Resources as of February 12, 2026. Related bills: HB496, HB591. If enacted, requires water providers to report total volume (including reclaimed water) provided to each data center monthly. Watch this closely — if it passes, the State Water Control Board reporting data becomes a major new scrape target.

**Sample prompt:**
> Build a simple legislative tracker that monitors the status of Virginia SB 553 and related bills (HB 496, HB 591) via the LegiScan API or Virginia LIS website. Alert when bill status changes. If SB 553 is enacted, design a scraper for the new monthly water consumption reports that will be filed with the State Water Control Board.

### Deduplication Engine
As we scrape multiple portals (now 20+ scrapers), the same document or data point may appear from different sources (e.g., a permit referenced in both DEQ and ECHO). Dedup is critical before the dataset grows larger.

**Sample prompt:**
> Build a deduplication module that identifies duplicate or near-duplicate records in the output CSV/JSON. Match on permit_number, source_url, and fuzzy-match on document_title. When duplicates are found, merge them into a single record keeping the most complete data from each source. Add a `sources` field that lists all portals where the record was found.

---

## Medium Priority

### JLARC Data Centers in Virginia Report
The December 2024 JLARC study found data center water use is sustainable but growing. Contains aggregate statistics, policy recommendations, and analysis of water impact. Reference material.

**URL:** `https://jlarc.virginia.gov/landing-2024-data-centers-in-virginia.asp`

**Data status:** Confirmed. Full report PDF available at `https://jlarc.virginia.gov/pdfs/reports/Rpt598-2.pdf`. Key water finding: "data center water use is currently sustainable, but use is growing and could be better managed." Recommends expressly authorizing localities to require water use estimates for proposed data center developments. Also covers energy (5 GW current demand, doubling in 15 years), economic impact ($9.1B GDP, 74K jobs), and sound/noise issues.

**Sample prompt:**
> Download the JLARC Data Centers in Virginia report and extract key findings: aggregate water consumption figures, growth projections, policy recommendations for local governments. Parse relevant tables and statistics. Store as reference data to provide context alongside scraped permit/DMR data.

### Fairfax County Data Centers Report
Fairfax County published a data centers report that includes water impact analysis.

**URL:** `https://www.fairfaxcounty.gov/planning-development/sites/planning-development/files/Assets/Documents/PDF/data-centers-report.pdf`

**Data status:** Not verified — URL not confirmed accessible. Attempt download before building a scraper.

**Sample prompt:**
> Download the Fairfax County data centers report PDF. Extract water usage estimates, zoning analysis, and infrastructure impact assessments. Useful as context for understanding data center water demand in the broader Northern Virginia region beyond Loudoun County.

### EIA Thermoelectric Cooling Water Data (Federal)
EIA Form 923 reports plant-level water withdrawal and consumption at power plants. While EIA doesn't track data centers directly, data centers drive ~4% of national electricity demand (projected 6.7-12% by 2028). Power plant cooling water data enables indirect water footprint calculations.

**URL:** `https://www.eia.gov/electricity/data/water/`

**Data status:** Confirmed. Final 2024 data was released September 18, 2025 and is downloadable from eia.gov. Covers all U.S. states (filter by VA or OH). Data includes generator type, fuel consumption, water consumption, cooling type, equipment status, and water source per plant. Next release (2025 early data) planned June 2026. Also available via data.gov and DOE OEDI.

**Sample prompt:**
> Build a module `extractors/eia_water.py` that downloads EIA thermoelectric cooling water spreadsheets (Form 923 data). Parse plant-level water withdrawal and consumption volumes for power plants in Virginia (PJM region) and Ohio. Cross-reference with data center electricity demand estimates to calculate the indirect water footprint (water used to generate electricity consumed by data centers). Store alongside direct water use data for a complete water footprint model.

### OCR for Scanned PDFs
Some government PDFs are scanned images with no text layer. pdfplumber and PyMuPDF return empty text for these.

**Sample prompt:**
> Add OCR support to the PDF extraction pipeline using pytesseract. When both pdfplumber and PyMuPDF return empty/minimal text from a PDF, fall back to OCR. Include preprocessing (deskewing, thresholding) for better accuracy on scanned government documents. Update requirements.txt and add tests with a sample scanned PDF.

### Dashboard / Visualization — Phase 2: Observable Framework
Phase 1 Streamlit dashboard is built (see `dashboard.py`). Phase 2 migrates the public-facing version to Observable Framework for static deployment, better data storytelling, and a scrollytelling landing page.

**UX research findings (March 2026):**
- California Drinking Water Tool: two-portal design (community vs. policy audience), GIS overlays with demographic data
- PJM LMP Map: contour heat map with 5-minute auto-refresh, brushable time selection
- EPA ECHO: Qlik-based cross-filtering, effluent charts with permit limit overlays
- WoodMac Lens: screen-on-map-then-benchmark workflow, scenario modeling
- Recommended tech: Observable Framework (static site, D3.js/deck.gl, pre-computed data from Python pipeline)

**Sample prompt:**
> Migrate the Streamlit dashboard to Observable Framework with a scrollytelling landing page (3-4 key findings with human-relatable comparisons like "equivalent to X households"), an interactive explorer with Leaflet map and cross-filtering, and facility detail pages with effluent-chart-style time series. Deploy as a static site on GitHub Pages. Use D3.js/Observable Plot for charts and deck.gl for the map.

---

## Low Priority

### USGS Water Use Data Integration (Federal)
USGS publishes county-level water use estimates every 5 years (latest: 2020). Too coarse for individual facility tracking but useful for regional trend analysis. The USWWD (United States Water Withdrawals Database) on CUAHSI HydroShare has facility-level data compiled from state reports (188,857 unique facilities).

**URLs:**
- NWIS: `https://waterdata.usgs.gov/nwis/wu`
- USWWD: `https://www.hydroshare.org/resource/11c91bde19864106a9e85b39ffcf0ff1/`
- New API: `https://api.waterdata.usgs.gov/`

**Data status:** Available but dated. Latest county-level data is 2020 (published every 5 years; next update expected 2025–2026). Coarse for individual facility tracking but useful for regional trend context.

**Sample prompt:**
> Build a module `extractors/usgs_water.py` that downloads USGS county-level water use estimates for Virginia and Ohio counties with data center clusters (Loudoun, Fairfax, Prince William, Franklin, Licking, Delaware). Parse the self-supplied industrial and public supply categories. Also check whether the USWWD HydroShare dataset includes facility-level records for Virginia and Ohio. Store as reference data for regional trend context.

### Email/Slack Notifications for New Documents
Alert when new documents are found on re-scrapes.

**Sample prompt:**
> Add a notification system that compares new scrape results against previous results and sends an alert (email via SMTP or Slack webhook) when new documents are found. Include the document title, source URL, and any extracted water metrics in the notification. Make the notification channel configurable in config.py.

### Scheduled Scraping via Cron/Airflow
Automate periodic re-scraping.

**Sample prompt:**
> Set up scheduled scraping using either cron jobs or Apache Airflow. Create a schedule that re-runs all scrapers weekly, with EPA ECHO DMR scraper running monthly (aligned with DMR reporting periods). Include error alerting if a scheduled run fails.

### Additional States (Option E)
Expand beyond Virginia and Ohio to other data center hub states. Texas, Oregon, and Georgia are the next biggest data center markets. Lower priority since the VA/OH pipeline isn't fully exploited yet, but this is the path to a national-scale dataset.

**Target states and agencies:**
- Texas: TCEQ permits, TCEQ ArcGIS data, PUC water availability studies
- Oregon: DEQ permits, Portland Water Bureau data
- Georgia: EPD permits, Atlanta watershed data

**Sample prompt:**
> Research and add scrapers for data center water usage documents in Texas (TCEQ permits), Oregon (DEQ), and Georgia (EPD). Follow the same architecture as existing scrapers — identify the relevant portals, determine the technology stack, and implement using the BaseScraper pattern. Start with Texas TCEQ which has the most accessible ArcGIS-based permit data.

### FOIA Request Templates (Option F)
Create template FOIA requests targeting local water utilities for facility-level data center water consumption records. This is the most direct path to facility-specific data, especially given the Botetourt County court ruling (2024) where a judge ruled water usage data is NOT proprietary.

**Key targets:**
- Loudoun Water — facility-level commercial/industrial water delivery records (highest priority — they sell ~1.6B gal/yr to data centers but only publish aggregate figures)
- Prince William Water — same approach, 56 data centers in the county
- Western Virginia Water Authority — Google data center water contract records (citing the Botetourt County court precedent)
- Fairfax Water — wholesale supply data to Loudoun Water (indirect metric)

**Legal context:**
- Virginia FOIA (Section 2.2-3700) requires disclosure unless exempt
- 25 of 31 Virginia localities with data centers have signed NDAs — FOIA can challenge these
- Botetourt County precedent (2024): water usage data is public record, NOT proprietary trade secret

**Sample prompt:**
> Create a `docs/foia_templates/` directory with template FOIA request letters for: (1) Loudoun Water — facility-level commercial/industrial water delivery records, (2) Prince William Water — same, (3) Western Virginia Water Authority — Google data center water contract records (citing the Botetourt County court precedent). Include guidance on Virginia FOIA law (Section 2.2-3700) and how to counter proprietary information exemption claims. Include sample follow-up templates if initial request is denied.

---

## Reference: Data Source Landscape

### Key findings from research (Feb 2026)

**Federal level:**
- EPA ECHO is the primary federal source for discharge data (DMR). No federal database tracks water *withdrawals* comprehensively — that's state-managed.
- EPA FRS cross-references facilities across 90+ databases by NAICS code — useful for discovering data center regulatory footprints.
- USGS data is county-level (too aggregated) except for the USWWD compilation.
- EIA tracks power plant cooling water, relevant for indirect water footprint calculations.

**Virginia:**
- Loudoun Water ACFRs and rate studies are the single best public source for aggregate data center water consumption (~1.6B gal/year in 2023, 250% increase from 2019).
- DEQ myDEQ portal has facility-level withdrawal data but requires account creation.
- 25 of 31 Virginia localities with data centers have signed NDAs complicating FOIA.
- SB 553 (2026) passed Senate 25-15 on Feb 9, 2026 — would mandate monthly reporting. Currently in House committee.
- VWP permits (ArcGIS layers 192/193) cover surface water withdrawals.

**Ohio:**
- Ohio EPA's draft General Permit OHD000001 for data center wastewater is a game-changer — will require DMR reporting for cooling water discharge. Public comment closed Jan 16, 2026; finalization pending.
- ODNR Water Withdrawal Facility Viewer has historical annual volumes by facility.
- Central Ohio Regional Water Study (March 2025) projects industrial water demand growing to >40 MGD by 2030, ~90 MGD by 2050. Intel's New Albany chip campus will need 6 MGD alone starting ~2030. Columbus building $1.6B fourth water treatment plant.
- New Albany/Licking County is the densest Ohio data center cluster (Google, Meta, Amazon).
