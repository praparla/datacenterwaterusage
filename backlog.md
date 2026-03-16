# Backlog

Items are ordered by priority (high / medium / low). Each includes a sample prompt for generating an implementation plan.

Last reviewed: 2026-02-24. Data status notes added after verifying live sources.

---

## Completed (March 2026)

### ✅ EPA ECHO NAICS Facility Discovery (Federal)
**Status**: Built — `scrapers/epa_echo_naics.py` (18 tests)

### ✅ Ohio EPA Data Center General Permit Tracker (OHD000001)
**Status**: Built — `scrapers/ohio/epa_general_permit.py` (20 tests)

### ✅ Loudoun Water ACFR Scraper (Virginia)
**Status**: Built — `scrapers/virginia/loudoun_acfr.py` (26 tests)

### ✅ Expand EPA ECHO DMR Target Permits
**Status**: Done — added Newark WWTP (OH0020257), SW Licking (OH0047627), Alexandria Renew (VA0025160), Noman Cole (VA0025364) to config.py

### ✅ Dashboard / Visualization
**Status**: Built — `dashboard.py` Streamlit dashboard with flow time series, permit limit overlays, seasonal heatmap, cross-filtering, data download (20 tests). Run with `streamlit run dashboard.py`.

---

## High Priority

### EPA ECHO NAICS Facility Discovery (Federal)
Use EPA ECHO API to find all data center facilities (NAICS 518210) with any CWA/NPDES regulatory footprint in Virginia and Ohio. This gives geographic coordinates to map facilities to WWTP service areas and builds a registry of known data center locations.

**API endpoint:** `https://ofmpub.epa.gov/echo/echo_rest_services.get_facilities?p_naic=518210&p_med=CWA&output=JSON`

**Data status:** API confirmed live. Caution: ECHO was last updated October 19, 2025, and has not been updated since due to federal funding; facility data may be stale. Bulk ICIS-NPDES downloads available as an alternative at echo.epa.gov/tools/data-downloads.

**Sample prompt:**
> Add a new scraper `scrapers/federal/epa_echo_naics.py` that queries the EPA ECHO REST API for all facilities with NAICS code 518210 (Data Processing/Hosting) in Virginia and Ohio. Extract facility name, FRS Registry ID, coordinates, permit numbers, and compliance status. Store results in the standard DocumentRecord format. Use the existing httpx client with rate limiting. This will serve as a facility registry to cross-reference against WWTP service areas.

### Ohio EPA Data Center General Permit Tracker (OHD000001)
Ohio EPA has drafted the first-ever general NPDES permit specifically for data center wastewater discharges (cooling tower blowdown, non-contact cooling water). Once finalized, data centers that obtain coverage will report flow volumes, pH, TDS, chlorine, temperature via DMR. This is a direct pipeline of data center water discharge data.

**Key documents:**
- Draft permit: `https://dam.assets.ohio.gov/image/upload/epa.ohio.gov/Portals/35/permits/Data_Centers/OHD000001_Draft.pdf`
- Fact sheet: `https://dam.assets.ohio.gov/image/upload/epa.ohio.gov/Portals/35/permits/Data_Centers/OHD000001_Draft.fs.pdf`
- General permits list: `https://www.epa.state.oh.us/dsw/permits/gplist`

**Data status:** Draft permit PDF confirmed available. Public hearing held December 17, 2025; public comment period closed January 16, 2026. Effective date is TBD — permit not yet finalized as of February 2026. Ohio hosts 200+ data centers (more than any other Great Lakes state). Once finalized, facilities must submit DMRs by the 20th of each month. NOI/facility coverage list will only be scrapeable post-finalization.

**Sample prompt:**
> Build a scraper `scrapers/ohio/epa_general_permit.py` that monitors the Ohio EPA general permits list for OHD000001 (data center wastewater). Once finalized, scrape the list of facilities that file Notices of Intent (NOIs) for coverage. Extract facility names, locations, and permit IDs. These facilities will then be added as targets for the existing `epa_echo_dmr.py` scraper to pull their discharge monitoring data. Also download and extract text from the draft permit PDF to document the monitoring parameters (flow, pH, TDS, chlorine, temperature, oil/grease, TSS).

### Loudoun Water ACFR Scraper (Virginia)
Loudoun Water's Annual Comprehensive Financial Reports contain statistical tables on water sales by category (residential, commercial, reclaimed). In 2023, Loudoun Water sold ~2.46 MGD potable + ~2.1 MGD reclaimed to data centers, totaling ~1.6 billion gallons/year (250% increase from 2019). This is the best publicly available aggregate data on data center water consumption.

**URL:** `https://www.loudounwater.org/about/comprehensive-annual-financial-reports`

**Data status:** Confirmed. 2023 ACFR PDF directly downloadable. Data centers consumed 899 million gallons of potable water in 2023 (up 250% in 4 years) plus ~736 million gallons of reclaimed water in 2024 — totaling ~1.6 billion gallons/year. Data center alley currently consumes ~2% of Potomac River Basin water, rising to 8% in summer; projected 33% by 2050. Reports available for 2020–2024.

**Sample prompt:**
> Build a scraper `scrapers/virginia/loudoun_acfr.py` that downloads Loudoun Water ACFR PDFs from their website. Use pdfplumber to extract statistical tables on water sales volume by customer class (residential, commercial, reclaimed). Parse out annual water delivery volumes, number of connections by type, and reclaimed water program statistics. Store time-series data in results with source attribution. Also scrape the reclaimed water program page at `https://www.loudounwater.org/commercial-customers/reclaimed-water-program` for current delivery volumes.

### Prince William Water Industrial User Survey (Virginia)
The March 2024 Industrial User Survey (IUS) lists industrial customers including data centers, with ERU (Equivalent Residential Unit) capacity allocations. Data centers consumed ~2.7% of average daily demand and ~5.3% of max daily demand in 2024. Prince William County has 56 data centers.

**URL:** `https://princewilliamwater.org/sites/default/files/IUS_March%202024.pdf`

**Data status:** Confirmed. PDF publicly available. 56 data centers in Prince William County; data centers consumed 2.7% of average daily demand and 5.3% of max daily demand in 2024. Each ERU = 400 gallons/day max (10,000 gallons/month). High Demand Charges apply above ERU thresholds.

**Sample prompt:**
> Build a scraper `scrapers/virginia/pwc_ius.py` that downloads the Prince William Water Industrial User Survey PDF. Extract the list of industrial customers (names, ERU allocations, water usage categories). Cross-reference with known data center operators. Also scrape the commercial customer page at `https://princewilliamwater.org/our-customers/commercial-customers` for rate structure and industrial customer definitions. Each ERU = 10,000 gallons/month capacity.

### Virginia DEQ Water Withdrawal Reporting (Virginia)
Virginia requires annual reporting of all withdrawals >10,000 GPD. In 2023, 1,174 facilities reported. Data centers that have their own wells would appear here. Municipal utilities serving data centers (Loudoun Water, Fairfax Water) report their aggregate withdrawals.

**Access:**
- myDEQ Portal (requires account): `https://portal.deq.virginia.gov/`
- VWP permits via ArcGIS: Layer 192 (individual) and Layer 193 (general) on DEQ EDMA MapServer
- Annual Water Resources Report: `https://www.deq.virginia.gov/get-involved/about-us/deq-reports`

**Data status:** Partial. ArcGIS VWP layers 192 and 193 are publicly queryable (no account needed). Facility-level withdrawal data on myDEQ portal requires account creation. Annual Water Resources Report PDFs are publicly available.

**Sample prompt:**
> Add VWP permit layers to the existing `scrapers/virginia/deq_arcgis.py` scraper. Query ArcGIS EDMA MapServer layers 192 (VWP Individual Permits) and 193 (VWP General Permits) for water withdrawal permits in Northern Virginia counties (Loudoun, Fairfax, Prince William). Extract permittee names, authorized withdrawal amounts, water sources, and permit status. Also build a PDF downloader for the DEQ Annual Water Resources Report to extract statewide withdrawal summaries.

### ODNR Water Withdrawal Facility Viewer (Ohio)
Ohio requires registration for facilities withdrawing >100,000 GPD. The ODNR ArcGIS-based facility viewer shows historical annual withdrawal volumes per facility. While 97% of data centers use municipal water, this captures the municipal suppliers and any self-supplied facilities.

**URL:** `https://experience.arcgis.com/experience/0605c2eaf8fe458481ac323404b4ab36/page/Water-Withdrawal-Facility-Viewer`

**Data status:** Confirmed live. ArcGIS Experience Builder viewer is active at the stated URL. Displays locations and historical annual water use data for all Ohio facilities withdrawing >100K GPD. A dedicated data request page is also available at the same experience (`/page/Water-Withdrawal-Facility-Data-Request`) for bulk export. Covers Franklin, Licking, and Delaware counties (central Ohio data center cluster).

**Sample prompt:**
> Build a scraper `scrapers/ohio/odnr_water_withdrawal.py` that queries the ArcGIS FeatureServer backing the ODNR Water Withdrawal Facility Viewer. Find the REST endpoint by inspecting the Experience Builder app's network requests. Query for all registered withdrawal facilities in Franklin County, Licking County, and Delaware County (major Ohio data center areas). Extract facility name, withdrawal volumes by year, water source type, and return flow data. Focus on public water systems serving New Albany, Columbus, and Newark areas.

### Ohio EPA ArcGIS NPDES Permits (Ohio)
Ohio EPA publishes NPDES permit data on ArcGIS Open Data, updated nightly. Can search by SIC code 7374 (data centers) to find facilities with discharge permits.

**URLs:**
- Individual permits: `https://data-oepa.opendata.arcgis.com/datasets/npdes-individual-permits`
- Industrial stormwater: `https://hub.arcgis.com/datasets/oepa::npdes-industrial-storm-water-permits`
- By county: `http://wwwapp.epa.ohio.gov/dsw/permits/permit_list.php`

**Data status:** Confirmed live. Ohio EPA Open Data portal at data-oepa.opendata.arcgis.com hosts the NPDES Individual Permits dataset (feature service ID `1118cdc038884214ba79a0712b60ece7_0`), updated nightly. OHD000001 draft permit confirms SIC 7374 is the applicable code for data center facilities.

**Sample prompt:**
> Build a scraper `scrapers/ohio/epa_npdes_arcgis.py` that queries the Ohio EPA ArcGIS Open Data NPDES Individual Permits dataset (feature service ID `1118cdc038884214ba79a0712b60ece7_0`). Filter for SIC code 7374 and/or facilities in Franklin, Licking, and Delaware counties. Extract permit numbers, permittee names, locations, discharge limits, and SIC codes. Also query the Industrial Storm Water Permits layer to identify data centers with stormwater-only permits. Follow the same pattern as `scrapers/virginia/deq_arcgis.py`.

### Expand EPA ECHO DMR Target Permits
Add more receiving wastewater treatment plants to `config.py` to capture flow data from areas with heavy data center presence.

**New targets to add:**
- Columbus Southerly WWTP (OH0024651) — receives industrial discharge from Columbus area
- Jackson Pike WWTP (OH0028061) — Columbus area
- Newark WWTP — Licking County, near New Albany data center cluster
- Southwest Licking Community WWTP — directly serves New Albany data centers
- Additional Loudoun County plants beyond Broad Run

**Data status:** Internal config change — no external URL to verify. ECHO permit lookup is available but has been stale since October 2025 (federal funding issue). Permit numbers for Columbus Southerly and Jackson Pike are listed above; Newark and Southwest Licking permit numbers need verification via ECHO facility search or Ohio EPA permit list.

**Sample prompt:**
> Research and identify the NPDES permit numbers for wastewater treatment plants receiving discharge from data center areas in Ohio (Columbus Southerly, Jackson Pike, Newark, Southwest Licking) and additional Virginia plants beyond Broad Run. Add these permits to `config.py` under `epa_echo_target_permits`. Run the existing `epa_echo_dmr.py` scraper against the expanded target list to collect flow data. Verify the permit numbers via EPA ECHO facility search.

### Central Ohio Regional Water Study Analysis (elevated from Medium)
The March 2025 Central Ohio Regional Water Study quantifies data center water demand: industrial demand projected to grow from negligible (2020) to 40 MGD (2030), 70 MGD (2040), 90 MGD (2050). Key context documents with major news coverage.

**URLs:**
- Overview: `https://dam.assets.ohio.gov/image/upload/epa.ohio.gov/Portals/0/water/CentralWaterStudyOverview.pdf`
- Licking County detail: `https://dam.assets.ohio.gov/image/upload/epa.ohio.gov/Portals/0/water/LickingWaterStudy.pdf`
- Pickaway County detail: `https://dam.assets.ohio.gov/image/upload/epa.ohio.gov/Portals/0/water/PickawayWaterStudy.pdf`

**Data status:** Confirmed. PDFs directly available at stated URLs. 15-county study released March 2025 (and covered extensively by US News, CBS, NBC4, 614Now). Projects industrial water demand (data centers + Intel chip plant) growing to >40 MGD by 2030, ~70 MGD by 2040, ~90 MGD by 2050 — a 120% increase 2021–2050. Columbus is building a $1.6B fourth water treatment plant partly to meet data center/Intel demand. Intel committed to 6 MGD for New Albany campus starting ~2030. Drought conditions in Ohio (driest August on record in 2025) add urgency.

**Sample prompt:**
> Download and extract key data from the Central Ohio Regional Water Study PDFs. Parse the demand projections table (industrial water demand by decade through 2050), current capacity figures, and infrastructure recommendations. Store as reference data in `data/output/reference/` to contextualize the scraper pipeline's flow data against regional projections.

### Fairfax Water Financial Reports (elevated from Medium, Virginia)
Fairfax Water is the upstream wholesale supplier to Loudoun Water (~18 MGD) and Prince William Water. Changes in wholesale demand reflect regional growth including data centers. Average daily production: 170 MGD, max capacity 375 MGD.

**URL:** `https://www.fairfaxwater.org/about-us` (financial reports section); 2024 report directly at `https://www.fairfaxwater.org/sites/default/files/about_us/2024%20Financial%20Report.pdf`

**Data status:** Confirmed. 2024 Financial Report PDF is publicly available. Key 2024 data: operating revenues increased 3.7% to $226.8 million; retail water sales up 4.8%; wholesale water sales up 2.8%; Loudoun Water purchases ~18 MGD from Fairfax Water (wholesale cost rose 42% from 2021–2024); wholesale customers account for ~47% of total water sales volume. Reports available for multiple years.

**Sample prompt:**
> Build a scraper `scrapers/virginia/fairfax_water.py` that downloads Fairfax Water's annual financial reports (Popular Annual Financial Report and Comprehensive Financial Report). Extract total water production volumes, wholesale delivery volumes to Loudoun Water and Prince William Water, revenue by customer class, and number of service connections. Track year-over-year trends in wholesale demand as a proxy for data center area growth.

---

## Medium Priority

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

### Deduplication Engine
As we scrape multiple portals, the same document or data point may appear from different sources (e.g., a permit referenced in both DEQ and Loudoun Water minutes).

**Sample prompt:**
> Build a deduplication module that identifies duplicate or near-duplicate records in the output CSV/JSON. Match on permit_number, source_url, and fuzzy-match on document_title. When duplicates are found, merge them into a single record keeping the most complete data from each source. Add a `sources` field that lists all portals where the record was found.

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

See `backlog.md` for detailed scraper plans and sample prompts for each source.
