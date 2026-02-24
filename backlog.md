# Backlog

Items are ordered by priority (high / medium / low). Each includes a sample prompt for generating an implementation plan.

---

## High Priority

*(None yet — will be populated as development progresses)*

---

## Medium Priority

### OCR for Scanned PDFs
Some government PDFs are scanned images with no text layer. pdfplumber and PyMuPDF return empty text for these.

**Sample prompt:**
> Add OCR support to the PDF extraction pipeline using pytesseract. When both pdfplumber and PyMuPDF return empty/minimal text from a PDF, fall back to OCR. Include preprocessing (deskewing, thresholding) for better accuracy on scanned government documents. Update requirements.txt and add tests with a sample scanned PDF.

### Deduplication Engine
As we scrape multiple portals, the same document or data point may appear from different sources (e.g., a permit referenced in both DEQ and Loudoun Water minutes).

**Sample prompt:**
> Build a deduplication module that identifies duplicate or near-duplicate records in the output CSV/JSON. Match on permit_number, source_url, and fuzzy-match on document_title. When duplicates are found, merge them into a single record keeping the most complete data from each source. Add a `sources` field that lists all portals where the record was found.

### Dashboard / Visualization
Build a simple web UI or notebook to visualize the extracted data.

**Sample prompt:**
> Create a Streamlit dashboard that reads the results.csv and displays: (1) a map of data center locations with water usage markers, (2) a table of all records sortable/filterable by state, company, and water volume, (3) a timeline of permits and agreements. Use plotly for charts.

---

## Low Priority

### Email/Slack Notifications for New Documents
Alert when new documents are found on re-scrapes.

**Sample prompt:**
> Add a notification system that compares new scrape results against previous results and sends an alert (email via SMTP or Slack webhook) when new documents are found. Include the document title, source URL, and any extracted water metrics in the notification. Make the notification channel configurable in config.py.

### Scheduled Scraping via Cron/Airflow
Automate periodic re-scraping.

**Sample prompt:**
> Set up scheduled scraping using either cron jobs or Apache Airflow. Create a schedule that re-runs all scrapers weekly, with Virginia DEQ scrapers running more frequently (daily) since they update most often. Include error alerting if a scheduled run fails.

### Additional States
Expand beyond Virginia and Ohio to other data center hub states.

**Sample prompt:**
> Research and add scrapers for data center water usage documents in Texas (TCEQ permits), Oregon (DEQ), and Georgia (EPD). Follow the same architecture as existing scrapers — identify the relevant portals, determine the technology stack, and implement using the BaseScraper pattern.
