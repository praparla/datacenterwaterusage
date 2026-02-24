"""CLI entry point for the Data Center Water Use Tracker pipeline.

Usage:
    python main.py --scraper va_deq_excel          # Run a single scraper
    python main.py --all-va                         # Run all Virginia scrapers
    python main.py --all-oh                         # Run all Ohio scrapers
    python main.py --all                            # Run everything
    python main.py --scraper va_deq_excel --limit 5 # Test with 5 docs
"""

import asyncio
import importlib

import click
import structlog

from config import CONFIG
from extractors.entity_extractor import EntityExtractor
from extractors.keyword_matcher import KeywordMatcher
from extractors.pdf_extractor import PDFExtractor
from storage.csv_writer import CSVWriter
from storage.json_writer import JSONWriter
from storage.state_manager import StateManager
from storage.file_store import FileStore
from utils.browser import BrowserManager
from utils.logging_config import setup_logging

# Scraper registry: key -> (module_path, class_name)
SCRAPERS = {
    # Virginia
    "va_deq_excel": ("scrapers.virginia.deq_vpdes_excel", "DEQVPDESExcelScraper"),
    "va_deq_arcgis": ("scrapers.virginia.deq_arcgis", "DEQArcGISScraper"),
    "va_deq_notices": ("scrapers.virginia.deq_public_notices", "DEQPublicNoticesScraper"),
    "va_deq_peep": ("scrapers.virginia.deq_peep_tableau", "DEQPEEPScraper"),
    "va_loudoun_boarddocs": ("scrapers.virginia.loudoun_boarddocs", "LoudounBoardDocsScraper"),
    "va_loudoun_highbond": ("scrapers.virginia.loudoun_highbond", "LoudounHighbondScraper"),
    "va_pwc": ("scrapers.virginia.pwc_eservices", "PWCEServicesScraper"),
    # Ohio
    "oh_epa": ("scrapers.ohio.epa_edocument", "OhioEPAScraper"),
    "oh_columbus_legistar": ("scrapers.ohio.columbus_legistar", "ColumbusLegistarScraper"),
    "oh_columbus_utilities": ("scrapers.ohio.columbus_utilities", "ColumbusUtilitiesScraper"),
    "oh_new_albany": ("scrapers.ohio.new_albany", "NewAlbanyScraper"),
}

VA_SCRAPERS = [k for k in SCRAPERS if k.startswith("va_")]
OH_SCRAPERS = [k for k in SCRAPERS if k.startswith("oh_")]


def _load_scraper_class(module_path: str, class_name: str):
    mod = importlib.import_module(module_path)
    return getattr(mod, class_name)


def _resolve_scrapers(scraper_names, all_va, all_oh, run_all) -> list[str]:
    if run_all:
        return list(SCRAPERS.keys())
    to_run = list(scraper_names) if scraper_names else []
    if all_va:
        to_run.extend(VA_SCRAPERS)
    if all_oh:
        to_run.extend(OH_SCRAPERS)
    # Deduplicate while preserving order
    seen = set()
    result = []
    for s in to_run:
        if s not in seen:
            seen.add(s)
            result.append(s)
    return result


async def run_pipeline(scraper_keys: list[str], limit: int | None, headless: bool):
    logger = structlog.get_logger()

    # Initialize infrastructure
    state_mgr = StateManager(CONFIG["state_db_path"])
    await state_mgr.initialize()
    file_store = FileStore(CONFIG["downloads_dir"])
    csv_writer = CSVWriter(CONFIG["csv_output_path"])
    json_writer = JSONWriter(CONFIG["json_output_path"])
    pdf_extractor = PDFExtractor()
    keyword_matcher = KeywordMatcher()
    entity_extractor = EntityExtractor()

    all_records = []

    async with BrowserManager(headless=headless) as browser:
        for scraper_key in scraper_keys:
            if scraper_key not in SCRAPERS:
                logger.warning("unknown_scraper", key=scraper_key)
                continue

            module_path, class_name = SCRAPERS[scraper_key]
            logger.info("running_scraper", scraper=scraper_key)

            try:
                scraper_cls = _load_scraper_class(module_path, class_name)
                scraper = scraper_cls(CONFIG, state_mgr, file_store, browser)
                records = await scraper.run(limit=limit)
            except Exception as e:
                logger.error("scraper_failed", scraper=scraper_key, error=str(e))
                continue

            # Post-processing: extract text from PDFs and enrich records
            for rec in records:
                try:
                    if rec.local_file_path and rec.local_file_path.endswith(".pdf"):
                        text = pdf_extractor.extract_text(rec.local_file_path)
                        matches = keyword_matcher.find_matches(text)
                        rec.keyword_matches = keyword_matcher.get_all_matched_keywords(matches)
                        rec.relevance_score = keyword_matcher.compute_relevance_score(matches)

                        metrics = entity_extractor.extract_water_metrics(text)
                        rec.extracted_water_metric = "; ".join(metrics) if metrics else None

                        companies = entity_extractor.extract_company_names(
                            text, CONFIG.get("known_companies", [])
                        )
                        rec.company_llc_name = "; ".join(companies) if companies else rec.company_llc_name

                        # Context quote around first data_center keyword match
                        if "data_center" in matches and matches["data_center"]:
                            rec.extracted_quote = entity_extractor.extract_surrounding_context(
                                text, matches["data_center"][0]
                            )

                    await state_mgr.mark_processed(scraper.name, rec.source_url or rec.document_title)

                except Exception as e:
                    logger.error(
                        "extraction_failed",
                        doc=rec.document_title,
                        error=str(e),
                    )

            all_records.extend(records)
            logger.info("scraper_complete", scraper=scraper_key, records=len(records))

    # Write output
    if all_records:
        csv_writer.write(all_records)
        json_writer.write(all_records)
        logger.info("output_written", total_records=len(all_records))
    else:
        logger.info("no_records_to_write")


@click.command()
@click.option("--scraper", "-s", multiple=True, help="Scraper(s) to run by key name")
@click.option("--all-va", is_flag=True, help="Run all Virginia scrapers")
@click.option("--all-oh", is_flag=True, help="Run all Ohio scrapers")
@click.option("--all", "run_all", is_flag=True, help="Run all scrapers")
@click.option("--limit", "-l", type=int, default=None, help="Max documents per scraper (for testing)")
@click.option("--headless/--no-headless", default=True, help="Run browser in headless mode")
@click.option("--log-level", default="INFO", help="Log level (DEBUG, INFO, WARNING, ERROR)")
def main(scraper, all_va, all_oh, run_all, limit, headless, log_level):
    """Data Center Water Use Tracker — scraping pipeline CLI."""
    setup_logging(log_level)
    logger = structlog.get_logger()

    to_run = _resolve_scrapers(scraper, all_va, all_oh, run_all)

    if not to_run:
        logger.error("no_scrapers_selected")
        click.echo("No scrapers selected. Use --scraper, --all-va, --all-oh, or --all.")
        click.echo(f"Available scrapers: {', '.join(SCRAPERS.keys())}")
        return

    logger.info("pipeline_starting", scrapers=to_run, limit=limit, headless=headless)
    asyncio.run(run_pipeline(to_run, limit, headless))
    logger.info("pipeline_complete")


if __name__ == "__main__":
    main()
