"""Scraper for Virginia DEQ VPDES Public Notices page.

Uses Playwright to navigate the public notices page,
extract notice entries, and download linked PDF documents.
"""

from __future__ import annotations

from datetime import datetime
from typing import AsyncGenerator, Optional
from urllib.parse import urljoin

from models.document import DocumentSource
from scrapers.base import BaseScraper
from utils.http_client import RateLimitedClient


class DEQPublicNoticesScraper(BaseScraper):

    BASE_URL = "https://www.deq.virginia.gov"

    @property
    def name(self) -> str:
        return "va_deq_public_notices"

    @property
    def source(self) -> DocumentSource:
        return DocumentSource.VA_DEQ_PUBLIC_NOTICES

    async def discover(self, limit: int | None = None) -> AsyncGenerator[dict, None]:
        """Navigate the VPDES public notices page and yield notice metadata."""
        if not self.browser:
            self.logger.error("browser_required_for_public_notices")
            return

        page = await self.browser.new_page()
        count = 0

        try:
            await page.goto(
                self.config["va_deq_public_notices"],
                wait_until="networkidle",
                timeout=30000,
            )

            # The page lists public notices, typically in sections or a table.
            # Try multiple selector strategies since the page structure may vary.

            # Strategy 1: Look for article/section elements with notice content
            notices = await self._extract_notices_from_page(page)

            for notice in notices:
                if limit and count >= limit:
                    break

                # Check if notice is relevant to data centers
                title = notice.get("title", "").lower()
                keywords = self.config.get("search_keywords", [])
                relevant = any(kw.lower() in title for kw in keywords)

                # Also check facility name against known companies
                if not relevant:
                    known = [c.lower() for c in self.config.get("known_companies", [])]
                    relevant = any(c in title for c in known)

                # If title doesn't match, still yield — we'll check the PDF content later
                # But prioritize relevant ones
                notice["state"] = "VA"
                notice["agency"] = "Virginia DEQ"
                notice["id"] = f"public-notice-{count}-{notice.get('title', '')[:50]}"
                yield notice
                count += 1

        except Exception as e:
            self.logger.error("public_notices_discovery_failed", error=str(e))
        finally:
            await page.close()

    async def fetch_document(self, metadata: dict) -> Optional[str]:
        """Download PDF linked from a public notice."""
        pdf_url = metadata.get("pdf_url")
        if not pdf_url:
            return None

        filename = pdf_url.split("/")[-1] or f"notice_{metadata.get('id', 'unknown')}.pdf"
        dest_path = self.file_store.get_path("virginia", "deq", filename)

        if self.file_store.exists("virginia", "deq", filename):
            return dest_path

        async with RateLimitedClient(
            min_delay=self.config["min_delay"],
            max_delay=self.config["max_delay"],
        ) as client:
            try:
                await client.download_file(pdf_url, dest_path)
                return dest_path
            except Exception as e:
                self.logger.error("pdf_download_failed", url=pdf_url, error=str(e))
                return None

    async def _extract_notices_from_page(self, page) -> list[dict]:
        """Extract notice entries from the page DOM."""
        notices = []

        # Try finding links to permit documents / PDFs
        links = await page.query_selector_all("a[href]")

        for link in links:
            try:
                href = await link.get_attribute("href")
                text = (await link.inner_text()).strip()

                if not href or not text:
                    continue

                full_url = urljoin(self.BASE_URL, href)

                # Collect PDF links
                if href.lower().endswith(".pdf"):
                    notices.append({
                        "title": text,
                        "url": full_url,
                        "pdf_url": full_url,
                        "date": None,
                        "match_term": "PDF on DEQ public notices page",
                    })
                # Collect links that mention permits or water
                else:
                    search_terms = ["permit", "vpdes", "discharge", "water"]
                    matched_kw = next(
                        (kw for kw in search_terms if kw in text.lower()),
                        None,
                    )
                    if matched_kw:
                        notices.append({
                            "title": text,
                            "url": full_url,
                            "pdf_url": None,
                            "date": None,
                            "match_term": f"link text matched: '{matched_kw}'",
                        })
            except Exception:
                continue

        # Also try extracting structured content from the page body
        content_sections = await page.query_selector_all(
            ".field-content, .views-row, article, .node, .card, .list-item"
        )
        for section in content_sections:
            try:
                title_el = await section.query_selector("h2, h3, h4, .title, a")
                if title_el:
                    title = (await title_el.inner_text()).strip()
                    href_el = await section.query_selector("a[href*='.pdf']")
                    pdf_url = None
                    if href_el:
                        href = await href_el.get_attribute("href")
                        pdf_url = urljoin(self.BASE_URL, href)

                    link_el = await section.query_selector("a[href]")
                    url = ""
                    if link_el:
                        url = urljoin(self.BASE_URL, await link_el.get_attribute("href"))

                    if title and title not in [n.get("title") for n in notices]:
                        notices.append({
                            "title": title,
                            "url": url,
                            "pdf_url": pdf_url,
                            "date": None,
                        })
            except Exception:
                continue

        self.logger.info("notices_extracted", count=len(notices))
        return notices
