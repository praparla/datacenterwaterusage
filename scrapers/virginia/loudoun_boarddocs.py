"""Scraper for Loudoun Water Board of Directors meeting minutes via BoardDocs Pro.

BoardDocs uses a Lotus Notes/Domino backend (Board.nsf) with heavy JavaScript rendering.
Requires Playwright for navigation and interaction.
"""

from __future__ import annotations

from datetime import datetime
from typing import AsyncGenerator, Optional
from urllib.parse import urljoin

from models.document import DocumentSource
from scrapers.base import BaseScraper
from utils.http_client import RateLimitedClient


class LoudounBoardDocsScraper(BaseScraper):

    BASE_URL = "https://go.boarddocs.com/va/lwva/Board.nsf"

    @property
    def name(self) -> str:
        return "va_loudoun_boarddocs"

    @property
    def source(self) -> DocumentSource:
        return DocumentSource.VA_LOUDOUN_BOARDDOCS

    async def discover(self, limit: int | None = None) -> AsyncGenerator[dict, None]:
        """Navigate BoardDocs, iterate meetings, find relevant agenda items."""
        if not self.browser:
            self.logger.error("browser_required_for_boarddocs")
            return

        page = await self.browser.new_page()
        count = 0

        try:
            await page.goto(
                f"{self.BASE_URL}/Public",
                wait_until="networkidle",
                timeout=45000,
            )

            # BoardDocs loads dynamically. Wait for the meeting list to appear.
            await page.wait_for_timeout(5000)

            # Try clicking the "MEETINGS" tab to see the meeting list
            meetings_tab = await page.query_selector(
                "a[data-name='meetings'], a:has-text('MEETINGS'), "
                "[id*='meeting'], .tab:has-text('Meeting')"
            )
            if meetings_tab:
                await meetings_tab.click()
                await page.wait_for_timeout(3000)

            # Extract meeting entries from the list
            meetings = await self._extract_meeting_list(page)
            self.logger.info("meetings_found", count=len(meetings))

            for meeting in meetings:
                if limit and count >= limit:
                    break

                # Navigate to each meeting and check agenda items
                meeting_url = meeting.get("url", "")
                if meeting_url:
                    agenda_items = await self._get_meeting_agenda_items(page, meeting_url)

                    for item in agenda_items:
                        if limit and count >= limit:
                            break

                        # Check relevance of agenda item
                        item_title = item.get("title", "").lower()
                        search_terms = self.config.get("search_keywords", [])
                        matched_kw = next(
                            (kw for kw in search_terms if kw.lower() in item_title),
                            None,
                        )

                        if matched_kw or item.get("has_pdf"):
                            reason = f"agenda item matched: '{matched_kw}'" if matched_kw else "PDF attachment"
                            meta = {
                                "title": f"{meeting.get('title', 'Meeting')} - {item.get('title', '')}",
                                "url": meeting_url,
                                "pdf_url": item.get("pdf_url"),
                                "date": meeting.get("date"),
                                "state": "VA",
                                "agency": "Loudoun Water",
                                "id": f"boarddocs-{meeting.get('id', '')}-{item.get('id', count)}",
                                "match_term": reason,
                            }
                            yield meta
                            count += 1

        except Exception as e:
            self.logger.error("boarddocs_discovery_failed", error=str(e))
        finally:
            await page.close()

    async def fetch_document(self, metadata: dict) -> Optional[str]:
        """Download a PDF attachment from a BoardDocs meeting item."""
        pdf_url = metadata.get("pdf_url")
        if not pdf_url:
            return None

        filename = pdf_url.split("/")[-1] or f"boarddocs_{metadata.get('id', 'unknown')}.pdf"
        dest_path = self.file_store.get_path("virginia", "loudoun", filename)

        if self.file_store.exists("virginia", "loudoun", filename):
            return dest_path

        async with RateLimitedClient(
            min_delay=self.config["min_delay"],
            max_delay=self.config["max_delay"],
        ) as client:
            try:
                await client.download_file(pdf_url, dest_path)
                return dest_path
            except Exception as e:
                self.logger.error("boarddocs_pdf_download_failed", url=pdf_url, error=str(e))
                return None

    async def _extract_meeting_list(self, page) -> list[dict]:
        """Extract meeting entries from the BoardDocs meeting list."""
        meetings = []

        # BoardDocs typically renders meetings as clickable list items
        selectors = [
            ".meeting-list-item", ".meeting-row", "tr.meeting",
            "[class*='meeting']", "div[onclick*='meeting']",
            "li a", ".list-group-item",
        ]

        elements = []
        for selector in selectors:
            elements = await page.query_selector_all(selector)
            if elements:
                break

        # If no specific meeting elements found, try all links
        if not elements:
            elements = await page.query_selector_all("a[href*='goto'], a[href*='Board.nsf']")

        for i, el in enumerate(elements[:50]):  # Cap at 50 meetings
            try:
                text = (await el.inner_text()).strip()
                href = await el.get_attribute("href") if await el.get_attribute("href") else ""

                if text and len(text) > 5:
                    url = urljoin(self.BASE_URL, href) if href else ""
                    meetings.append({
                        "id": f"meeting-{i}",
                        "title": text[:200],
                        "url": url,
                        "date": self._parse_date_from_text(text),
                    })
            except Exception:
                continue

        return meetings

    async def _get_meeting_agenda_items(self, page, meeting_url: str) -> list[dict]:
        """Navigate to a meeting page and extract agenda items with attachments."""
        items = []

        try:
            await page.goto(meeting_url, wait_until="networkidle", timeout=30000)
            await page.wait_for_timeout(3000)

            # Look for agenda items
            agenda_elements = await page.query_selector_all(
                ".agenda-item, .item-row, tr.item, [class*='agenda'], "
                ".category-item, li.item"
            )

            # Also find all PDF links on the page
            pdf_links = await page.query_selector_all("a[href*='.pdf']")
            pdf_urls = []
            for pdf_link in pdf_links:
                href = await pdf_link.get_attribute("href")
                if href:
                    pdf_urls.append(urljoin(self.BASE_URL, href))

            if agenda_elements:
                for i, el in enumerate(agenda_elements):
                    text = (await el.inner_text()).strip()
                    # Check for PDF link within this element
                    pdf_el = await el.query_selector("a[href*='.pdf']")
                    pdf_url = None
                    if pdf_el:
                        href = await pdf_el.get_attribute("href")
                        pdf_url = urljoin(self.BASE_URL, href)

                    items.append({
                        "id": f"item-{i}",
                        "title": text[:300],
                        "pdf_url": pdf_url,
                        "has_pdf": pdf_url is not None,
                    })
            elif pdf_urls:
                # No agenda structure found, but PDFs are available
                for i, url in enumerate(pdf_urls):
                    items.append({
                        "id": f"pdf-{i}",
                        "title": url.split("/")[-1],
                        "pdf_url": url,
                        "has_pdf": True,
                    })

        except Exception as e:
            self.logger.error("agenda_extraction_failed", url=meeting_url, error=str(e))

        return items

    def _parse_date_from_text(self, text: str) -> Optional[datetime]:
        """Try to extract a date from meeting title text."""
        import re

        # Common patterns: "January 12, 2025", "01/12/2025", "2025-01-12"
        patterns = [
            (r'(\w+)\s+(\d{1,2}),?\s+(\d{4})', "%B %d %Y"),
            (r'(\d{1,2})/(\d{1,2})/(\d{4})', None),
            (r'(\d{4})-(\d{2})-(\d{2})', None),
        ]

        for pattern, fmt in patterns:
            match = re.search(pattern, text)
            if match:
                try:
                    if fmt:
                        date_str = f"{match.group(1)} {match.group(2)} {match.group(3)}"
                        return datetime.strptime(date_str, fmt)
                    elif "/" in match.group(0):
                        return datetime(
                            int(match.group(3)),
                            int(match.group(1)),
                            int(match.group(2)),
                        )
                    else:
                        return datetime(
                            int(match.group(1)),
                            int(match.group(2)),
                            int(match.group(3)),
                        )
                except (ValueError, TypeError):
                    continue
        return None
