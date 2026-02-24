"""Scraper for Loudoun Water's new Highbond community portal.

The portal at loudounwater.community.highbond.com may require authentication.
If public access is available, scrape board documents. Otherwise fall back
to the BoardDocs legacy scraper.
"""

from __future__ import annotations

from typing import AsyncGenerator, Optional
from urllib.parse import urljoin

from models.document import DocumentSource
from scrapers.base import BaseScraper
from utils.http_client import RateLimitedClient


class LoudounHighbondScraper(BaseScraper):

    PORTAL_URL = "https://loudounwater.community.highbond.com/portal/"

    @property
    def name(self) -> str:
        return "va_loudoun_highbond"

    @property
    def source(self) -> DocumentSource:
        return DocumentSource.VA_LOUDOUN_HIGHBOND

    async def discover(self, limit: int | None = None) -> AsyncGenerator[dict, None]:
        """Attempt to access the Highbond portal and find board documents."""
        if not self.browser:
            self.logger.error("browser_required_for_highbond")
            return

        page = await self.browser.new_page()
        count = 0

        try:
            response = await page.goto(
                self.PORTAL_URL,
                wait_until="networkidle",
                timeout=30000,
            )

            # Check if we got redirected to a login page
            current_url = page.url.lower()
            if any(term in current_url for term in ["login", "signin", "auth", "sso"]):
                self.logger.warning(
                    "highbond_requires_authentication",
                    url=current_url,
                    message="Portal requires login. Use BoardDocs legacy scraper instead.",
                )
                await page.close()
                return

            # Check for login forms on the page
            login_form = await page.query_selector(
                "form[action*='login'], input[type='password'], "
                ".login-form, #loginForm"
            )
            if login_form:
                self.logger.warning("highbond_login_form_detected")
                await page.close()
                return

            await page.wait_for_timeout(3000)

            # If we're in, look for documents
            # Highbond portals typically have document sections or libraries
            links = await page.query_selector_all("a[href]")

            for link in links:
                if limit and count >= limit:
                    break

                try:
                    href = await link.get_attribute("href")
                    text = (await link.inner_text()).strip()

                    if not href or not text or len(text) < 5:
                        continue

                    full_url = urljoin(self.PORTAL_URL, href)

                    # Look for board meeting documents
                    text_lower = text.lower()
                    relevant_terms = self.config.get("search_keywords", [])
                    matched_kw = next(
                        (kw for kw in relevant_terms if kw.lower() in text_lower),
                        None,
                    )

                    if matched_kw or href.endswith(".pdf"):
                        reason = f"link text matched: '{matched_kw}'" if matched_kw else "PDF link"
                        yield {
                            "title": text[:300],
                            "url": full_url,
                            "pdf_url": full_url if href.endswith(".pdf") else None,
                            "date": None,
                            "state": "VA",
                            "agency": "Loudoun Water",
                            "id": f"highbond-{count}",
                            "match_term": reason,
                        }
                        count += 1

                except Exception:
                    continue

        except Exception as e:
            self.logger.error("highbond_discovery_failed", error=str(e))
        finally:
            await page.close()

        if count == 0:
            self.logger.info("highbond_no_public_documents_found")

    async def fetch_document(self, metadata: dict) -> Optional[str]:
        """Download a PDF from the Highbond portal."""
        pdf_url = metadata.get("pdf_url")
        if not pdf_url:
            return None

        filename = pdf_url.split("/")[-1] or f"highbond_{metadata.get('id', 'unknown')}.pdf"
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
                self.logger.error("highbond_pdf_download_failed", url=pdf_url, error=str(e))
                return None
