import structlog

logger = structlog.get_logger()


class PDFExtractor:
    """Extract text and tables from PDFs using pdfplumber (primary) and PyMuPDF (fallback)."""

    def extract_text(self, pdf_path: str) -> str:
        """Extract full text from PDF. Tries pdfplumber first, falls back to PyMuPDF."""
        text = self._extract_with_pdfplumber(pdf_path)
        if not text or len(text.strip()) < 50:
            logger.debug("pdfplumber_insufficient, trying_pymupdf", path=pdf_path)
            text = self._extract_with_pymupdf(pdf_path)
        if not text or len(text.strip()) < 50:
            logger.warning("pdf_extraction_empty", path=pdf_path)
        return text

    def extract_tables(self, pdf_path: str) -> list[list[list[str]]]:
        """Extract tables from PDF using pdfplumber.

        Returns list of tables, each table is a list of rows,
        each row is a list of cell strings.
        """
        tables = []
        try:
            import pdfplumber
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    page_tables = page.extract_tables()
                    if page_tables:
                        tables.extend(page_tables)
        except Exception as e:
            logger.error("table_extraction_failed", path=pdf_path, error=str(e))
        return tables

    def _extract_with_pdfplumber(self, pdf_path: str) -> str:
        try:
            import pdfplumber
            with pdfplumber.open(pdf_path) as pdf:
                pages_text = []
                for page in pdf.pages:
                    page_text = page.extract_text() or ""
                    pages_text.append(page_text)
                return "\n\n".join(pages_text)
        except Exception as e:
            logger.debug("pdfplumber_failed", path=pdf_path, error=str(e))
            return ""

    def _extract_with_pymupdf(self, pdf_path: str) -> str:
        try:
            import fitz
            doc = fitz.open(pdf_path)
            pages_text = []
            for page in doc:
                pages_text.append(page.get_text())
            doc.close()
            return "\n\n".join(pages_text)
        except Exception as e:
            logger.debug("pymupdf_failed", path=pdf_path, error=str(e))
            return ""
