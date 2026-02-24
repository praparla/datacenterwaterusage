import structlog

logger = structlog.get_logger()


class ExcelExtractor:
    """Parse Excel files using openpyxl."""

    def extract_rows(self, excel_path: str) -> list[dict]:
        """Read an Excel file and return rows as list of dicts (column header -> value).

        Uses the first row as headers.
        """
        try:
            import openpyxl
            wb = openpyxl.load_workbook(excel_path, read_only=True, data_only=True)
            ws = wb.active

            rows = list(ws.iter_rows(values_only=True))
            if len(rows) < 2:
                logger.warning("excel_too_few_rows", path=excel_path, rows=len(rows))
                return []

            headers = [str(h).strip() if h else f"col_{i}" for i, h in enumerate(rows[0])]
            result = []
            for row in rows[1:]:
                row_dict = {}
                for i, val in enumerate(row):
                    if i < len(headers):
                        row_dict[headers[i]] = val
                result.append(row_dict)

            wb.close()
            logger.info("excel_extracted", path=excel_path, rows=len(result))
            return result

        except Exception as e:
            logger.error("excel_extraction_failed", path=excel_path, error=str(e))
            return []
