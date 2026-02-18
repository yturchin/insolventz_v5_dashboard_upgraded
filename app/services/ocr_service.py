from __future__ import annotations

from pathlib import Path
from typing import Callable, Optional

from app.core.config import settings


class OCRDependencyError(RuntimeError):
    pass


def ocr_pdf_to_text(
    pdf_path: Path,
    *,
    lang: str = "deu+eng",
    dpi: int = 200,
    on_progress: Optional[Callable[[int, int], None]] = None,
) -> str:
    """Convert an image-based PDF to text using Poppler (pdf2image) + Tesseract.

    on_progress(current_page, total_pages) is called after each page.
    """

    try:
        from pdf2image import convert_from_path
    except Exception as e:  # pragma: no cover
        raise OCRDependencyError("pdf2image is not installed. Install requirements.txt") from e

    try:
        import pytesseract
    except Exception as e:  # pragma: no cover
        raise OCRDependencyError("pytesseract is not installed. Install requirements.txt") from e

    if settings.tesseract_cmd:
        pytesseract.pytesseract.tesseract_cmd = settings.tesseract_cmd

    try:
        pages = convert_from_path(str(pdf_path), dpi=dpi)
    except Exception as e:
        # Most common on Windows: Poppler missing
        raise OCRDependencyError(
            "Failed to render PDF pages. Poppler is required for pdf2image. "
            "On Windows: install Poppler and add its /bin to PATH."
        ) from e

    total = len(pages)
    out_chunks: list[str] = []
    for i, img in enumerate(pages, start=1):
        try:
            text = pytesseract.image_to_string(img, lang=lang)
        except Exception as e:
            raise OCRDependencyError(
                "Tesseract OCR failed. Ensure Tesseract is installed and available on PATH. "
                "Also install the language pack (deu) if using German statements."
            ) from e
        out_chunks.append(text)
        if on_progress:
            on_progress(i, total)

    return "\n".join(out_chunks)
