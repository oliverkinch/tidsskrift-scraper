#!/usr/bin/env python3
"""
Filter downloaded PDFs to keep only those with Danish main text.
Copies matching PDFs to filtered/{journal}/ preserving directory structure.

langdetect often confuses Danish and Norwegian (very similar languages),
so both 'da' and 'no' are accepted.

Usage:
    python filter_danish.py
"""

import logging
import shutil
import warnings
from pathlib import Path

import pypdf

logging.getLogger("pypdf").setLevel(logging.ERROR)
warnings.filterwarnings("ignore")
from langdetect import detect, LangDetectException

DOWNLOADS_DIR = Path(__file__).parent / "downloads"
FILTERED_DIR = Path(__file__).parent / "filtered"

ACCEPTED_LANGS = {"da"}

# Minimum characters of extracted text needed to make a reliable detection.
# PDFs with less text than this are skipped (likely scanned or near-empty).
MIN_CHARS = 300

# Number of pages to sample for language detection (first N pages)
PAGES_TO_SAMPLE = 4


def extract_text(pdf_path: Path) -> str:
    try:
        reader = pypdf.PdfReader(pdf_path)
        pages = reader.pages[:PAGES_TO_SAMPLE]
        return " ".join((p.extract_text() or "") for p in pages).strip()
    except Exception:
        return ""


def detect_lang(text: str) -> str | None:
    try:
        return detect(text)
    except LangDetectException:
        return None


def main():
    pdfs = sorted(DOWNLOADS_DIR.rglob("*.pdf"))
    # Exclude the test folder from the uni-portals check run
    pdfs = [p for p in pdfs if "test" not in p.parts]
    print(f"Total PDFs: {len(pdfs)}")

    counts = {"kept": 0, "rejected": 0, "too_short": 0, "error": 0}
    rejected_langs: dict[str, int] = {}

    for pdf in pdfs:
        text = extract_text(pdf)

        if len(text) < MIN_CHARS:
            counts["too_short"] += 1
            continue

        lang = detect_lang(text)
        if lang is None:
            counts["error"] += 1
            continue

        if lang in ACCEPTED_LANGS:
            dest = FILTERED_DIR / pdf.relative_to(DOWNLOADS_DIR)
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(pdf, dest)
            counts["kept"] += 1
        else:
            counts["rejected"] += 1
            rejected_langs[lang] = rejected_langs.get(lang, 0) + 1

    print(f"\nResults:")
    print(f"  Kept (da/no):  {counts['kept']}")
    print(f"  Rejected:      {counts['rejected']}")
    print(f"  Too short:     {counts['too_short']}")
    print(f"  Error:         {counts['error']}")
    if rejected_langs:
        print(f"\nRejected languages:")
        for lang, n in sorted(rejected_langs.items(), key=lambda x: -x[1]):
            print(f"  {lang:6s}: {n}")


if __name__ == "__main__":
    main()
