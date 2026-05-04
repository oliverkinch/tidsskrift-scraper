#!/usr/bin/env python3
"""
Filter downloaded PDFs by language.

Example:
    uv run python filter_language.py --source downloads_english_cc_by --target filtered_english --langs en
"""

import argparse
import logging
import shutil
import warnings
from pathlib import Path

import pypdf
from langdetect import LangDetectException, detect

logging.getLogger("pypdf").setLevel(logging.ERROR)
warnings.filterwarnings("ignore")

PAGES_TO_SAMPLE = 4
MIN_CHARS = 300


def extract_text(pdf_path: Path, pages_to_sample: int) -> str:
    try:
        reader = pypdf.PdfReader(pdf_path)
        pages = reader.pages[:pages_to_sample]
        return " ".join((p.extract_text() or "") for p in pages).strip()
    except Exception:
        return ""


def detect_lang(text: str) -> str | None:
    try:
        return detect(text)
    except LangDetectException:
        return None


def main():
    parser = argparse.ArgumentParser(description="Filter PDFs by language")
    parser.add_argument("--source", default="downloads", help="Source root directory")
    parser.add_argument("--target", default="filtered", help="Target root directory")
    parser.add_argument(
        "--langs",
        nargs="+",
        default=["da"],
        help="Accepted language codes (e.g. en da no)",
    )
    parser.add_argument("--min-chars", type=int, default=MIN_CHARS, help="Minimum extracted chars")
    parser.add_argument("--pages", type=int, default=PAGES_TO_SAMPLE, help="Pages to sample per PDF")
    parser.add_argument("--clear-target", action="store_true", help="Delete target directory before copying")
    args = parser.parse_args()

    source_dir = Path(args.source)
    target_dir = Path(args.target)
    accepted_langs = {lang.lower() for lang in args.langs}

    if args.clear_target and target_dir.exists():
        shutil.rmtree(target_dir)

    pdfs = sorted(source_dir.rglob("*.pdf"))
    print(f"Total PDFs: {len(pdfs)}")
    print(f"Accepted languages: {', '.join(sorted(accepted_langs))}")

    counts = {"kept": 0, "rejected": 0, "too_short": 0, "error": 0}
    rejected_langs: dict[str, int] = {}

    for pdf in pdfs:
        text = extract_text(pdf, pages_to_sample=args.pages)

        if len(text) < args.min_chars:
            counts["too_short"] += 1
            continue

        lang = detect_lang(text)
        if lang is None:
            counts["error"] += 1
            continue

        if lang in accepted_langs:
            dest = target_dir / pdf.relative_to(source_dir)
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(pdf, dest)
            counts["kept"] += 1
        else:
            counts["rejected"] += 1
            rejected_langs[lang] = rejected_langs.get(lang, 0) + 1

    print("\nResults:")
    print(f"  Kept:         {counts['kept']}")
    print(f"  Rejected:     {counts['rejected']}")
    print(f"  Too short:    {counts['too_short']}")
    print(f"  Error:        {counts['error']}")
    if rejected_langs:
        print("\nRejected languages:")
        for lang, num in sorted(rejected_langs.items(), key=lambda x: -x[1]):
            print(f"  {lang:6s}: {num}")


if __name__ == "__main__":
    main()
