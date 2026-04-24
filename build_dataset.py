"""
Convert filtered PDFs to markdown using docling and push to HF Hub.

Docling outputs are cached in .cache/conversions/ so re-runs skip already-converted files.

Usage:
    uv run python build_dataset.py
    uv run python build_dataset.py --repo oliverkinch/tidsskrift-dk-cc-by
    uv run python build_dataset.py --progress downloads/progress_filtered.jsonl
    uv run python build_dataset.py --dry-run  # convert only, skip push
    uv run python build_dataset.py --no-cache  # ignore cache
"""

import argparse
import json
from pathlib import Path

from datasets import Dataset
from docling.document_converter import DocumentConverter

BASE_DIR = Path(__file__).parent
FILTERED_DIR = BASE_DIR / "filtered"
PROGRESS_FILE = BASE_DIR / "downloads" / "progress.jsonl"
JOURNALS_FILE = BASE_DIR / "journals.json"
CACHE_DIR = BASE_DIR / ".cache" / "conversions"
DEFAULT_REPO = "oliverkinch/tidsskrift-dk-cc-by"


def load_metadata_lookup() -> dict[str, dict]:
    """Build filename -> metadata dict from progress.jsonl."""
    lookup = {}
    for line in PROGRESS_FILE.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        record = json.loads(line)
        if record.get("file"):
            filename = Path(record["file"]).name
            lookup[filename] = record
    return lookup


def load_journal_descriptions() -> dict[str, str]:
    return json.loads(JOURNALS_FILE.read_text(encoding="utf-8"))


def convert_pdfs(use_cache: bool) -> list[dict]:
    converter = DocumentConverter()
    meta_lookup = load_metadata_lookup()
    journal_descriptions = load_journal_descriptions()
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    records = []
    pdf_paths = sorted(FILTERED_DIR.glob("*/*.pdf"))
    print(f"Found {len(pdf_paths)} PDFs to convert\n")

    for i, pdf_path in enumerate(pdf_paths, 1):
        journal = pdf_path.parent.name
        cache_file = CACHE_DIR / journal / (pdf_path.stem + ".txt")
        meta = meta_lookup.get(pdf_path.name, {})

        if use_cache and cache_file.exists():
            text = cache_file.read_text(encoding="utf-8")
            print(f"[{i}/{len(pdf_paths)}] {journal}/{pdf_path.name} (cached)")
        else:
            print(f"[{i}/{len(pdf_paths)}] {journal}/{pdf_path.name} ...", end=" ", flush=True)
            try:
                result = converter.convert(str(pdf_path))
                text = result.document.export_to_markdown()
                cache_file.parent.mkdir(parents=True, exist_ok=True)
                cache_file.write_text(text, encoding="utf-8")
                print("ok")
            except Exception as e:
                text = ""
                print(f"FAILED ({e})")

        if not text.strip():
            continue

        records.append({
            "text": text,
            "journal": journal,
            "journal_description": journal_descriptions.get(journal, ""),
            "title": meta.get("title") or "",
            "authors": meta.get("authors") or [],
            "doi": meta.get("doi") or "",
            "date": meta.get("date") or "",
            "url": meta.get("url") or "",
            "license": "CC BY",
        })

    return records


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", default=DEFAULT_REPO)
    parser.add_argument("--progress", default=str(BASE_DIR / "downloads" / "progress.jsonl"),
                        help="Path to progress JSONL file (default: downloads/progress.jsonl)")
    parser.add_argument("--dry-run", action="store_true", help="Convert but do not push")
    parser.add_argument("--no-cache", action="store_true", help="Ignore cached conversions")
    args = parser.parse_args()

    global PROGRESS_FILE
    PROGRESS_FILE = Path(args.progress)

    records = convert_pdfs(use_cache=not args.no_cache)
    print(f"\nTotal records: {len(records)}")

    ds = Dataset.from_list(records)
    print(ds)

    if args.dry_run:
        print("Dry run — skipping push")
        return

    print(f"\nPushing to {args.repo} ...")
    ds.push_to_hub(args.repo, split="train")
    print("Done.")


if __name__ == "__main__":
    main()
