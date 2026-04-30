"""
Full pipeline: scrape → filter → convert → push to HuggingFace Hub.

Steps:
  1. Scrape CC BY PDFs from tidsskrift.dk (resumable via progress.jsonl)
  2. Filter to Danish-language PDFs only
  3. Convert PDFs to markdown with docling and push to HF Hub

Usage:
    uv run python create.py
    uv run python create.py --repo oliverkinch/tidsskrift-dk
    uv run python create.py --skip-scrape      # skip step 1
    uv run python create.py --skip-filter      # skip step 2
    uv run python create.py --dry-run          # skip push to hub
    uv run python create.py --no-cache         # ignore docling cache

Dependencies:
    requests, beautifulsoup4, pypdf, langdetect, docling, datasets
"""

import argparse
import json
import logging
import re
import shutil
import time
import warnings
from pathlib import Path

import pypdf
import requests
from bs4 import BeautifulSoup
from datasets import Dataset
from docling.document_converter import DocumentConverter
from langdetect import LangDetectException, detect

# ── Paths ─────────────────────────────────────────────────────────────────────

BASE_DIR = Path(__file__).parent
DOWNLOADS_DIR = BASE_DIR / "downloads"
FILTERED_DIR = BASE_DIR / "filtered"
PROGRESS_FILE = DOWNLOADS_DIR / "progress.jsonl"
JOURNALS_FILE = BASE_DIR / "journals.json"
CACHE_DIR = BASE_DIR / ".cache" / "conversions"
DEFAULT_REPO = "oliverkinch/tidsskrift-dk"

# ── Scraper config ─────────────────────────────────────────────────────────────

JOURNALS = [
    "passage",
    "kok",
    "journalistica",
    "periskop",
    "rvt",
    "politica",
    "akut",
    "frakvangaardtilhumlekule",
    "kierkegaardiana",
    "FPPU",
    "prototyper",
    "dttk",
    "forumforidraet",
    "tidsskriftforuddannelsesvidens",
    "politik",
    "tidsskrift-for-arbejdsliv",
]

BASE_URL = "https://tidsskrift.dk"
REQUEST_DELAY = 0.5

SESSION = requests.Session()
SESSION.headers["User-Agent"] = (
    "Mozilla/5.0 (research bot; Danish Foundation Models; "
    "https://github.com/centre-for-humanities-computing)"
)

# ── Filter config ──────────────────────────────────────────────────────────────

ACCEPTED_LANGS = {"da"}
MIN_CHARS = 300
PAGES_TO_SAMPLE = 4

logging.getLogger("pypdf").setLevel(logging.ERROR)
warnings.filterwarnings("ignore")


# ══════════════════════════════════════════════════════════════════════════════
# Step 1: Scrape
# ══════════════════════════════════════════════════════════════════════════════

def load_progress() -> set[str]:
    seen = set()
    if PROGRESS_FILE.exists():
        for line in PROGRESS_FILE.read_text(encoding="utf-8").splitlines():
            if line.strip():
                seen.add(json.loads(line)["url"])
    return seen


def log_progress(record: dict):
    PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(PROGRESS_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def get(url: str) -> requests.Response | None:
    try:
        r = SESSION.get(url, timeout=20, allow_redirects=True)
        r.raise_for_status()
        return r
    except Exception as e:
        print(f"    GET failed {url}: {e}")
        return None


def soup(url: str) -> BeautifulSoup | None:
    r = get(url)
    if r is None:
        return None
    time.sleep(REQUEST_DELAY)
    return BeautifulSoup(r.text, "html.parser")


def get_issue_urls(journal: str) -> list[str]:
    archive_url = f"{BASE_URL}/{journal}/issue/archive"
    page = soup(archive_url)
    if page is None:
        return []
    urls = []
    for a in page.select("a[href]"):
        href = a["href"]
        if re.search(rf"/{re.escape(journal)}/issue/view/", href, re.IGNORECASE):
            full = href if href.startswith("http") else BASE_URL + href
            if full not in urls:
                urls.append(full)
    current_url = f"{BASE_URL}/{journal}/issue/current"
    r = get(current_url)
    if r:
        for a in BeautifulSoup(r.text, "html.parser").select("a[href]"):
            href = a["href"]
            if re.search(rf"/{re.escape(journal)}/issue/view/", href, re.IGNORECASE):
                full = href if href.startswith("http") else BASE_URL + href
                if full not in urls:
                    urls.append(full)
        time.sleep(REQUEST_DELAY)
    return urls


def get_article_urls(issue_url: str, journal: str) -> list[str]:
    page = soup(issue_url)
    if page is None:
        return []
    urls = []
    for a in page.select("a[href]"):
        href = a["href"]
        if re.search(rf"/{re.escape(journal)}/article/view/\d+", href, re.IGNORECASE):
            if re.search(r"/article/view/\d+/\d+", href):
                continue
            full = href if href.startswith("http") else BASE_URL + href
            if full not in urls:
                urls.append(full)
    return urls


def scrape_article(article_url: str, journal: str, dest_dir: Path) -> dict:
    page = soup(article_url)
    if page is None:
        return {"url": article_url, "status": "fetch_failed"}

    record: dict = {"url": article_url, "journal": journal}

    title_el = page.select_one("h1.page-header, h1.title, .article-title h1, h1")
    record["title"] = title_el.get_text(strip=True) if title_el else None

    authors = [a.get_text(strip=True) for a in page.select(".authors .name, .author-string")]
    record["authors"] = authors or None

    abstract_el = page.select_one(".abstract p, section.abstract, #articleAbstract")
    record["abstract"] = abstract_el.get_text(strip=True) if abstract_el else None

    doi_el = page.select_one("a[href*='doi.org']")
    record["doi"] = doi_el["href"] if doi_el else None

    date_el = page.select_one(".published .value, .pub-date")
    record["date"] = date_el.get_text(strip=True) if date_el else None

    pdf_url = None

    for a in page.select("a[href]"):
        href = a["href"]
        if re.search(r"/article/download/\d+", href, re.IGNORECASE):
            pdf_url = href if href.startswith("http") else BASE_URL + href
            break

    if pdf_url is None:
        galley_url = None
        for a in page.select("a[href]"):
            href = a["href"]
            link_text = a.get_text(strip=True).upper()
            if re.search(r"/article/view/\d+/\d+", href, re.IGNORECASE) and "PDF" in link_text:
                galley_url = href if href.startswith("http") else BASE_URL + href
                break
        if galley_url:
            galley_page = soup(galley_url)
            if galley_page:
                for a in galley_page.select("a[href]"):
                    href = a["href"]
                    if re.search(r"/article/download/\d+", href, re.IGNORECASE):
                        pdf_url = href if href.startswith("http") else BASE_URL + href
                        break

    if pdf_url is None:
        for a in page.select("a[href$='.pdf']"):
            pdf_url = a["href"]
            if not pdf_url.startswith("http"):
                pdf_url = BASE_URL + pdf_url
            break

    record["pdf_url"] = pdf_url

    if pdf_url is None:
        record["status"] = "no_pdf"
        return record

    article_id = re.search(r"/article/(?:view|download)/(\d+)", article_url)
    filename = f"{article_id.group(1)}.pdf" if article_id else re.sub(r"[^\w]", "_", article_url[-40:]) + ".pdf"
    dest = dest_dir / filename

    if dest.exists():
        record["status"] = "already_downloaded"
        record["file"] = str(dest)
        return record

    r = get(pdf_url)
    if r is None:
        record["status"] = "download_failed"
        return record

    content_type = r.headers.get("content-type", "")
    if "pdf" not in content_type and not pdf_url.lower().endswith(".pdf"):
        record["status"] = "not_a_pdf"
        return record

    dest_dir.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(r.content)
    size_kb = dest.stat().st_size // 1024
    print(f"      {size_kb} KB → {dest.name}")
    record["status"] = "downloaded"
    record["file"] = str(dest)
    return record


def run_scrape():
    print("\n" + "=" * 60)
    print("STEP 1: Scraping PDFs from tidsskrift.dk")
    print("=" * 60)

    DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)
    seen = load_progress()
    print(f"Resuming — {len(seen)} articles already processed\n")

    for journal in JOURNALS:
        print(f"\n{'='*60}")
        print(f"Journal: {journal}")
        print(f"{'='*60}")
        dest_dir = DOWNLOADS_DIR / journal

        issue_urls = get_issue_urls(journal)
        print(f"  {len(issue_urls)} issues found")

        article_urls = []
        for issue_url in issue_urls:
            article_urls.extend(get_article_urls(issue_url, journal))
            time.sleep(REQUEST_DELAY)

        article_urls = list(dict.fromkeys(article_urls))
        new_articles = [u for u in article_urls if u not in seen]
        print(f"  {len(article_urls)} articles total, {len(new_articles)} new")

        for i, url in enumerate(new_articles, 1):
            print(f"  [{i}/{len(new_articles)}] {url}")
            record = scrape_article(url, journal, dest_dir)
            log_progress(record)
            seen.add(url)
            time.sleep(REQUEST_DELAY)


# ══════════════════════════════════════════════════════════════════════════════
# Step 2: Filter
# ══════════════════════════════════════════════════════════════════════════════

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


def run_filter():
    print("\n" + "=" * 60)
    print("STEP 2: Filtering to Danish-language PDFs")
    print("=" * 60)

    pdfs = sorted(DOWNLOADS_DIR.rglob("*.pdf"))
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

    print("\nResults:")
    print(f"  Kept (da):   {counts['kept']}")
    print(f"  Rejected:    {counts['rejected']}")
    print(f"  Too short:   {counts['too_short']}")
    print(f"  Error:       {counts['error']}")
    if rejected_langs:
        print("\nRejected languages:")
        for lang, n in sorted(rejected_langs.items(), key=lambda x: -x[1]):
            print(f"  {lang:6s}: {n}")


# ══════════════════════════════════════════════════════════════════════════════
# Step 3: Convert & push
# ══════════════════════════════════════════════════════════════════════════════

def load_metadata_lookup() -> dict[str, dict]:
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


def run_convert_and_push(repo: str, use_cache: bool, dry_run: bool):
    print("\n" + "=" * 60)
    print("STEP 3: Converting PDFs to markdown and pushing to Hub")
    print("=" * 60)

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

    print(f"\nTotal records: {len(records)}")
    ds = Dataset.from_list(records)
    print(ds)

    if dry_run:
        print("Dry run — skipping push")
        return

    print(f"\nPushing to {repo} ...")
    ds.push_to_hub(repo, split="train")
    print("Done.")


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Full pipeline: scrape → filter → convert → push")
    parser.add_argument("--repo", default=DEFAULT_REPO, help="HuggingFace dataset repo ID")
    parser.add_argument("--skip-scrape", action="store_true", help="Skip step 1 (scraping)")
    parser.add_argument("--skip-filter", action="store_true", help="Skip step 2 (language filter)")
    parser.add_argument("--dry-run", action="store_true", help="Convert but do not push to hub")
    parser.add_argument("--no-cache", action="store_true", help="Ignore cached docling conversions")
    args = parser.parse_args()

    if not args.skip_scrape:
        run_scrape()

    if not args.skip_filter:
        run_filter()

    run_convert_and_push(
        repo=args.repo,
        use_cache=not args.no_cache,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
