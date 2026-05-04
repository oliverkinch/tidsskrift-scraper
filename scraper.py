#!/usr/bin/env python3
"""
Scrape article PDFs from tidsskrift.dk (OJS platform).

Supports two modes:
  1) Static list mode (legacy, default)
  2) English CC-BY discovery mode (dynamic):
     - discover all journals from tidsskrift.dk index
     - keep journals with English submissions page + explicit CC BY (not NC/ND/SA)
     - download PDFs and keep only English article text

Usage:
    uv run python scraper.py
    uv run python scraper.py --english-cc-by
"""

import argparse
import io
import json
import re
import time
from pathlib import Path

import pypdf
import requests
from bs4 import BeautifulSoup
from langdetect import LangDetectException, detect

LEGACY_JOURNALS = [
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
    "hiphilnovum",
    "culturehistoryku",
]

BASE_URL = "https://tidsskrift.dk"
REQUEST_DELAY = 0.5
PAGES_TO_SAMPLE = 4
MIN_CHARS_FOR_LANG = 300

SESSION = requests.Session()
SESSION.headers["User-Agent"] = (
    "Mozilla/5.0 (research bot; Danish Foundation Models; "
    "https://github.com/centre-for-humanities-computing)"
)


def load_progress(progress_file: Path) -> set[str]:
    seen = set()
    if progress_file.exists():
        for line in progress_file.read_text(encoding="utf-8").splitlines():
            if line.strip():
                seen.add(json.loads(line)["url"])
    return seen


def log_progress(progress_file: Path, record: dict):
    progress_file.parent.mkdir(parents=True, exist_ok=True)
    with progress_file.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def get(url: str) -> requests.Response | None:
    try:
        r = SESSION.get(url, timeout=25, allow_redirects=True)
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


def classify_cc_license(text: str) -> tuple[str, str | None]:
    normalized = re.sub(r"\s+", " ", text.lower())
    strict_non_cc_by = [
        r"cc\s*by\s*[-–]?\s*(nc|nd|sa)",
        r"cc\s*by\s*[-–]\s*(nc|nd|sa)",
        r"attribution[-\s]*noncommercial",
        r"attribution[-\s]*noderivatives",
        r"attribution[-\s]*sharealike",
        r"creative\s+commons\s+attribution[-\s]*(noncommercial|noderivatives|sharealike)",
    ]
    cc_by_markers = [
        r"cc\s*by(?:\s*4\.0)?",
        r"creative\s+commons\s+attribution",
        r"creative\s+commons\s+4\.0\s+attribution\s+international\s+license",
    ]

    for pattern in strict_non_cc_by:
        m = re.search(pattern, normalized, re.IGNORECASE)
        if m:
            snippet = normalized[max(0, m.start() - 60):m.end() + 60]
            return "non_cc_by", snippet

    for pattern in cc_by_markers:
        m = re.search(pattern, normalized, re.IGNORECASE)
        if m:
            snippet = normalized[max(0, m.start() - 60):m.end() + 60]
            return "cc_by", snippet

    return "unknown", None


def is_english_submissions_page(page: BeautifulSoup) -> bool:
    html_tag = page.find("html")
    lang = (html_tag.get("lang", "") if html_tag else "").lower()
    if lang.startswith("en"):
        return True

    title = page.title.get_text(" ", strip=True).lower() if page.title else ""
    if "submissions" in title:
        return True

    h1 = page.find("h1")
    if h1 and "submissions" in h1.get_text(" ", strip=True).lower():
        return True

    return False


def discover_all_journals() -> list[str]:
    page = soup(f"{BASE_URL}/index/index")
    if page is None:
        return []

    slugs = []
    for a in page.select("a[href]"):
        href = a["href"]
        if not href.startswith(f"{BASE_URL}/"):
            continue
        if "/issue/current" not in href:
            continue
        slug = href.split(f"{BASE_URL}/", 1)[1].split("/issue/current", 1)[0].strip("/")
        if slug and slug != "index":
            slugs.append(slug)

    return sorted(set(slugs))


def discover_english_cc_by_journals() -> list[str]:
    all_journals = discover_all_journals()
    kept = []
    skipped_non_english = 0
    skipped_non_cc_by = 0
    skipped_unknown_license = 0

    print(f"Discovered {len(all_journals)} journals from index")

    for idx, journal in enumerate(all_journals, 1):
        submissions_url = f"{BASE_URL}/{journal}/about/submissions"
        page = soup(submissions_url)
        if page is None:
            continue

        if not is_english_submissions_page(page):
            skipped_non_english += 1
            continue

        full_text = page.get_text(" ", strip=True)
        license_class, _ = classify_cc_license(full_text)
        if license_class == "cc_by":
            kept.append(journal)
        elif license_class == "non_cc_by":
            skipped_non_cc_by += 1
        else:
            skipped_unknown_license += 1

        if idx % 30 == 0:
            print(f"  checked {idx}/{len(all_journals)} journals...")

    print("\nJournal discovery summary")
    print(f"  kept (english + CC BY):   {len(kept)}")
    print(f"  skipped non-english:      {skipped_non_english}")
    print(f"  skipped non-CC-BY:        {skipped_non_cc_by}")
    print(f"  skipped unknown license:  {skipped_unknown_license}")
    return kept


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


def detect_language_from_pdf_bytes(pdf_bytes: bytes) -> str | None:
    try:
        reader = pypdf.PdfReader(io.BytesIO(pdf_bytes))
        pages = reader.pages[:PAGES_TO_SAMPLE]
        text = " ".join((p.extract_text() or "") for p in pages).strip()
    except Exception:
        return None

    if len(text) < MIN_CHARS_FOR_LANG:
        return None

    try:
        return detect(text)
    except LangDetectException:
        return None


def get_journal_license_details(journal: str) -> tuple[str | None, str | None]:
    submissions_url = f"{BASE_URL}/{journal}/about/submissions"
    page = soup(submissions_url)
    if page is None:
        return None, submissions_url
    license_class, snippet = classify_cc_license(page.get_text(" ", strip=True))
    if license_class == "cc_by":
        return "CC BY", submissions_url
    return None, submissions_url


def scrape_article(
    article_url: str,
    journal: str,
    dest_dir: Path,
    target_lang: str | None,
    journal_license: str | None,
    license_source_url: str | None,
) -> dict:
    page = soup(article_url)
    if page is None:
        return {"url": article_url, "status": "fetch_failed"}

    record: dict = {
        "url": article_url,
        "journal": journal,
        "journal_license": journal_license,
        "license_source_url": license_source_url,
    }

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

    lang = detect_language_from_pdf_bytes(r.content)
    record["language"] = lang

    if target_lang and lang != target_lang:
        record["status"] = "language_mismatch"
        record["target_language"] = target_lang
        return record

    dest_dir.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(r.content)
    size_kb = dest.stat().st_size // 1024
    print(f"      {size_kb} KB -> {dest.name}")
    record["status"] = "downloaded"
    record["file"] = str(dest)
    return record


def summarize_progress(progress_file: Path):
    all_records = []
    if progress_file.exists():
        for line in progress_file.read_text(encoding="utf-8").splitlines():
            if line.strip():
                all_records.append(json.loads(line))

    counts: dict[str, int] = {}
    for record in all_records:
        status = record.get("status", "unknown")
        counts[status] = counts.get(status, 0) + 1

    print(f"\n{'=' * 60}")
    print("SUMMARY")
    print(f"{'=' * 60}")
    for status, count in sorted(counts.items()):
        print(f"  {status:25s}: {count}")
    downloaded = sum(1 for record in all_records if record.get("status") == "downloaded")
    print(f"\n  PDFs saved: {downloaded}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape article PDFs from tidsskrift.dk")
    parser.add_argument(
        "--english-cc-by",
        action="store_true",
        help="Discover journals dynamically and keep English journals with explicit CC BY",
    )
    parser.add_argument(
        "--downloads-dir",
        default=None,
        help="Output directory for downloaded PDFs (default depends on mode)",
    )
    parser.add_argument(
        "--progress-file",
        default=None,
        help="Progress JSONL file path (default depends on mode)",
    )
    parser.add_argument(
        "--max-journals",
        type=int,
        default=None,
        help="Optional limit for number of journals",
    )
    parser.add_argument(
        "--request-delay",
        type=float,
        default=REQUEST_DELAY,
        help="Seconds between requests",
    )
    parser.add_argument(
        "--journal",
        action="append",
        default=None,
        help="Restrict run to specific journal slug (repeatable)",
    )
    return parser.parse_args()


def main():
    global REQUEST_DELAY
    args = parse_args()
    REQUEST_DELAY = args.request_delay

    base_dir = Path(__file__).parent
    if args.english_cc_by:
        downloads_dir = Path(args.downloads_dir) if args.downloads_dir else (base_dir / "downloads_english_cc_by")
        progress_file = Path(args.progress_file) if args.progress_file else (downloads_dir / "progress_english_cc_by.jsonl")
        target_lang = "en"
    else:
        downloads_dir = Path(args.downloads_dir) if args.downloads_dir else (base_dir / "downloads")
        progress_file = Path(args.progress_file) if args.progress_file else (downloads_dir / "progress.jsonl")
        target_lang = None

    downloads_dir.mkdir(parents=True, exist_ok=True)
    seen = load_progress(progress_file)
    print(f"Resuming -- {len(seen)} articles already processed")
    print(f"Downloads dir: {downloads_dir}")
    print(f"Progress file: {progress_file}\n")

    if args.journal:
        journals = args.journal
    elif args.english_cc_by:
        journals = discover_english_cc_by_journals()
    else:
        journals = LEGACY_JOURNALS

    if args.max_journals is not None:
        journals = journals[: args.max_journals]

    print(f"\nWill scrape {len(journals)} journals")

    for journal in journals:
        print(f"\n{'=' * 60}")
        print(f"Journal: {journal}")
        print(f"{'=' * 60}")
        dest_dir = downloads_dir / journal

        journal_license = None
        license_source_url = None
        if args.english_cc_by:
            journal_license, license_source_url = get_journal_license_details(journal)

        issue_urls = get_issue_urls(journal)
        print(f"  {len(issue_urls)} issues found")

        article_urls = []
        for issue_url in issue_urls:
            article_urls.extend(get_article_urls(issue_url, journal))
            time.sleep(REQUEST_DELAY)

        article_urls = list(dict.fromkeys(article_urls))
        new_articles = [url for url in article_urls if url not in seen]
        print(f"  {len(article_urls)} articles total, {len(new_articles)} new")

        for idx, url in enumerate(new_articles, 1):
            print(f"  [{idx}/{len(new_articles)}] {url}")
            record = scrape_article(
                article_url=url,
                journal=journal,
                dest_dir=dest_dir,
                target_lang=target_lang,
                journal_license=journal_license,
                license_source_url=license_source_url,
            )
            log_progress(progress_file, record)
            seen.add(url)
            time.sleep(REQUEST_DELAY)

    summarize_progress(progress_file)


if __name__ == "__main__":
    main()
