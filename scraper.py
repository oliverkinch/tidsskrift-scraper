#!/usr/bin/env python3
"""
Scrape CC BY articles from tidsskrift.dk (OJS platform).

For each journal: enumerate all issues → all articles → download PDF.
Progress is saved to downloads/progress.jsonl so runs are resumable.

Usage:
    uv run scraper.py
"""

import json
import re
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ── Configuration ─────────────────────────────────────────────────────────────

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
DOWNLOADS_DIR = Path(__file__).parent / "downloads"
PROGRESS_FILE = DOWNLOADS_DIR / "progress.jsonl"

REQUEST_DELAY = 0.5  # seconds between requests

SESSION = requests.Session()
SESSION.headers["User-Agent"] = (
    "Mozilla/5.0 (research bot; Danish Foundation Models; "
    "https://github.com/centre-for-humanities-computing)"
)

# ── Progress tracking ─────────────────────────────────────────────────────────

def load_progress() -> set[str]:
    """Return set of article URLs already processed."""
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


# ── HTTP helpers ──────────────────────────────────────────────────────────────

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


# ── OJS scraping ──────────────────────────────────────────────────────────────

def get_issue_urls(journal: str) -> list[str]:
    """Return all issue URLs for a journal from its archive page."""
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
    # Also include current issue
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
    """Return all article URLs from an issue page."""
    page = soup(issue_url)
    if page is None:
        return []
    urls = []
    for a in page.select("a[href]"):
        href = a["href"]
        if re.search(rf"/{re.escape(journal)}/article/view/\d+", href, re.IGNORECASE):
            # Exclude galley sub-pages (view/{id}/{galley})
            if re.search(rf"/article/view/\d+/\d+", href):
                continue
            full = href if href.startswith("http") else BASE_URL + href
            if full not in urls:
                urls.append(full)
    return urls


def scrape_article(article_url: str, journal: str, dest_dir: Path) -> dict:
    """Scrape metadata and download PDF for a single article."""
    page = soup(article_url)
    if page is None:
        return {"url": article_url, "status": "fetch_failed"}

    record: dict = {"url": article_url, "journal": journal}

    # Title
    title_el = page.select_one("h1.page-header, h1.title, .article-title h1, h1")
    record["title"] = title_el.get_text(strip=True) if title_el else None

    # Authors
    authors = [a.get_text(strip=True) for a in page.select(".authors .name, .author-string")]
    record["authors"] = authors or None

    # Abstract
    abstract_el = page.select_one(".abstract p, section.abstract, #articleAbstract")
    record["abstract"] = abstract_el.get_text(strip=True) if abstract_el else None

    # DOI
    doi_el = page.select_one("a[href*='doi.org']")
    record["doi"] = doi_el["href"] if doi_el else None

    # Publication date
    date_el = page.select_one(".published .value, .pub-date")
    record["date"] = date_el.get_text(strip=True) if date_el else None

    # Find PDF download link.
    # OJS pattern: article page has a galley link (/article/view/{id}/{galley_id})
    # with text "PDF". That galley page then has the actual download link
    # at /article/download/{id}/{galley_id}/{format_id}.
    pdf_url = None

    # First: check if there's already a direct download link on the article page
    for a in page.select("a[href]"):
        href = a["href"]
        if re.search(r"/article/download/\d+", href, re.IGNORECASE):
            pdf_url = href if href.startswith("http") else BASE_URL + href
            break

    # Second: follow the galley view link to find the download link
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

    # Fallback: direct .pdf link
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

    # Download PDF
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


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
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

        # Deduplicate
        article_urls = list(dict.fromkeys(article_urls))
        new_articles = [u for u in article_urls if u not in seen]
        print(f"  {len(article_urls)} articles total, {len(new_articles)} new")

        for i, url in enumerate(new_articles, 1):
            print(f"  [{i}/{len(new_articles)}] {url}")
            record = scrape_article(url, journal, dest_dir)
            log_progress(record)
            seen.add(url)
            time.sleep(REQUEST_DELAY)

    # Summary
    all_records = []
    if PROGRESS_FILE.exists():
        for line in PROGRESS_FILE.read_text(encoding="utf-8").splitlines():
            if line.strip():
                all_records.append(json.loads(line))

    counts: dict[str, int] = {}
    for r in all_records:
        s = r.get("status", "unknown")
        counts[s] = counts.get(s, 0) + 1

    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    for status, count in sorted(counts.items()):
        print(f"  {status:25s}: {count}")
    downloaded = sum(1 for r in all_records if r.get("status") == "downloaded")
    print(f"\n  PDFs saved: {downloaded}")


if __name__ == "__main__":
    main()
