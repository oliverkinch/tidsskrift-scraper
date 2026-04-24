# tidsskrift-scraper

Scraper for [tidsskrift.dk](https://tidsskrift.dk), the Royal Danish Library's national portal for open-access Danish academic journals. Downloads PDFs, converts them to markdown, filters to Danish-language articles, and pushes the result to Hugging Face Hub.

Part of the [Danish Foundation Models](https://github.com/centre-for-humanities-computing/danish-foundation-models) project.

The resulting dataset is [`oliverkinch/tidsskrift-dk`](https://huggingface.co/datasets/oliverkinch/tidsskrift-dk) on Hugging Face Hub. See [DATASET_CARD.md](DATASET_CARD.md) for dataset documentation.

## Scripts

| Script | Description |
|---|---|
| `scraper.py` | Scrapes article metadata and downloads PDFs from tidsskrift.dk |
| `filter_danish.py` | Filters downloaded PDFs to Danish-language articles only |
| `build_dataset.py` | Converts PDFs to markdown using Docling and pushes to Hugging Face Hub |

## Installation

Requires Python 3.10+. Install dependencies with [uv](https://github.com/astral-sh/uv):

```bash
uv sync
```

Or with pip:

```bash
pip install -e .
```

## Usage

Run the scripts in order:

**1. Scrape articles**
```bash
uv run python scraper.py
```
Downloads PDFs to `downloads/` and tracks progress in `progress.jsonl`.

**2. Filter to Danish**
```bash
uv run python filter_danish.py
```
Copies Danish-language PDFs to `filtered/`, skipping short or non-Danish files.

**3. Convert and push to Hugging Face**

First, authenticate with Hugging Face:
```bash
huggingface-cli login
```

Then run:
```bash
uv run python build_dataset.py
```

To push to a different repository:
```bash
uv run python build_dataset.py --repo your-username/your-dataset-name
```

## Journals

Scrapes the journals listed in `journals.json`. All articles are published under a CC BY license.

## License

Source code: [MIT](https://opensource.org/licenses/MIT)

Dataset content: [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — attribution goes to the individual authors and journals.
