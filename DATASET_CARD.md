---
language:
- da
license: cc-by-4.0
size_categories:
- 1K<n<10K
task_categories:
- text-generation
tags:
- danish
- academic
- tidsskrift.dk
- danish-foundation-models
pretty_name: tidsskrift-dk-cc-by
---

# tidsskrift-dk-cc-by

Danish academic articles scraped from [tidsskrift.dk](https://tidsskrift.dk), the Royal Danish Library's national portal for open-access journals. All articles are published under a CC BY license.

Collected as part of the [Danish Foundation Models](https://github.com/centre-for-humanities-computing/danish-foundation-models) project.

## Dataset composition

5,699 articles across 16 journals. PDFs were converted to markdown using [Docling](https://github.com/DS4SD/docling). Articles in English, Norwegian, Swedish, or other non-Danish languages have been removed based on automatic language detection.

| Journal | Articles | Topic |
|---|---|---|
| K&K – Kultur og Klasse | 1,141 | Humanities: literature, art, film, music, cultural studies |
| Politica | 972 | Political science |
| Passage | 814 | Literature and literary criticism — mix of creative writing, academic articles, and editorials |
| Religionsvidenskabeligt Tidsskrift | 687 | Religious studies |
| Kierkegaardiana | 356 | Kierkegaard scholarship |
| Periskop | 272 | Art history |
| Dansk Tidsskrift for Teologi og Kirke | 237 | Theology and church studies |
| Forum for Idræt | 230 | Sports and physical activity |
| Politik | 193 | Interdisciplinary political studies |
| Tidsskrift for Arbejdsliv | 183 | Working life, work environment, labour market |
| Journalistica | 170 | Journalism research |
| Forskning i Pædagogers Profession og Uddannelse | 144 | Pedagogy and educator training |
| Dansk Tidsskrift for Akutmedicin | 124 | Emergency medicine |
| Fra Kvangård til Humlekule | 109 | Garden history |
| Tidsskrift for Uddannelsesvidenskab | 37 | Education science |
| Prototyper – Studier i design | 29 | Design studies |

## Fields

| Field | Description |
|---|---|
| `text` | Full article text in markdown format |
| `journal` | Journal slug (matches tidsskrift.dk URL) |
| `journal_description` | Short description of the journal in Danish |
| `title` | Article title |
| `authors` | List of authors |
| `doi` | DOI if available |
| `date` | Publication date |
| `url` | Source URL on tidsskrift.dk |
| `license` | Always `CC BY` |

## Filtering

- Non-Danish articles removed via `langdetect` (accepted: `da`)
- Removed: tables of contents, abstract-only pages, reviewer acknowledgements
- PDFs with fewer than 300 extractable characters were excluded

## License

All articles are published under a [Creative Commons Attribution (CC BY)](https://creativecommons.org/licenses/by/4.0/) license. Attribution goes to the individual authors and journals.
