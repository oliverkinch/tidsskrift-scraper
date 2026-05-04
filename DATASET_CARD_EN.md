---
language:
- en
license: cc-by-4.0
size_categories:
- 1K<n<10K
task_categories:
- text-generation
tags:
- english
- academic
- tidsskrift.dk
- danish-foundation-models
pretty_name: tidsskrift-dk-en
---

# tidsskrift-dk-en

English academic articles scraped from [tidsskrift.dk](https://tidsskrift.dk), the Royal Danish Library's national portal for open-access Danish academic journals. All articles are published under a CC BY license.

Collected as part of the [Danish Foundation Models](https://github.com/centre-for-humanities-computing/danish-foundation-models) project.

## Dataset composition

1,164 articles across 11 journals. PDFs were converted to markdown using [Docling](https://github.com/DS4SD/docling). Articles in non-English languages were removed based on automatic language detection.

| Journal | Articles | Topic |
|---|---|---|
| HERMES – Journal of Language and Communication in Business | 456 | Business communication, language, and discourse |
| The Nordic Journal of Aesthetics | 243 | Aesthetics, art theory, and philosophy of art |
| Classica et Mediaevalia | 88 | Classical philology and medieval studies |
| HiPhil Novum | 67 | History and philosophy of science, technology, and medicine |
| Scandinavian Journal of Child and Adolescent Psychiatry and Psychology | 66 | Child and adolescent mental health |
| Scandinavian Studies in Language | 54 | Scandinavian linguistics |
| Culture and History (KU) | 44 | Cultural history and history of ideas |
| Futures of Education, Culture and Nature | 44 | Education, sustainability, and cultural futures |
| Journalistica | 61 | Journalism research |
| Privacy Studies Journal | 21 | Privacy, surveillance, and data ethics |
| Imagining the Impossible | 20 | The fantastic in contemporary media and literature |

## Fields

| Field | Description |
|---|---|
| `text` | Full article text in markdown format |
| `journal` | Journal slug (matches tidsskrift.dk URL) |
| `journal_description` | Short description of the journal |
| `title` | Article title |
| `authors` | List of authors |
| `doi` | DOI if available |
| `date` | Publication date |
| `url` | Source URL on tidsskrift.dk |
| `license` | Always `CC BY` |

## Filtering

- Non-English articles removed via `langdetect` (accepted: `en`)
- PDFs with fewer than 300 extractable characters were excluded

## License

All articles are published under a [Creative Commons Attribution (CC BY)](https://creativecommons.org/licenses/by/4.0/) license. Attribution goes to the individual authors and journals.
