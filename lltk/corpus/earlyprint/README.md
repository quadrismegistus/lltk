# EarlyPrint Corpus

Linguistically tagged TCP texts from the [EarlyPrint Project](https://earlyprint.org). Combines three Text Creation Partnership collections with enhanced XML (lemmatization, POS tagging, regularized spelling):

- **EEBO-TCP** (~60K texts, 1470s–1690s)
- **ECCO-TCP** (~2K texts, 1700s–1790s)
- **Evans-TCP** (~4K texts, 1640s–1800s)

## Setup

```bash
lltk compile earlyprint
```

This clones three BitBucket repos (shallow, ~15GB total), initializes git submodules, gzip-copies XMLs (~10x compression), and builds metadata.csv.

Compile one repo at a time:

```bash
lltk compile earlyprint --repos eccotcp     # ~2K texts, quick
lltk compile earlyprint --repos evanstcp    # ~4K texts, quick
lltk compile earlyprint --repos eebotcp     # ~60K texts, slow (~30 min)
```

Resumable — re-running skips already downloaded repos and already compressed files.

## Updating

Pull latest text corrections from EarlyPrint:

```python
import lltk
c = lltk.load('earlyprint')
c.update()                      # update all cloned repos
c.update(repos=['eccotcp'])     # update just one
```

This runs `git pull` + `git submodule update`, gzip-copies any new/changed files, and rebuilds metadata.csv.

## Directory structure

```
~/lltk_data/corpora/earlyprint/
  repos/
    eebotcp/texts/A00/A00001.xml   (git repos, original files)
    eccotcp/texts/C00/C00980.xml
    evanstcp/texts/N00/N00346.xml
  xml/
    eebotcp/A00/A00001.xml.gz      (gzip-compressed copies, ~10x smaller)
    eccotcp/C00/C00980.xml.gz
    evanstcp/N00/N00346.xml.gz
  metadata.csv
```

## TCP ID prefixes

| Prefix | Collection | Example |
|--------|-----------|---------|
| A, B | EEBO-TCP | A18993 |
| K | ECCO-TCP | K01470 |
| N | Evans-TCP | N00346 |

## Metadata fields

Extracted from TEI headers by `_parse_earlyprint_meta()`:

| Field | Source | Example |
|---|---|---|
| `id` | filename | `A18993` |
| `title` | `<titleStmt><title>` | `The first book of Amadis of Gaule` |
| `author` | `<biblFull><author>` | `Munday, Anthony, 1553-1633.` |
| `year` | `<ep:publicationYear>` → int | `1590` |
| `year_creation` | `<ep:creationYear>` | |
| `publisher` | `<biblFull><publicationStmt><publisher>` | `E. Allde,` |
| `pubplace` | `<biblFull><publicationStmt><pubPlace>` | `[London :` |
| `date` | `<biblFull><publicationStmt><date>` | `1590?]` |
| `extent` | `<biblFull><extent>` | `[1+], 201, [3] leaves` |
| `language` | `<language ident>` | `eng` |
| `subject` | `<keywords><term>` | `Romances, Spanish -- Translations into English` |
| `notes` | `<biblFull><notesStmt>` | pipe-separated notes |
| `id_dlps` | `<idno type="DLPS">` | `A18993` |
| `id_stc` | `<idno type="STC">` | `STC 541` |
| `id_estc` | normalized from `<idno type="STC">` where `ESTC ...` | `S112788` |
| `id_eebo_citation` | `<idno type="EEBO-CITATION">` | `99848031` |
| `id_vid` | `<idno type="VID">` | `13103` |
| `id_proquest` | `<idno type="PROQUEST">` | `99848031` |
| `id_proquestgoid` | `<idno type="PROQUESTGOID">` | `2240909306` |
| `num_pages` | `<ep:pageCount>` → int | `416` |
| `num_words` | `<ep:wordCount>` → int | `165762` |
| `ep_quality_grade` | `<ep:finalGrade>` | `D` (A/B/C/D) |
| `ep_defect_rate` | `<ep:defectRate>` | `44.52` (per 10K words) |
| `ep_corpus` | `<ep:corpus>` | `eebo` / `ecco` / `evans` |
| `ep_repo` | which git repo | `eebotcp` / `eccotcp` / `evanstcp` |
| `tcp_source` | derived from ID prefix | `eebo` / `ecco` / `evans` |

## ESTC linking

EarlyPrint texts link to ESTC via `id_estc` (normalized from `<idno type="STC">` entries prefixed with `ESTC`). This enables genre inheritance from ESTC's classification system.

## Quality grades

EarlyPrint assigns quality grades based on OCR/transcription defect rates:

| Grade | Defects per 10K words |
|-------|----------------------|
| A | 0–5 |
| B | 5–35 |
| C | 35–100 (not stated, inferred) |
| D | 35–100 |

## Sources

- [EarlyPrint Project](https://earlyprint.org)
- [EEBO-TCP repo](https://bitbucket.org/eads004/eebotcp)
- [ECCO-TCP repo](https://bitbucket.org/eads004/eccotcp)
- [Evans-TCP repo](https://bitbucket.org/eads004/evanstcp)
- [Combined view repo](https://bitbucket.org/eads004/eptexts)
