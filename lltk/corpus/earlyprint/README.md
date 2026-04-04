# EarlyPrint Corpus

Linguistically tagged TCP texts from the [EarlyPrint Project](https://earlyprint.org). Combines three Text Creation Partnership collections with enhanced XML (lemmatization, POS tagging, regularized spelling):

- **EEBO-TCP** (~60K texts, 1470s–1690s)
- **ECCO-TCP** (~2K texts, 1700s–1790s)
- **Evans-TCP** (~4K texts, 1640s–1800s)

## Setup

```bash
lltk compile earlyprint
```

This clones three BitBucket repos, initializes git submodules, symlinks XMLs, and builds metadata.csv. Takes 20-30 minutes (mostly submodule downloads).

## Directory structure

```
~/lltk_data/corpora/earlyprint/
  repos/
    eebotcp/texts/A00/A00001.xml   (git repos with actual files)
    eccotcp/texts/K00/K00001.xml
    evanstcp/texts/N00/N00001.xml
  xml/
    eebotcp/A00/A00001.xml → symlink to repos
    eccotcp/K00/K00001.xml → symlink to repos
    evanstcp/N00/N00001.xml → symlink to repos
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
