# Literary Language Toolkit (LLTK)

A Python package for computational literary analysis and digital humanities research. 70+ literary corpora (English, French, German, Spanish), a ClickHouse analytical database for querying 2.8M+ texts across all sources, cross-corpus deduplication, multilingual passage search with embeddings, automated genre classification, language detection, and metrical scansion.

**Package:** [`lltk-dh`](https://pypi.org/project/lltk-dh/) on PyPI | **License:** MIT | **Python:** >=3.8

## Install

```bash
pip install -U lltk-dh

# or latest from source:
pip install -U git+https://github.com/quadrismegistus/lltk
```

Optional extras:

```bash
pip install "lltk-dh[embeddings]"   # sentence-transformers + torch for semantic search
pip install "lltk-dh[analysis]"     # scipy for statistical analysis
```

## Quick start

```python
import lltk

# List available corpora
lltk.show()

# Load a corpus
c = lltk.load('ecco_tcp')

# Metadata as a pandas DataFrame
c.meta
c.meta.query('1770 < year < 1830')

# Iterate texts
for t in c.texts():
    print(t.id, t.author, t.title, t.year)
    print(t.text_plain()[:200])
    print(t.freqs())       # word frequencies (Counter)

# Corpus-level analysis
mfw = c.mfw(n=10000)              # top 10K words across corpus
dtm = c.dtm(n=10000)              # document-term matrix (DataFrame)
dtm = c.dtm(n=10000, tfidf=True)  # TF-IDF weighted
```

### Installing corpus data

Corpora live at `~/lltk_data/corpora/<corpus_id>/`. Each has: `metadata.csv`, `txt/`, and optionally `xml/`, `freqs/`. Some corpora are freely downloadable; others require institutional access.

```bash
# Download a corpus (metadata + freqs)
lltk install ecco_tcp --parts metadata,freqs

# Full texts
lltk install ecco_tcp --parts txt
```

## The centralized database

The core of LLTK is a ClickHouse analytical database (`lltk.db`) that indexes all corpora into a single queryable store. It enables sub-second queries across 2.8M texts, cross-corpus deduplication, genre enrichment from bibliography corpora, language detection, and virtual corpus construction.

### Building the database

```bash
lltk db-rebuild                     # ingest all corpus CSVs -> lltk.texts
lltk db-freqs                       # ingest per-text word frequencies
lltk db-text-words                  # build flat word index for analytics
lltk db-match                       # cross-corpus dedup matching (~2 min)
lltk db-enrich-genres               # propagate genre from bibliographies
lltk db-detect-langs                # per-text language detection
lltk db-detect-translations         # flag translations via match groups
lltk db-info                        # genre x corpus crosstab
```

### Querying

```python
import lltk

# Single-row lookup
lltk.db.get('_estc/T012345')

# SQL queries on the texts table
lltk.db.query("SELECT * FROM texts WHERE year < 1700 AND genre = 'Fiction'")
lltk.db.query("SELECT corpus, COUNT(*) as n FROM texts GROUP BY corpus")

# Iterate text objects with filters + dedup
for t in lltk.db.texts(genre='Fiction', year_min=1600, year_max=1800, dedup=True):
    print(t.corpus.id, t.title, t.year)
    print(t.freqs())   # resolves through source corpus

# As DataFrame
df = lltk.db.texts_df(genre='Fiction', dedup=True)

# Ngram frequencies (with dedup and genre filtering)
lltk.db.ngram(['virtue', 'honor'], genre='Fiction', dedup=True)
```

Text objects returned by `lltk.db.texts()` keep their original corpus reference, so `t.text_plain()`, `t.freqs()`, and file paths all resolve through the source corpus.

### Cross-corpus matching

Matching finds duplicate and reprint texts across corpora via multiple tiers:

| Tier | Method | Description |
|------|--------|-------------|
| 0 | `id_link` | Shared IDs from declared cross-corpus links |
| 1a | `exact_norm` | Normalized title + author |
| 1b | `exact_norm_year` | Normalized title + year (authorless texts) |
| 2a | `containment` | Short title within long title, same author |
| 2b | `containment_year` | Same, by year |
| 3 | `fuzzy_title` | Jaro-Winkler > 0.85 (opt-in with `--fuzzy`) |

Connected components are grouped and ranked by corpus source preference. Normalization includes MorphAdorner spelling modernization (358K entries for early modern English).

```python
lltk.db.match()                     # exact + containment matching
lltk.db.find_matches('Incognita')   # search match groups by title
```

### Full-text and semantic search

LLTK splits texts into ~500-word passages and indexes them for search:

```bash
lltk db-passages                    # build passage chunks
lltk search "virtue AND honor"      # full-text search (FTS5)
lltk search "NEAR(virtue honor, 5)" # proximity search
```

```python
# Full-text search with filters
results = lltk.db.search('virtue', genre='Fiction', year_min=1700, year_max=1800)

# Semantic search (requires embeddings extra)
results = lltk.db.search_semantic('concept of honor in battle')
```

Passage embeddings use `intfloat/multilingual-e5-large` and support cross-lingual queries.

```bash
lltk db-embed-passages              # compute embeddings (GPU recommended)
lltk db-match-embeddings            # find duplicates via embedding similarity
```

### Annotations

A priority-based annotation system for storing and resolving metadata across multiple sources (human labels, bibliographies, LLM predictions):

```python
from lltk.tools import annotations as A

# Write annotations
A.write(source='llm:gemini-2.5-pro', rows=[
    {'_id': '_estc/T068056', 'field': 'genre', 'value': 'Fiction', 'confidence': 0.95}
])

# Resolve: highest-priority source wins per (text, field)
A.resolve(ids=['_estc/T068056'], fields=['genre'])

# Find disagreements between sources
A.disagreements('genre', min_sources=2)
```

Source priorities: human (100) > bibliography (90) > authority corpus (70) > heuristic (50) > LLM (10).

## Texts

```python
c = lltk.load('ecco_tcp')

for t in c.texts():
    t.id                    # text identifier
    t.author                # metadata attributes
    t.title
    t.year

    t.text_plain()          # plain text as string
    t.xml                   # XML source (if available)
    t.freqs()               # word frequencies (Counter)

# Direct access by ID
t = c.text('some_text_id')
```

### Sections

Texts can be split into structural sections (chapters, letters, etc.) from XML, or into paragraphs and fixed-length passages:

```python
for ch in t.chapters.texts():
    print(ch.get('title'), ch.text_plain()[:100])

for p in t.paragraphs.texts():
    print(p.id, len(p.text_plain()))
```

### Prosodic analysis

Optional integration with [prosodic](https://github.com/quadrismegistus/prosodic) (>=3.1) for metrical scansion:

```bash
lltk prosodic-parse ecco_tcp         # parse a corpus
lltk prosodic-aggregate ecco_tcp     # build prosodic.parquet
```

```python
t.prosodic(cached=True)              # per-text scansion data
```

## Corpus-level analysis

### Document-term matrix

```python
dtm = c.dtm(n=10000)               # raw counts (DataFrame)
dtm = c.dtm(n=10000, tf=True)      # term frequencies
dtm = c.dtm(n=10000, tfidf=True)   # TF-IDF weighted
```

Returns a pandas DataFrame: rows = text IDs, columns = words.

### Virtual corpora (CuratedCorpus)

Declarative corpus classes that pull texts from multiple sources with filters and deduplication:

```python
from lltk.corpus.arc_corpora.arc_corpora import ArcFiction

c = lltk.load('arc_fiction')
c.meta       # all English fiction, deduplicated across 10+ source corpora
```

Built-in curated corpora include `ArcFiction`, `ArcPoetry`, `ArcFictionFr`, `ArcFictionDe`, `ArcBiography`, `ArcEssays`, `ArcSermons`, and `ArcPeriodical`.

Define your own:

```python
from lltk.corpus.arc_corpora.arc_corpora import CuratedCorpus

class MyFiction(CuratedCorpus):
    ID = 'my_fiction'
    NAME = 'MyFiction'
    SOURCES = {
        'chadwyck': {'genre': 'Fiction'},
        'ecco_tcp': {'genre': 'Fiction'},
        'hathi_englit': {'genre': 'Fiction', 'year_max': 1900},
    }
    DEDUP = True
    DEDUP_BY = 'oldest'
```

## CLI reference

```
Corpus management:
  lltk show                              list available corpora
  lltk status                            check install status of all corpora
  lltk info <corpus>                     corpus details
  lltk install <corpus> [--parts ...]    download corpus data
  lltk compile <corpus>                  compile corpus from raw sources
  lltk preprocess <corpus> --parts ...   XML->TXT, TXT->freqs

Database (ClickHouse):
  lltk db-rebuild [corpus ...]           ingest corpus CSVs -> lltk.texts
  lltk db-freqs [corpus ...]             ingest per-text freqs JSONs
  lltk db-text-words [corpus ...]        build flat word index
  lltk db-wordindex [--vocab-size N]     build aggregation tables
  lltk db-info                           genre x corpus crosstab

Matching & dedup:
  lltk db-match [--fuzzy]                cross-corpus dedup matching
  lltk db-matches "title"                search match groups
  lltk db-match-stats                    matching statistics
  lltk db-match-embeddings               embedding-based matching

Genre & language:
  lltk db-enrich-genres                  propagate genre from bibliographies
  lltk db-tag-genres                     materialize genre tags from annotations
  lltk db-detect-langs                   per-text language detection
  lltk db-detect-translations            flag translations via match groups

Search & embeddings:
  lltk search "query" [--genre ...]      full-text passage search
  lltk db-passages [corpus ...]          build passage chunks
  lltk db-embed-passages [corpus ...]    compute passage embeddings

Prosodic:
  lltk prosodic-parse <corpus>           metrical scansion
  lltk prosodic-aggregate <corpus>       build prosodic.parquet

Web:
  lltk app [--port N]                    launch explorer web app
  lltk annotate <corpus> [--port N]      launch annotation interface
```

## Architecture

```
lltk/
+-- cli.py                 # CLI entry point
+-- text/
|   +-- text.py            # BaseText, TextSection, Text() factory
|   +-- textlist.py        # TextList collection class
+-- corpus/
|   +-- corpus.py          # BaseCorpus, SectionCorpus, Corpus() factory
|   +-- synthetic.py       # SyntheticCorpus -- virtual corpora from DB queries
|   +-- arc_corpora/       # CuratedCorpus subclasses (ArcFiction, etc.)
|   +-- manifest.txt       # Corpus registry (71 entries)
|   +-- <corpus_name>/     # Per-corpus implementations
+-- tools/
|   +-- metadb_ch.py       # MetaDBCH -- ClickHouse-backed lltk.db singleton
|   +-- annotations.py     # Priority-based annotation system
|   +-- genre_tags.py      # Genre tag materialization
|   +-- clickhouse_*.py    # CH build/query modules (match, rebuild, embeddings, ...)
|   +-- prosodic_tools.py  # Prosodic integration
|   +-- db_adapter.py      # Database adapter abstraction
+-- web/
    +-- app.py             # Explorer web app (FastAPI + Svelte)
    +-- annotate.py        # Annotation interface
```

Key patterns:

- **Inheritance:** `BaseObject` -> `TextList` -> `BaseCorpus` -> corpus subclasses
- **Factories:** `Text(id)` and `Corpus(id)` return cached objects
- **Lazy hydration:** Text metadata loaded from CH on first attribute access, CSV fallback
- **Path resolution:** `corpus.path_*` attributes resolved via `__getattr__` -> `get_path()`
- **Manifest:** Corpora registered in `manifest.txt` (configparser); merged from package dir + `~/lltk_data/` + user config
- **Parquet caching:** Metadata CSVs cached as `.parquet` for 5-10x faster subsequent reads

## Development

### Running tests

```bash
pip install pytest
python -m pytest tests/ -v
python -m pytest tests/ --cov=lltk --cov-report=term
```

374 tests using the `test_fixture` corpus (Blake, Austen, Shelley) checked into the repo -- no external data needed.

### Adding a new corpus

1. Create `lltk/corpus/my_corpus/my_corpus.py`:

```python
from lltk.imports import *

class TextMyCorpus(BaseText):
    pass

class MyCorpus(BaseCorpus):
    TEXT_CLASS = TextMyCorpus

    def load_metadata(self):
        meta = super().load_metadata()
        # add/transform columns here
        return meta
```

2. Register in `lltk/corpus/manifest.txt`:

```ini
[MyCorpus]
id = my_corpus
name = MyCorpus
desc = Description of the corpus
path_python = my_corpus/my_corpus.py
class_name = MyCorpus
```

3. Place data at `~/lltk_data/corpora/my_corpus/`:
   - `metadata.csv` with `id` column + any metadata columns
   - `txt/` text files as `<text_id>.txt`
   - `freqs/` (optional) precomputed word frequencies as JSON

## Available corpora

71 corpora across English, French, German, and Spanish. Some are freely downloadable, others require institutional access.

### English

| Corpus | Description | Period | License |
|--------|-------------|--------|---------|
| [EarlyPrint](https://earlyprint.org) | EEBO/ECCO/Evans TCP with linguistic tagging (~60K texts) | 1473-1800 | Free |
| [EEBO_TCP](https://textcreationpartnership.org/tcp-texts/eebo-tcp-early-english-books-online/) | Early English Books Online (TCP) | 1473-1700 | Free |
| [ECCO_TCP](https://textcreationpartnership.org/tcp-texts/ecco-tcp-eighteenth-century-collections-online/) | Eighteenth Century Collections Online (TCP) | 1701-1800 | Free |
| [ECCO](https://www.gale.com/intl/primary-sources/eighteenth-century-collections-online) | Eighteenth Century Collections Online (full) | 1701-1800 | Commercial |
| [ESTC](http://estc.ucr.edu/) | English Short Title Catalogue (481K bib. records) | 1473-1800 | Academic |
| [Chadwyck](http://collections.chadwyck.co.uk) | Chadwyck-Healey Fiction, Drama, Poetry | 1500-1900 | Mixed |
| [HathiEngLit](https://wiki.htrc.illinois.edu) | Hathi Trust fiction, drama, verse | 1700-1900 | Academic |
| [InternetArchive](https://archive.org/details/19thcennov) | 19th Century Novels (U of Illinois) | 1800-1900 | Free |
| [GaleAmericanFiction](https://www.gale.com/c/american-fiction-1774-1920) | Gale American Fiction | 1774-1920 | Academic |
| [OldBailey](https://www.oldbaileyonline.org/) | Old Bailey trial proceedings | 1674-1913 | Free |
| [CLMET](https://perswww.kuleuven.be/~u0044428/clmet3_0.htm) | Corpus of Late Modern English Texts | 1710-1920 | Academic |
| [COCA](https://www.english-corpora.org/coca/) | Corpus of Contemporary American English | 1990-2019 | Commercial |
| [COHA](https://www.english-corpora.org/coha/) | Corpus of Historical American English | 1820-2019 | Commercial |
| [Spectator](http://www.gutenberg.org/ebooks/12030) | The Spectator (1711-1714) | 1711-1714 | Free |
| [SOTU](https://www.kaggle.com/rtatman/state-of-the-union-corpus-1989-2017) | State of the Union Addresses | 1790-2017 | Free |

Plus: BPO, Chicago, DialNarr, EnglishDialogues, EvansTCP, GildedAge, LitLab, MarkMark, Sellers, SemanticCohort, TedJDH, and genre-specific Hathi subcorpora (Bio, Essays, Letters, Novels, Sermons, Stories, Tales, Treatises, Proclamations, Almanacs, Romances).

### Bibliography & reference

| Corpus | Description |
|--------|-------------|
| [FictionBiblio](https://en.wikipedia.org/wiki/English_novel) | 6,862 entries from 6 fiction bibliographies (1475-1799) |
| [RavenGarside](https://catalog.hathitrust.org/Record/004098100) | Bibliography of English Novels, 1770-1830 |
| [END](https://earlynovels.github.io/) | Early Novels Database: 2,002 MARCXML records (1660-1830) |

### French

| Corpus | Description | Size | License |
|--------|-------------|------|---------|
| [ARTFL](https://artfl-project.uchicago.edu) | Treasury of the French Language | 3.6K | Academic |
| FrenchPDBooks | French public domain books | 290K | Free |
| [Gallica](https://gallica.bnf.fr/) | Gallica literary fictions | 15.5K | Free |
| [PAIGE](https://github.com/PAIGE-project) | French fiction corpus | 3.2K | Academic |

### German

| Corpus | Description | Size | License |
|--------|-------------|------|---------|
| [DTA](http://www.deutschestextarchiv.de) | Deutsches Text Archiv | 3.3K | Free (CC BY-SA) |
| GermanPD | German public domain texts | 275K | Free |
| GermanFiction | Curated German literary fiction (1600-1799) | 140 | Academic |
| DeCorp | German fiction corpus | ~5K | Academic |

### Multilingual & other

| Corpus | Description |
|--------|-------------|
| [TxtLab](https://txtlab.org/2016/01/txtlab450/) | 450 novels in English, French, and German |
| SpanishPDBooks | Spanish public domain books |
| ImpactES | Spanish historical texts |

### Curated virtual corpora

These combine and deduplicate texts from multiple source corpora:

| Corpus | Description |
|--------|-------------|
| `arc_fiction` | English fiction across all sources, deduplicated |
| `arc_poetry` | English poetry across all sources |
| `arc_fiction_fr` | French fiction across all sources |
| `arc_fiction_de` | German fiction across all sources |
| `arc_biography` | English biography |
| `arc_essays` | English essays |
| `arc_sermons` | English sermons |
| `arc_periodical` | English periodicals |
