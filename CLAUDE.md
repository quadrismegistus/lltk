# CLAUDE.md

## Project overview

LLTK (Literary Language Toolkit) is a Python package for computational literary analysis and digital humanities research. It provides 50+ literary corpora, text processing tools, and analysis methods (word frequencies, document-term matrices, most frequent words).

**Author:** Ryan Heuser
**Package name:** `lltk-dh` (PyPI)
**Version:** 0.7.0
**License:** MIT
**Python:** >=3.8

## Architecture

```
lltk/
├── imports.py          # Constants, config, third-party imports, logger setup
├── __init__.py         # Package entry: from .imports import *; exposes lltk.db (MetaDB)
├── text/
│   ├── text.py         # BaseText, TextSection, Text() factory
│   ├── textlist.py     # TextList collection class
│   └── utils.py        # Tokenization, XML parsing, text utilities
├── corpus/
│   ├── corpus.py       # BaseCorpus, SectionCorpus, ParagraphSectionCorpus, PassageSectionCorpus, Corpus() factory
│   ├── utils.py        # load_corpus(), manifest loading, corpus discovery
│   ├── manifest.txt    # Corpus registry (configparser format)
│   ├── test_fixture/   # Test corpus checked into repo (3 texts + XML)
│   └── <corpus_name>/  # Per-corpus implementations (50+)
├── model/
│   ├── preprocess.py   # Preprocessing (XML→TXT, TXT→freqs)
│   ├── matcher.py      # Text matching/dedup by title
│   └── ...             # word2vec, doc2vec, characters, networks, etc.
└── tools/
    ├── baseobj.py      # BaseObject (root class)
    ├── tools.py        # Config, utilities, parallel mapping
    ├── db.py           # Local DB backends (sqlite, tinydb, etc.)
    ├── metadb.py       # DuckDB centralized metadata store (lltk.db)
    └── logs.py         # Logging
```

## Key patterns

- **Inheritance:** BaseObject → TextList → BaseCorpus → specific corpus classes
- **Text factory:** `Text(id)` returns cached text objects; `Corpus(id)` returns cached corpus objects
- **Lazy loading:** Metadata loaded on first access, texts created on demand
- **Lazy text hydration:** `C.texts()` constructs bare text shells (just `id` + `_corpus` ref). Metadata is hydrated lazily on first attribute access (`t.author`, `t.year`, `t.get(key)`) via `_hydrate_meta()`, which tries a DuckDB indexed lookup first (`lltk.db.get()`), then falls back to `corpus.load_metadata().loc[id]`. Hydration runs once per text (guarded by `_meta_hydrated` flag) and populates `_meta` with corpus DataFrame values as the base layer, with any local overrides winning.
- **Path resolution:** `corpus.path_*` attributes resolved via `__getattr__` → `get_path()`, supporting relative and absolute paths
- **Sections:** `t.chapters`, `t.paragraphs`, `t.passages(n=500)` return `SectionCorpus` objects with `TextSection` children. XML sections parsed in-memory, passages respect sentence boundaries.
- **Manifest:** Corpora registered in `manifest.txt` (configparser format). Multiple manifest files merged from package dir, `~/lltk_data/`, and user config.
- **Metadata loading:** `C.meta` uses `load_metadata()` (fast CSV read) rather than per-text iteration. Results are cached in `_metadfd`. Subclasses override `load_metadata()` to enrich columns (e.g., ESTC adds format/extent/fiction fields).
- **Cross-corpus linking:** Corpora declare `LINKS = {target_corpus_id: (my_col, their_col)}` for shared-ID relationships. `merge_linked_metadata()` left-joins linked corpus metadata with prefixed columns (many-to-one). `t.linked(corpus_id)` traverses to linked text objects (one-to-many). Lookup dicts are built once and cached.

## MetaDB (centralized DuckDB metadata store)

`lltk.db` is a DuckDB-backed metadata cache for fast single-row lookups and cross-corpus queries. CSV files and `load_metadata()` remain the source of truth; the DB is a read cache that must be explicitly rebuilt when source data or enrichment logic changes.

### Schema

Single `texts` table with `_id` (e.g. `_estc/T012345`) as primary key, `corpus` indexed. Core columns stored as real columns:

| Column | Type | Description |
|--------|------|-------------|
| `_id` | TEXT PK | `_{corpus}/{id}` — canonical text address |
| `corpus` | TEXT NOT NULL | corpus identifier, indexed |
| `id` | TEXT NOT NULL | text id within corpus |
| `title` | TEXT | |
| `author` | TEXT | |
| `year` | INTEGER | parsed to int at ingest (handles ranges, circa dates) |
| `genre` | TEXT | harmonized to `GENRE_VOCAB` standard vocabulary |
| `genre_raw` | TEXT | raw genre value from corpus metadata |
| `meta` | TEXT (JSON) | all other corpus-specific fields |

All other corpus-specific metadata is packed into the `meta` JSON column. This keeps the table at 9 columns regardless of how many corpora are ingested, avoids column name collisions, and means re-ingesting one corpus can't affect the schema. DuckDB JSON functions can query into the blob: `meta->>'is_fiction'`, `json_extract(meta, '$.format_std')`.

A `corpus_info` table tracks ingest timestamps (`corpus TEXT PK, ingested_at DOUBLE, n_texts INTEGER`). Use `lltk.db.corpus_info()` to check when each corpus was last ingested.

### Standard metadata contract

Every corpus's `load_metadata()` should return a DataFrame with at least: `id` (index), `title`, `author`, `year`, `genre`, `genre_raw`. Genre normalization is corpus-specific — each corpus's `load_metadata()` is responsible for setting `genre_raw` (whatever the source says) and `genre` (harmonized to `GENRE_VOCAB`). The standard genre vocabulary is defined in `lltk.tools.metadb.GENRE_VOCAB`: Fiction, Poetry, Drama, Periodical, Essay, Treatise, Letters, Sermon, Biography, Nonfiction, Legal, Political, Criticism, Almanac, Reference. `validate_genres()` flags values not in this set. `year` should be an integer; the DB ingest parses ranges/circa dates via `_parse_year()`. Use `lltk.db.validate()` to check coverage.

### Usage

```python
import lltk

# Ingest
lltk.db.ingest('estc')                   # one corpus
lltk.db.rebuild()                         # all corpora from manifest
lltk.db.rebuild(['estc', 'ecco'])         # specific list

# Query
lltk.db.get('_estc/T012345')             # single-row lookup by _id
lltk.db.get('estc', 'T012345')           # single-row lookup by corpus + id
lltk.db.query("SELECT * FROM texts WHERE year < 1700 AND genre = 'fiction'")
lltk.db.query("SELECT corpus, COUNT(*) as n FROM texts GROUP BY corpus")

# Cross-corpus queries
lltk.db.query("""
    SELECT a._id, b._id, a.corpus, b.corpus
    FROM texts a JOIN texts b ON a.title = b.title AND a.corpus != b.corpus
""")

# Validate metadata coverage
lltk.db.validate()                        # shows % non-null for standard cols per corpus
lltk.db.validate('estc')                  # one corpus
lltk.db.validate_genres()                 # distinct genre values per corpus, flags non-standard

# Manage
lltk.db.corpora()                         # list ingested corpora with row counts
lltk.db.corpus_info()                     # ingest timestamps per corpus
lltk.db.drop('estc')                      # clear one corpus
lltk.db.drop()                            # clear everything
```

### Invalidation

The DB does not auto-detect changes to `metadata.csv` or `load_metadata()` enrichment logic. After modifying source data or corpus Python code (e.g. genre classification functions), explicitly rebuild:

```python
lltk.db.rebuild(['estc'])    # re-ingest affected corpora
```

### How text hydration uses the DB

When `t.author`, `t.year`, `t.get('genre')`, etc. are accessed on a text object, `_hydrate_meta()` fires once and populates `_meta`. It tries `lltk.db.get(corpus_id, text_id)` first (fast indexed DuckDB lookup, returns core cols + unpacked meta JSON, no DataFrame in memory), falling back to `corpus.load_metadata().loc[id]` if the corpus hasn't been ingested into the DB. This means enriched fields from `load_metadata()` overrides (e.g. ESTC linked fields on EEBO_TCP texts) are available on individual text objects.

### DB location

`~/lltk_data/data/metadb.duckdb`

## Running tests

```bash
python -m pytest tests/ -v
```

Tests use the `test_fixture` corpus (3 texts: Blake, Austen, Shelley) checked into the repo — no external data needed.

## Common operations

```python
import lltk

# Load a corpus
c = lltk.load('canon_fiction')

# Access metadata
c.meta                          # pandas DataFrame
c.meta.query('1700 < year < 1800')

# Iterate texts
for t in c.texts():
    print(t.id, t.author, t.title, t.year)
    print(t.txt[:100])          # plain text
    print(t.freqs())            # word frequencies (Counter)

# Corpus-level analysis
c.mfw(n=10000)                  # most frequent words
c.dtm(n=10000)                  # document-term matrix
c.dtm(n=10000, tfidf=True)     # TF-IDF weighted

# Sections (chapters from XML)
for ch in t.chapters.texts():
    print(ch.get('title'), ch.txt[:100])

# Paragraphs and passages
for p in t.paragraphs.texts():
    print(p.id, p.txt[:50])

for p in t.passages(n=500).texts():
    print(p.id, p.get('num_words'), p.freqs())

# Cross-corpus linking (ESTC ↔ ECCO/EEBO)
ecco = lltk.load('ecco')
ecco.meta                       # has estc_author, estc_format_std, etc.

estc = lltk.load('estc')
t = estc.text('some_id')
t.linked('ecco')                # → [TextECCO(...), ...]
t.linked('eebo_tcp')            # → [TextEEBO_TCP(...), ...]
```

## Corpus data location

- Corpora live at `~/lltk_data/corpora/<corpus_id>/`
- Each corpus has: `metadata.csv`, `txt/`, optionally `xml/`, `freqs/`
- Text files: `txt/<text_id>.txt` (flat) or `texts/<text_id>/text.txt` (per-text dirs)
- Manifest files searched in: package `corpus/manifest.txt`, `~/lltk_data/manifest.txt`, and others
- Centralized DB: `~/lltk_data/data/metadb.duckdb`

## Development notes

- `__getattr__` on BaseCorpus handles `path_*` attributes; raises `AttributeError` for everything else (not silently returning None)
- `BaseText.get(key)` does fuzzy metadata lookup (`ish=True`): searches for keys starting with `key`, handles corpus-prefixed metadata keys. Calls `_hydrate_meta()` to ensure corpus-level metadata is loaded before searching.
- `BaseText.meta_()` also calls `_hydrate_meta()` — this is the path used by `t.year`, `t.years`, etc.
- The `_init` attribute on corpus objects tracks initialization state (set/bool, not consistent — be aware)
- `iter_init()` uses `load_metadata()` to get the ID list and constructs bare `TEXT_CLASS` shells with no metadata kwargs — metadata is hydrated lazily via `_hydrate_meta()` on first access
- `iter_texts()` no longer wraps text objects through the `Text()` factory — objects from `_textd` are used directly
- `SectionCorpus.parse_sections()` is the override hook for custom section parsing; `init()` handles caching
- Corpus downloads use Dropbox URLs defined in manifest `url_*` fields
- `load_metadata()` is cached via `_metadfd` with key `('load_metadata', clean)`. Subclass overrides (ESTC, ECCO, EEBO) are also cached.
- `BaseText.linked()` has a class-level `_linked_cache` dict keyed by `(source_corpus_id, target_corpus_id)` storing `(target_corpus, target_meta, lookup_dict)`. The lookup dict maps link column values to lists of text IDs for O(1) traversal.
- `BaseText.linked()` existed before as a graph DB wrapper; the new version adds an optional `target_corpus_id` positional arg and falls back to the old behavior when called without arguments.
