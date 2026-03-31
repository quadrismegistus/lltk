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
├── __init__.py         # Package entry: from .imports import *
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
    └── logs.py         # Logging
```

## Key patterns

- **Inheritance:** BaseObject → TextList → BaseCorpus → specific corpus classes
- **Text factory:** `Text(id)` returns cached text objects; `Corpus(id)` returns cached corpus objects
- **Lazy loading:** Metadata loaded on first access, texts created on demand
- **Path resolution:** `corpus.path_*` attributes resolved via `__getattr__` → `get_path()`, supporting relative and absolute paths
- **Sections:** `t.chapters`, `t.paragraphs`, `t.passages(n=500)` return `SectionCorpus` objects with `TextSection` children. XML sections parsed in-memory, passages respect sentence boundaries.
- **Manifest:** Corpora registered in `manifest.txt` (configparser format). Multiple manifest files merged from package dir, `~/lltk_data/`, and user config.
- **Metadata loading:** `C.meta` uses `load_metadata()` (fast CSV read) rather than per-text iteration. Results are cached in `_metadfd`. Subclasses override `load_metadata()` to enrich columns (e.g., ESTC adds format/extent/fiction fields).
- **Cross-corpus linking:** Corpora declare `LINKS = {target_corpus_id: (my_col, their_col)}` for shared-ID relationships. `merge_linked_metadata()` left-joins linked corpus metadata with prefixed columns (many-to-one). `t.linked(corpus_id)` traverses to linked text objects (one-to-many). Lookup dicts are built once and cached.

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

## Development notes

- `__getattr__` on BaseCorpus handles `path_*` attributes; raises `AttributeError` for everything else (not silently returning None)
- `BaseText.get(key)` does fuzzy metadata lookup (`ish=True`): searches for keys starting with `key`, handles corpus-prefixed metadata keys
- The `_init` attribute on corpus objects tracks initialization state (set/bool, not consistent — be aware)
- `SectionCorpus.parse_sections()` is the override hook for custom section parsing; `init()` handles caching
- Corpus downloads use Dropbox URLs defined in manifest `url_*` fields
- `load_metadata()` is cached via `_metadfd` with key `('load_metadata', clean)`. Subclass overrides (ESTC, ECCO, EEBO) are also cached.
- `BaseText.linked()` has a class-level `_linked_cache` dict keyed by `(source_corpus_id, target_corpus_id)` storing `(target_corpus, target_meta, lookup_dict)`. The lookup dict maps link column values to lists of text IDs for O(1) traversal.
- `BaseText.linked()` existed before as a graph DB wrapper; the new version adds an optional `target_corpus_id` positional arg and falls back to the old behavior when called without arguments.
