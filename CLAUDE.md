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
│   ├── corpus.py       # BaseCorpus, SectionCorpus, Corpus() factory
│   ├── synthetic.py    # SyntheticCorpus — virtual corpora from DuckDB queries
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
- **Lazy text hydration:** `C.texts()` constructs bare text shells (just `id` + `_corpus` ref). Metadata is hydrated lazily on first attribute access (`t.author`, `t.year`, `t.get(key)`) via `_hydrate_meta()`, which tries a DuckDB indexed lookup first (`lltk.db.get()`), then falls back to `corpus.load_metadata().loc[id]`. Hydration runs once per text (guarded by `_meta_hydrated` flag).
- **Path resolution:** `corpus.path_*` attributes resolved via `__getattr__` → `get_path()`, supporting relative and absolute paths
- **Manifest:** Corpora registered in `manifest.txt` (configparser format). Multiple manifest files merged from package dir, `~/lltk_data/`, and user config.
- **Metadata loading:** `C.meta` uses `load_metadata()` (fast CSV read) rather than per-text iteration. Results are cached in `_metadfd`. Subclasses override `load_metadata()` to enrich columns.
- **Cross-corpus linking:** Corpora declare `LINKS = {target_corpus_id: (my_col, their_col)}` for shared-ID relationships. `merge_linked_metadata()` left-joins linked corpus metadata with prefixed columns.

## MetaDB (centralized DuckDB metadata store)

`lltk.db` is a DuckDB-backed metadata cache for fast single-row lookups, cross-corpus queries, dedup, and virtual corpus construction. CSV files and `load_metadata()` remain the source of truth; the DB is a read cache that must be explicitly rebuilt when source data or enrichment logic changes.

### Database files

Two separate DuckDB files connected via ATTACH:

| File | Contents | Size |
|------|----------|------|
| `~/lltk_data/data/metadb.duckdb` | `texts` table, `corpus_info` table | ~300MB |
| `~/lltk_data/data/metadb_matches.duckdb` | `matches` table, `match_groups` table | ~165MB |

Single DuckDB connection opens `metadb.duckdb` and ATTACHes `metadb_matches.duckdb` as `match_db`. All queries go through one connection. Match tables are prefixed `match_db.matches`, `match_db.match_groups` in SQL. Texts table is plain `texts`.

The split means matches can be deleted/rebuilt independently without touching the texts metadata. Delete `metadb_matches.duckdb` and re-run `lltk db-match` to rebuild matches only.

### Schema

**`texts` table** (in `metadb.duckdb`):

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
| `title_norm` | TEXT | normalized title for matching (indexed) |
| `author_norm` | TEXT | normalized author last name (indexed) |
| `meta` | TEXT (JSON) | all other corpus-specific fields |

**`corpus_info` table** (in `metadb.duckdb`): `corpus TEXT PK, ingested_at DOUBLE, n_texts INTEGER`

**`matches` table** (in `metadb_matches.duckdb`, accessed as `match_db.matches`):
`_id_a TEXT, _id_b TEXT, similarity FLOAT, match_type TEXT, PRIMARY KEY (_id_a, _id_b)`

**`match_groups` table** (in `metadb_matches.duckdb`, accessed as `match_db.match_groups`):
`_id TEXT PK, group_id INTEGER, rank INTEGER`

### Title and author normalization

Computed at ingest time, stored as indexed columns for fast matching:

**`normalize_title(title)`**: lowercase, strip subtitle after first `:;.([,!?`, collapse whitespace. E.g. `"Incognita: or, Love and duty reconcil'd. A novel."` → `"incognita"`. Also strips title-end phrases ("a novel", "by the author", "edited by", etc.).

**`normalize_author(author)`**: lowercase, take text before first comma. E.g. `"Congreve, William, 1670-1729."` → `"congreve"`.

### Blacklist

`DB_BLACKLIST = {'hathi', 'bighist'}` — corpora excluded from DB ingest. `hathi` parent corpus has 17M texts (government documents, serial publications) that cause matching explosions. Use subcorpora instead (hathi_englit, hathi_novels, etc.). `bighist` is a composite.

### Standard metadata contract

Every corpus's `load_metadata()` should return a DataFrame with at least: `id` (index), `title`, `author`, `year`, `genre`, `genre_raw`.

**`GENRE_VOCAB`**: Fiction, Poetry, Drama, Periodical, Essay, Treatise, Letters, Sermon, Biography, History, Nonfiction, Legal, Speech, Spoken, Criticism, Academic, Almanac, Reference.

`genre` = broad harmonized category. `genre_raw` = most specific true label (e.g. `Novel`, `Epistolary fiction`, `Ballad/Song`).

### Cross-corpus matching and dedup

Matching finds duplicate/reprint texts both within and across corpora using `title_norm` + `author_norm`.

**Algorithm**: Chain linking via SQL `LEAD()` window function. For N texts with the same title+author, stores N-1 edges (a chain) instead of N*(N-1)/2 (all pairs). Connected components via NetworkX produce identical groups with 94% fewer stored pairs.

**Match groups**: Each text in a match group gets a `rank` based on `CORPUS_SOURCE_RANKS` (chadwyck=1, canon_fiction=2, ... hathi_englit=9, internet_archive=12). Rank 0 = preferred representative.

**Dedup modes**: `dedup_by='rank'` picks preferred corpus source. `dedup_by='oldest'` picks earliest year, breaking ties by rank.

**Scale**: ~2M texts across 50 corpora → 489K match pairs, 170K groups, runs in ~5 seconds.

### CLI

```bash
lltk db-rebuild                          # drop all + rebuild all corpora (~4 min)
lltk db-rebuild estc ecco                # re-ingest specific corpora (rest untouched)
lltk db-info                             # genre × corpus crosstab with totals
lltk db-match                            # run exact title+author matching (~5 sec)
lltk db-match --fuzzy                    # also run fuzzy matching (~15 sec)
lltk db-matches "Incognita"              # search matches by title
lltk db-match-stats                      # show matching statistics
```

### Python API

```python
import lltk

# ── Ingest ──
lltk.db.ingest('estc')                   # one corpus
lltk.db.rebuild()                         # all corpora from manifest
lltk.db.rebuild(['estc', 'ecco'])         # specific list

# ── Metadata queries ──
lltk.db.get('_estc/T012345')             # single-row lookup by _id → dict
lltk.db.get('estc', 'T012345')           # single-row lookup by corpus + id → dict
lltk.db.query("SELECT * FROM texts WHERE year < 1700 AND genre = 'Fiction'")
lltk.db.query("SELECT corpus, COUNT(*) as n FROM texts GROUP BY corpus")

# ── Matching ──
lltk.db.match()                           # exact title+author matching
lltk.db.match(fuzzy=True)                # + fuzzy matching
lltk.db.match(corpora=['estc', 'ecco'])  # match specific corpora only
lltk.db.find_matches('Incognita')        # search match groups by title
lltk.db.get_group('_estc/T012345')       # all texts in same match group
lltk.db.match_stats()                    # summary statistics
lltk.db.drop_matches()                   # clear all matches (keeps texts)

# ── Virtual corpus queries (returns real text objects) ──
for t in lltk.db.texts(genre='Fiction', dedup=True, dedup_by='oldest'):
    print(t.corpus.id, t.title, t.year)
    print(t.txt[:100])     # works — resolves through source corpus
    print(t.freqs())       # works — resolves through source corpus

# With filters
for t in lltk.db.texts(genre='Poetry', year_min=1600, year_max=1800):
    print(t.title)

# As DataFrame (no text objects)
df = lltk.db.texts_df(genre='Fiction', dedup=True)

# As corpus object (for .mfw, .dtm, .meta)
fiction = lltk.db.corpus(genre='Fiction', dedup=True)
fiction.meta

# ── Validation ──
lltk.db.validate()                        # % non-null for standard cols per corpus
lltk.db.validate_genres()                 # distinct genre values per corpus
lltk.db.corpora()                         # list ingested corpora with row counts
lltk.db.corpus_info()                     # ingest timestamps per corpus
```

### SyntheticCorpus (virtual corpora from DB queries)

Declarative corpus class backed by DuckDB queries. Pulls texts from multiple source corpora, deduplicated. Text objects retain their original corpus for file access.

```python
from lltk.corpus.synthetic import SyntheticCorpus

class BigFiction(SyntheticCorpus):
    ID = 'big_fiction'
    NAME = 'BigFiction'
    SOURCES = {
        'canon_fiction': {'genre': 'Fiction'},
        'chadwyck': {'genre': 'Fiction'},
        'gildedage': {},
        'hathi_englit': {'genre': 'Fiction'},
        'estc': {'genre': 'Fiction'},
    }
    DEDUP = True
    DEDUP_BY = 'oldest'   # or 'rank'

C = BigFiction()
C.meta                    # DataFrame — all fiction, deduplicated
for t in C.texts():
    t.corpus.id           # 'chadwyck' (original source, not 'big_fiction')
    t.txt[:100]           # works — resolves through source corpus paths
```

### How text objects work across corpora

Text objects created by `lltk.db.texts()` or `SyntheticCorpus.texts()` keep their original `corpus` reference. This means:

- `t.corpus.id` → the real source corpus (e.g. `'chadwyck'`)
- `t.path_txt` → resolves through the source corpus's path configuration
- `t.path_freqs` → resolves through the source corpus (including Hathi freqs index)
- `t.txt` → reads from the source corpus's txt directory
- `t.freqs()` → reads from the source corpus's freqs directory

The virtual/synthetic corpus is just a view — it selects which texts to include, the text objects handle their own file access.

### Data flow summary

```
metadata.csv → load_metadata() → DataFrame (cached in _metadfd)
                                      ↓
                                  lltk.db.ingest() → metadb.duckdb (texts table)
                                                          ↓
                                  lltk.db.match()  → metadb_matches.duckdb (matches, match_groups)
                                                          ↓
                                  lltk.db.texts()  → text objects (from source corpora)
```

`load_metadata()` is the source of truth. The DB caches its output. Matching operates on the DB. Virtual corpus queries return real text objects that delegate file access to their source corpus.

### For the abstraction project

The abstraction project can:

1. **Query LLTK's DuckDB directly** for metadata, genre, year, dedup:
   ```python
   df = lltk.db.texts_df(genre='Fiction', dedup=True, dedup_by='oldest')
   ```

2. **Get text objects with working `.freqs()`** for scoring:
   ```python
   for t in lltk.db.texts(genre='Fiction', dedup=True):
       freqs = t.freqs()  # works — resolves through source corpus
   ```

3. **Write scores to a separate DuckDB file** (not LLTK's):
   ```python
   import duckdb
   scores_conn = duckdb.connect('abstraction_scores.duckdb')
   scores_conn.execute("ATTACH '~/lltk_data/data/metadb.duckdb' AS lltk (READ_ONLY)")
   scores_conn.execute("""
       SELECT t.*, s.* FROM lltk.texts t
       JOIN scores s ON t._id = s._id
       WHERE t.genre = 'Fiction'
   """)
   ```

4. **Use `_id` as the join key** between LLTK metadata and abstraction scores. The `_id` format is `_{corpus}/{text_id}` (e.g. `_chadwyck/Early_English_Prose_Fiction/ee28010.01`). This matches what LLTK uses internally. For freqs-based scoring, the `text_id` part of `_id` should match the freqs filename (this is what the Hathi ID normalization ensures).

5. **Check staleness** via `lltk.db.corpus_info()` which shows `ingested_at` timestamps per corpus.

## Genre classification

Genre assignment happens in each corpus's `load_metadata()`. The approach varies by corpus type:

### ESTC genre classification (`estc.py`)

`classify_genres(form, subject_topic, title, title_sub)` classifies ESTC records using three tiers:
1. **form** field (MARC 655_a, most reliable — cataloger-assigned)
2. **subject_topic** field (MARC 650_a, cataloger-assigned but noisier)
3. **title keywords** (last resort fallback, only fires if tiers 1+2 found nothing)

Returns a set of fine-grained genre labels from `GENRE_RULES` (40+ genres). Mapped to broad `GENRE_VOCAB` via `_genres_to_harmonized()`.

Key design decisions:
- `history` removed from title keywords — too many novels use "History of..."
- Satire maps to `None` — cross-cutting mode, not a genre. When co-occurring with Poetry/Drama/Fiction, those win.
- `FICTION_GENRES` = {Fiction, Novel, Romance, Tale, Fable, Picaresque, Epistolary fiction, Imaginary voyage}

### Linked corpora (ECCO, EEBO_TCP, ECCO_TCP)

Inherit genre from ESTC via `merge_linked_metadata()`:
- **ECCO**: links via `ESTCID` → `id_estc`. Copies `estc_genre` → `genre`.
- **EEBO_TCP**: links via `id_stc` → `id_estc` (zero-padding). Own `genre` renamed to `medium`. If medium=Verse → genre=Poetry. If medium=Drama → genre=Drama.
- **ECCO_TCP**: same pattern as EEBO.

### Simple corpora (all one genre)

Examples: gildedage, chicago, ravengarside, txtlab (Fiction); chadwyck_poetry (Poetry); sotu (Speech); oldbailey (Legal); hathi_novels/stories/tales/romances (Fiction).

### Mapping corpora

- **COCA**: FIC→Fiction, MAG/NEWS→Periodical, ACAD→Academic, SPOK→Spoken
- **COHA**: Magazine/News→Periodical, Non-Fiction→Nonfiction, Film→Drama
- **BPO**: Fiction, Poem→Poetry, Correspondence→Letters, Review→Criticism, News→Periodical
- **canon_fiction**: major_genre Verse/Epic→Poetry, Drama→Drama, History→History, rest→Fiction
- **litlab**: fine-grained subgenres→Fiction (raw preserved in genre_raw)

### Not yet harmonized

- **dta** (Deutsches Textarchiv, 3K) — German corpus

## Hathi ID normalization

`hathi_id_normalize()` collapses all HathiTrust ID variants to canonical flat form `{library}/{volume_id}`:

```
mdp/390/15009144422      → mdp/39015009144422       (3-char dir split)
bc/ark/+=13960=t0bv7v96f → bc/ark+=13960=t0bv7v96f  (3-char split ark)
aeu/ark:/13960/t0000ds1j → aeu/ark+=13960=t0000ds1j (colon-slash ark)
```

Applied in `load_metadata()` for all Hathi corpora. Freqs index (`_build_freqs_index()`) maps `canonical_id → filepath` on disk. Hathi subcorpora share a freqs pool at `~/lltk_data/corpora/hathi/freqs` via manifest `path_freqs = ../hathi/freqs`.

## Running tests

```bash
python -m pytest tests/ -v
```

Tests use the `test_fixture` corpus (3 texts: Blake, Austen, Shelley) checked into the repo — no external data needed.

## Corpus data location

- Corpora live at `~/lltk_data/corpora/<corpus_id>/`
- Each corpus has: `metadata.csv`, `txt/`, optionally `xml/`, `freqs/`
- Text files: `txt/<text_id>.txt` (flat) or `texts/<text_id>/text.txt` (per-text dirs)
- Manifest files searched in: package `corpus/manifest.txt`, `~/lltk_data/manifest.txt`, and others
- Text metadata DB: `~/lltk_data/data/metadb.duckdb`
- Match/dedup DB: `~/lltk_data/data/metadb_matches.duckdb`

## Performance

- **Parquet caching**: `BaseCorpus.load_metadata()` caches CSV as `.parquet` next to the CSV. 5-10x faster reads. Auto-regenerated if CSV is newer.
- **Enriched parquet**: ESTC, ECCO, EEBO_TCP, ECCO_TCP cache full enrichment (genre classification, linked metadata) as `metadata_enriched.parquet`. Skips all enrichment on subsequent loads. `load_metadata(force=True)` bypasses.
- **Pre-populated text metadata**: `iter_init()` passes DataFrame row directly to each text constructor, sets `_meta_hydrated=True`. No per-text DuckDB lookups when iterating via `C.texts()`.
- **pmap**: Built-in parallel map using `concurrent.futures` (replaced yapmap). ThreadPoolExecutor for I/O-bound, ProcessPoolExecutor for CPU-bound. `DEFAULT_NUM_PROC = cpu_count - 2`.

## Annotation web app

```bash
lltk annotate arc_fiction          # launch on http://0.0.0.0:8989
lltk annotate arc_fiction --port 9000
```

FastAPI app for browsing and annotating CuratedCorpus metadata:
- **Table view**: paginated, filterable (corpus, genre, year, translated, search), deduped
- **Detail panel**: full metadata, text preview (~10K words), match group with links
- **Annotation form**: genre (dropdown from GENRE_VOCAB + clear option), genre_raw (autocomplete datalist), is_translated, exclude, notes, dynamic custom fields
- **Bulk actions**: select multiple → exclude or set genre
- **Manual duplicate linking**: search + link button in match group panel
- **Auto-reload**: code changes auto-restart server
- **Annotations**: saved to `~/lltk_data/corpora/{corpus_id}/annotations.json`, keyed by `_id`
- **Propagation**: annotations propagate across match groups (annotate eebo_tcp → earlyprint version inherits)

## CuratedCorpus

Extends SyntheticCorpus with annotations.json support:

```python
class ArcFiction(CuratedCorpus):
    SOURCES = {'chadwyck': {}, 'earlyprint': {'genre': 'Fiction'}, ...}
    DEDUP = True
    DEDUP_BY = 'oldest'
```

- `annotations.json`: overrides DB values for individual texts. Keyed by `_id`.
- `exclude` field: any truthy value removes text from corpus
- `__none__` sentinel: explicitly clears a field (vs no-override)
- `annotate()`: launches web app
- Annotations propagate across match groups via `match_db.match_groups`

## EarlyPrint corpus

Combined EEBO/ECCO/Evans TCP with linguistic tagging from [EarlyPrint Project](https://earlyprint.org). ~66K texts.

```bash
lltk compile earlyprint                    # all repos
lltk compile earlyprint --repos eccotcp    # one at a time
```

- Shallow git clones + gzip-compressed XML copies (~10x smaller)
- Rich TEI header parser: title, author, year, IDs, quality grades, word counts
- Medium detection from body tag counts (Verse/Drama/Prose)
- `LINKS` to ESTC for genre. `MATCH_LINKS` to eebo_tcp/ecco_tcp/evans_tcp for dedup.
- `update()`: git pull + re-gzip + rebuild metadata
- See `lltk/corpus/earlyprint/README.md` for full field reference

## Development notes

- `__getattr__` on BaseCorpus handles `path_*` attributes; raises `AttributeError` for everything else
- `BaseText.get(key)` does fuzzy metadata lookup (`ish=True`): searches for keys starting with `key`. Calls `_hydrate_meta()` first.
- `metadata_initial()` calls `_hydrate_meta()` so `t.meta` works on bare text shells
- `_corpus_meta_row()` tries DB lookup, then checks cached `_metadfd` before triggering expensive `load_metadata()`
- `iter_init()` pre-populates text._meta from DataFrame and sets `_meta_hydrated=True`
- `iter_texts()` uses objects from `_textd` directly (no `Text()` factory wrapping)
- `get_idx()` preserves spaces, `+`, `$` in IDs (no longer forces snake_case)
- `CORPUS_SOURCE_RANKS` in `metadb.py` defines preference order for dedup
- `MATCH_LINKS`: ID-based matching without metadata merge (separate from `LINKS`)
- `is_translated`: ESTC detection via title/notes/subject, inherited by linked corpora, core DB column
