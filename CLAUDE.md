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

Three separate DuckDB files connected via ATTACH:

| File | Alias | Contents | Lifecycle |
|------|-------|----------|-----------|
| `~/lltk_data/data/metadb.duckdb` | (main) | `texts` table, `corpus_info` table | Rebuilt by `db-rebuild` |
| `~/lltk_data/data/metadb_matches.duckdb` | `match_db` | `matches` table, `match_groups` table | Rebuilt by `db-match` |
| `~/lltk_data/data/metadb_wordcounts.duckdb` | `wc_db` | `wordcounts` table (path_freqs → n_words) | Persistent cache, survives rebuilds |

Single DuckDB connection opens `metadb.duckdb` and ATTACHes both other files. Match tables prefixed `match_db.*`, wordcount table prefixed `wc_db.*`. Texts table is plain `texts`.

The split means matches can be deleted/rebuilt independently. Wordcounts persist across all rebuilds — `ingest_df()` backfills `n_words` from the cache automatically.

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
| `genre` | TEXT | **Enriched genre** — may differ from corpus original after `db-enrich-genres` |
| `genre_raw` | TEXT | fine-grained genre (e.g. Novel, Novel epistolary, Romance) — also enriched |
| `genre_corpus` | TEXT | original genre from corpus `load_metadata()` (pre-enrichment) |
| `genre_enriched_source` | TEXT | provenance: `corpus`, `form`, `topic`, `title`, `bibliography:fiction_biblio`, etc. |
| `title_norm` | TEXT | normalized title for matching (indexed) |
| `author_norm` | TEXT | normalized author last name (indexed) |
| `path_freqs` | TEXT | freqs file path relative to PATH_CORPUS (NULL if no freqs) |
| `n_words` | INTEGER | word count from freqs (sum of values). Cached in `wc_db`, backfilled on ingest |
| `meta` | TEXT (JSON) | all other corpus-specific fields |

**`corpus_info` table** (in `metadb.duckdb`): `corpus TEXT PK, ingested_at DOUBLE, n_texts INTEGER`

**`wordcounts` table** (in `metadb_wordcounts.duckdb`, accessed as `wc_db.wordcounts`): `path_freqs TEXT PK, n_words INTEGER`. Persistent cache — survives `db-rebuild`.

**`matches` table** (in `metadb_matches.duckdb`, accessed as `match_db.matches`):
`_id_a TEXT, _id_b TEXT, similarity FLOAT, match_type TEXT, PRIMARY KEY (_id_a, _id_b)`

**`match_groups` table** (in `metadb_matches.duckdb`, accessed as `match_db.match_groups`):
`_id TEXT PK, group_id INTEGER, rank INTEGER`

### Title and author normalization

Computed at ingest time, stored as indexed columns for fast matching:

**`normalize_title(title)`**: HTML entity unescape, Unicode dash normalization, strip `[]` brackets, strip abbreviation periods (Mr., Mrs., Dr., St., Q., single letters), modernize early modern spelling (MorphAdorner, 358K entries: u/v, vv/w, i/j, terminal -e, etc.), lowercase, strip subtitle after first `:;.([,!?`, collapse whitespace. E.g. `"Loues load-starre"` → `"loves loadstar"`, `"Love&hyphen;Letters Between a Noble&hyphen;Man"` → `"loveletters between a nobleman"`, `"The Life and Death of Mr. Badman"` → `"the life and death of mr badman"`. Also strips title-end phrases ("a novel", "by the author", "edited by", etc.).

**`normalize_author(author)`**: lowercase, take text before first comma. E.g. `"Congreve, William, 1670-1729."` → `"congreve"`.

### Blacklist

`DB_BLACKLIST = {'hathi', 'bighist'}` — corpora excluded from DB ingest. `hathi` parent corpus has 17M texts (government documents, serial publications) that cause matching explosions. Use subcorpora instead (hathi_englit, hathi_novels, etc.). `bighist` is a composite.

### Standard metadata contract

Every corpus's `load_metadata()` should return a DataFrame with at least: `id` (index), `title`, `author`, `year`, `genre`, `genre_raw`.

**`GENRE_VOCAB`**: Fiction, Poetry, Drama, Periodical, Essay, Treatise, Letters, Sermon, Biography, History, Nonfiction, Legal, Speech, Spoken, Criticism, Academic, Almanac, Reference.

`genre` = broad harmonized category. `genre_raw` = most specific true label (e.g. `Novel`, `Epistolary fiction`, `Ballad/Song`).

### Cross-corpus matching and dedup

Matching finds duplicate/reprint texts both within and across corpora via multiple tiers:

| Tier | match_type | Constraint | Notes |
|------|-----------|-----------|-------|
| 0 | `id_link` | Shared IDs from LINKS/MATCH_LINKS | SQL, instant |
| 1a | `exact_norm` | title_norm + author_norm | SQL LEAD() chain linking |
| 1b | `exact_norm_year` | title_norm + year (authorless only) | SQL LEAD(), min title 11 chars |
| 2a | `containment` | short title `in` long title, same author | Python, min_sim=0.3, min 8 chars |
| 2b | `containment_year` | short title `in` long title, same year (authorless) | Python, min_sim=0.3, min 15 chars |
| 3 | `fuzzy_title` | Jaro-Winkler > 0.85, same author (opt-in `--fuzzy`) | Python, slow |

**Chain linking**: SQL `LEAD()` window function. For N texts with the same title+author, stores N-1 edges (a chain) instead of N*(N-1)/2 (all pairs). Connected components via NetworkX produce identical groups.

**Containment**: `min_sim` = `len(short) / len(long)` — filters generic fragments like "the life" matching every biography. 0.3 threshold keeps good matches like "pompey the little" / "the history of pompey the little" (sim=0.53).

**Authorless matching**: 26% of fiction_biblio texts have no author. Year substitutes for author constraint in both exact (tier 1b) and containment (tier 2b) tiers, with stricter min title lengths.

**Match groups**: Each text in a match group gets a `rank` based on `CORPUS_SOURCE_RANKS` (chadwyck=1, earlyprint=2, eebo_tcp/ecco_tcp=3, ... hathi_englit=5, internet_archive=7). Rank 0 = preferred representative.

**Dedup modes**: `dedup_by='rank'` picks preferred corpus source. `dedup_by='oldest'` picks earliest year, breaking ties by rank.

**Scale**: ~2.2M texts across 52 corpora → 1.75M match pairs, 1.14M texts in 300K groups. Exact tiers run in seconds; containment ~5 min.

### Genre enrichment (post-match)

After matching, `db-enrich-genres` propagates genre labels from bibliography authority corpora across match groups. This adds `genre_enriched` and `genre_enriched_source` columns to the `texts` table.

**Authority corpora**: `fiction_biblio`, `end`, `ravengarside` — metadata-only corpora from scholarly bibliographies whose genre labels are more reliable than ESTC heuristics.

**How it works**:
1. Saves original corpus genre to `genre_corpus` column
2. `genre_enriched_source` set from ESTC `genre_source` where available (`form`/`topic`/`title`)
3. For each match group containing an authority corpus text, all members get `genre` and `genre_raw` updated to the authority's values (bibliography > form > topic > title)

**Query integration**: enrichment writes directly to `genre`, so all existing queries, views, and external consumers (e.g. abstraction web app) see enriched genres with zero code changes. `genre_corpus` preserves the original. `genre_enriched_source` provides provenance.

**genre_raw enrichment**: END enriches genre_raw from narrative_form (Epistolary→"Novel, epistolary", First/Third-person→"Novel"). fiction_biblio enriches from Raven category codes (E→"Novel, epistolary", N→"Novel"). These propagate across match groups via `enrich_genres()`.

**Impact** (as of 2026-04-06): ~10K texts reclassified, mostly ESTC-linked corpora gaining Fiction labels from bibliographies. e.g. eebo_tcp 131→621, ecco 7469→10229, earlyprint 268→859.

### CLI

```bash
lltk db-rebuild                          # drop all + rebuild all corpora (~4 min)
lltk db-rebuild estc ecco                # re-ingest specific corpora (rest untouched)
lltk db-info                             # genre × corpus crosstab with totals
lltk db-match                            # exact + containment matching (~5 min)
lltk db-match --fuzzy                    # also run fuzzy matching (adds ~15 sec)
lltk db-enrich-genres                    # propagate genre from bibliographies (~5 sec)
lltk db-wordcounts [-j 8]               # compute word counts from freqs (persistent cache)
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

# ── Genre enrichment ──
lltk.db.enrich_genres()                   # after match, propagates bibliography genres

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
                                  lltk.db.ingest()         → metadb.duckdb (texts table)
                                                                 ↓
                                  lltk.db.match()          → metadb_matches.duckdb
                                                                 ↓
                                  lltk.db.enrich_genres()  → genre + genre_raw on texts table
                                                                 ↓
                                  lltk.db.wordcounts()     → metadb_wordcounts.duckdb (persistent cache)
                                                                 ↓
                                  lltk.db.texts()          → text objects (from source corpora)
```

`load_metadata()` is the source of truth. The DB caches its output. Matching operates on the DB. Genre enrichment propagates bibliography labels across match groups, writing directly to `genre`. Word counts cached persistently and backfilled on ingest. Virtual corpus queries return real text objects that delegate file access to their source corpus.

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

### ESTC genre classification (`parse_estc_genre.py`)

`classify_genres(form_terms, subject_terms, title, title_sub)` classifies ESTC records using three tiers:
1. **form_terms** (MARC 655$a, most reliable — cataloger-assigned)
2. **subject_terms** (MARC 650$a, cataloger-assigned but noisier)
3. **title keywords** (last resort fallback, only fires if tiers 1+2 found nothing)

Accepts `list[str]` or pipe-joined strings (backward compat). Returns `{'genres': set, 'source': str}`. Mapped to broad `GENRE_VOCAB` via `_genres_to_harmonized()`.

Key design decisions:
- `history` removed from title keywords — too many novels use "History of..."
- Satire maps to `None` — cross-cutting mode, not a genre. When co-occurring with Poetry/Drama/Fiction, those win.
- `FICTION_GENRES` = {Fiction, Novel, Romance, Tale, Fable, Picaresque, Epistolary fiction, Imaginary voyage}

### ESTC translation detection (`parse_estc_genre.py`)

`detect_translation(rec)` uses three tiers on a parsed MARC bib record:
1. **MARC structural signals** (strongest): 700$e relator = translator, 240$l uniform title language subfield
2. **Title/subtitle/notes keyword matching**: "translated", "englished", "done into english", etc.
3. **Subject topic foreign language indicators**: "french", "latin", etc. in 650$a

~37% more translations detected vs old title-only approach (extrapolates to ~9,500 additional catches across 481K records).

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
python -m pytest tests/ --cov=lltk --cov-report=term   # with coverage
```

199 tests using the `test_fixture` corpus (3 texts: Blake, Austen, Shelley) checked into the repo — no external data needed. Tests cover: corpus/text path resolution, metadata hydration, MetaDB with temp DuckDB (ingest, get, query, match), normalize_title/author, xml2txt_earlyprint, fiction_biblio ID normalization, pmap, clean_text, tokenize. CI runs on push via GitHub Actions + Codecov.

## Corpus data location

- Corpora live at `~/lltk_data/corpora/<corpus_id>/`
- Each corpus has: `metadata.csv`, `txt/`, optionally `xml/`, `freqs/`
- Text files: `txt/<text_id>.txt` (flat) or `texts/<text_id>/text.txt` (per-text dirs)
- Manifest files searched in: package `corpus/manifest.txt`, `~/lltk_data/manifest.txt`, and others
- Text metadata DB: `~/lltk_data/data/metadb.duckdb`
- Match/dedup DB: `~/lltk_data/data/metadb_matches.duckdb`

## Performance

- **Parquet caching**: `BaseCorpus.load_metadata()` caches CSV as `.parquet` next to the CSV. 5-10x faster reads. Auto-regenerated if CSV is newer.
- **Enriched parquet**: ECCO, EEBO_TCP, ECCO_TCP cache full enrichment (linked metadata) as `metadata_enriched.parquet`. Skips all enrichment on subsequent loads. `load_metadata(force=True)` bypasses. ESTC no longer needs this — all enrichment done in `compile()`.
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
- **Annotations**: saved to `~/lltk_data/corpora/{corpus_id}/annotations.json` as a list of dicts with `_id` and `genre_source`
- **Propagation**: annotations propagate across match groups at read time in `load_metadata()` (annotate eebo_tcp → earlyprint version inherits)

## CuratedCorpus

Extends SyntheticCorpus with annotations.json support:

```python
class ArcFiction(CuratedCorpus):
    SOURCES = {'chadwyck': {}, 'earlyprint': {'genre': 'Fiction'}, ...}
    DEDUP = True
    DEDUP_BY = 'oldest'
```

- `annotations.json`: list of dicts, each with `_id`, `genre_source`, and annotation columns:
  ```json
  [
    {"_id": "_eebo_tcp/A69320", "genre_source": "human", "exclude": true},
    {"_id": "_eebo_tcp/A07095", "genre_source": "fiction_biblio", "genre": "Fiction"},
    {"_id": "_eebo_tcp/A07095", "genre_source": "llm:gemini-flash", "genre": "Fiction", "genre_raw": "Novel"}
  ]
  ```
- Multiple entries per `_id` from different sources. Legacy dict format auto-migrated on load.
- `SOURCE_HIERARCHY = ['human']` — flattening picks highest-priority source per column
- `exclude` field: any truthy value removes text from corpus
- `__none__` sentinel: explicitly clears a field (vs no-override)
- `annotate()`: launches web app
- Annotations propagate across match groups at read time in `load_metadata()`
- **Whitelist**: any `_id` in annotations.json (not excluded) is included in the corpus even if it doesn't match SOURCES genre filters. This lets bibliography-corrected texts enter the corpus.

### propagate_from()

Add annotation entries from a corpus, DataFrame, or list of dicts:

```python
# From a corpus (queries DB)
C.propagate_from('fiction_biblio', columns=['genre'])

# From LLM results (list of dicts)
C.propagate_from(records)  # records have _id, genre_source, genre, genre_raw, etc.

# From a DataFrame
C.propagate_from(df, columns=['genre', 'genre_raw'])

# Preview
C.propagate_from(records, dry_run=True)
```

- Accepts str (corpus ID), DataFrame, or list[dict] as source
- Re-running is safe: old entries from same source are replaced
- Match group propagation happens at read time in `load_metadata()`, not at write time
- `genre_source` from source data is respected (not overwritten)

### Multi-source hierarchy

```python
class ArcFiction(CuratedCorpus):
    SOURCE_HIERARCHY = ['human', 'fiction_biblio', 'llm:gemini-2.5-pro', 'llm:gemini-2.5-flash']
```

For each `_id`, each column is set by the highest-priority source that provides it. Lower-priority sources fill gaps. Raw entries preserved in `_load_annotations_raw()` for provenance analysis.

## Fiction bibliography corpus (fiction_biblio)

Metadata-only corpus from parsed scholarly bibliographies of early English fiction.

```python
C = lltk.load('fiction_biblio')
C.compile()  # reads sources_parsed/ CSVs, auto-matches to ESTC, writes metadata.csv
```

### Sources (6 bibliographies, 6,862 entries)

| Bibliography | ID | Period | Entries | ESTC matching method | Match rate |
|---|---|---|---|---|---|
| Mish 1967 | mish1967 | 1475-1700 | 1,497 | STC/Wing IDs | 85.2% |
| Odell 1954 | odell1954 | 1475-1700 | 1,024 | STC/Wing IDs | 88.2% |
| McBurney 1960 | mcburney1960 | 1700-1739 | 1,089 | Shelfmarks (BM→bL etc.) | 73.6% |
| Beasley 1972 | beasley1972 | 1740-1749 | 494 | McBurney cross-refs | 60.1% |
| Raven 1987 | raven1987 | 1750-1770 | 1,357 | Direct ESTC IDs | 79.7% |
| Raven 2000 | raven2000 | 1770-1799 | 1,401 | Direct ESTC IDs | 83.4% |

- Gemini Flash-parsed page images in `sources_parsed/{biblio}.csv`
- `compile()` assigns IDs (`{biblio}_{NNNN}`), auto-matches to ESTC (STC/Wing + shelfmarks + McBurney xrefs + direct ESTC IDs)
- `matches_to_verify.csv`: auto-generated with fuzzy scores for human review
- `matches_verified.csv`: manual overrides (y/n per match)
- `load_metadata()` enriches genre_raw from Raven category codes (E→"Novel, epistolary", N→"Novel")
- All entries get `genre='Fiction'`, no text files — genre propagation via match groups + `db-enrich-genres`
- 1,425 fiction_biblio texts matched to public digitized corpora (earlyprint/TCP); 831 public-only

### ESTC ID normalization in compile()

Multi-step normalization at compile time:
- Strip `ESTC ` prefix, uppercase first letter (`t068056` → `T068056`)
- Strip leading zeros (`T068056` → `T68056`) to match ESTC canonical form
- Parse multi-value IDs (`T90269, t090270`) → `id_estc` (first) + `id_estc_all` (pipe-separated)
- Strip bracketed qualifiers (`T63646 [vols. 1–4]` → `T63646`)
- Validate format (letter + digits) — rejects years-as-IDs (`1785`, `1790?`)
- ESTC linkage gap: 5 remaining (valid IDs not in our ESTC dump)

### fiction_biblio → ESTC linking

| Method | How | Corpora |
|--------|-----|---------|
| STC/Wing | `id_stc`/`id_wing` → ESTC `id_stc`/`id_wing` | Mish, Odell |
| Shelfmarks | Parse references field, map library codes (BM→bL, Bod→bO, H→nMH), lookup in ESTC `holdings` JSON | McBurney |
| McBurney xrefs | `id_mcburney` → McBurney entry with ESTC ID | Beasley |
| Direct ESTC IDs | `id_estc` from bibliography | Raven |
| Title matching | via `db-match` (containment, exact_norm) | All (catches what ID-based misses) |

### Early Novels Database (END)

Source: `~/lltk_data/corpora/fiction_biblio/sources/end-dataset-11282018-full.xml`
Downloaded from: `github.com/earlynovels/end-dataset`

2,002 MARCXML records of early novels from Penn's Collection of British and American Fiction (1660-1830). Core C18 subset: 1,440 records (all Penn holdings 1700-1797). Penn estimates ~14% coverage of all known English fiction for the 1760s.

**ESTC cross-references:** 1,168/2,002 records have ESTC IDs in MARC 510 field (e.g. `T77338`, `N6875`). Direct matching to ESTC → ECCO/TCP, no fuzzy title matching needed.

**Built from standard bibliographies:** Raven (1750-1770, 155 records), Garside/Raven/Schöwerling (1770-1829, ~520), McBurney (pre-1740, 74), Block (~495), Beasley (1740-1749, 55).

**Rich metadata beyond title/author/date:**

| MARC field | Coverage | Content |
|-----------|---------|---------|
| 592 (narrative form) | 95% | Primary: Third-person (951), First-person (508), Epistolary (449). Secondary forms + non-prose (poems, verse, dialogue) |
| 599 (author gender) | 79% | Male (1,571), Female (820), Indeterminate (1,266) |
| 520 (paratext) | 83% | Dedication, preface, advertisement, etc. with first-sentence transcriptions |
| 591 (epigraph) | 42% | Transcription + source author/work identification |
| 300 (physical) | 100% | Volume count, format (duodecimo, octavo, etc.) |
| 700/710 (publishers) | 99% | Named publishers and printers, VIAF-authorized |
| 596 (translation) | 15% | Structured source language, translation claims |
| 510 (bibliography refs) | 83% | Cross-references to ESTC, Raven, Garside, Block, McBurney |

**Complementary to fiction_biblio:** END covers C18 (1,440 records), fiction_biblio/Mish covers C17 (1,455 records). Together they provide expert-curated fiction identification across 1600-1830.

**Integration plan:** Parse END XML, extract ESTC IDs for direct ESTC→ECCO/TCP matching. Use narrative form (field 592) to test whether epistolary/first-person novels score differently on abstractness. Use author gender (field 599) for gender analysis.

## LLM genre classification

Genre classification task at `~/github/largeliterarymodels/largeliterarymodels/tasks/classify_genre.py`.

```python
from largeliterarymodels.tasks import GenreTask, format_text_for_classification
task = GenreTask()
prompt = format_text_for_classification(title=t.title, author_norm='richardson', year=1740)
result = task.run(prompt)  # or task.run(prompt, model=GEMINI_FLASH)
```

- Schema: genre, genre_raw, is_translated, author_first_name, year_estimated, confidence, reasoning
- **Verification**: sends author last name only + century year range; LLM returns first name + exact year to prove it recognizes the work
- Few-shot examples cover novels, romances, misclassified non-fiction, allegory, translations
- Tested with Claude Sonnet and Gemini Flash — both excellent
- Results cached via hashstash (pay once per text per model)

## ESTC corpus

481K bibliographic records from the English Short Title Catalogue. Metadata-only (no full text) — serves as the genre/metadata authority for linked corpora (ECCO, EEBO_TCP, ECCO_TCP).

### ESTC compile (`estc.py`)

```bash
lltk compile estc     # ~3 min, writes metadata.csv (42 columns, 481K rows)
```

Parses raw MARC JSON files from `_json_estc/` (bib) and `_json_estc_holdings/` (holdings) using `estc_json_parser.py`. Produces a wide, pre-enriched `metadata.csv` — all enrichment done at compile time, `load_metadata()` is just a plain CSV read.

### ESTC metadata columns (42)

| Category | Columns |
|----------|---------|
| Core | id, id_estc, title, title_sub, author, author_dates, year, year_end, year_type |
| Language/Place | lang, country, pub_place, publisher, pub_date, pub_nation, pub_region, pub_city |
| Physical | extent, dimensions, illustrations, format_std, format_modifier, num_pages, num_volumes, has_plates, extent_type |
| Genre | genre, genre_raw, genre_source, is_fiction, form (655$a), subject_topic (650$a), subject_place, subject_person |
| Translation | is_translated (enhanced: relator codes + uniform title language + keywords) |
| References | id_stc, id_wing (from 510), references (all 510 pipe-joined) |
| Other | added_persons, notes, urls, n_holdings |

### ESTC MARC JSON parser (`estc_json_parser.py`)

- `parse_bib_record(path_or_data)` → structured dict covering all MARC tags (control fields, 1XX authors, 245 title, 260 publication, 300 physical, 5XX notes, 6XX subjects/genres, 7XX added entries, 752 place, 76X-78X linking, 856 URLs)
- `parse_holdings_record(path_or_data)` → estc_id + list of 852 holdings (institution, shelfmark, provenance)
- Raw data: `~/lltk_data/corpora/estc/_json_estc/` (4096 shards) and `_json_estc_holdings/` (4096 shards)

### ESTC → linked corpora

ESTC metadata flows to linked corpora via `merge_linked_metadata()`:
- **ECCO**: `LINKS = {'estc': ('id_estc', 'id_estc')}` → copies estc_genre → genre
- **EEBO_TCP**: same pattern; own genre renamed to medium; medium overrides (Verse→Poetry, Drama→Drama)
- **ECCO_TCP**: same pattern as EEBO

All ESTC columns get prefixed as `estc_*` in linked corpora. Linked corpora cherry-pick what they need (genre, is_translated, title).

## EarlyPrint corpus

Combined EEBO/ECCO/Evans TCP with linguistic tagging from [EarlyPrint Project](https://earlyprint.org). ~60K texts.

```bash
lltk compile earlyprint                    # all repos
lltk compile earlyprint --repos eccotcp    # one at a time
lltk preprocess earlyprint --parts txt     # xml→txt with reg spelling (~20GB)
lltk preprocess earlyprint --parts freqs   # txt→freqs
```

- Shallow git clones + gzip-compressed XML copies to flat `xml/{ID}.xml.gz` (~10x smaller)
- Rich TEI header parser: title, author, year, IDs, quality grades, word counts
- Medium detection from body tag counts (Verse/Drama/Prose)
- `LINKS` to ESTC for genre. `MATCH_LINKS` to eebo_tcp/ecco_tcp/evans_tcp for dedup.
- `update()`: git pull + re-gzip + rebuild metadata
- XML path resolution: `xml/{tcp_id}.xml.gz` (flat directory, TCP ID = text ID)
- `ep_repo` derived from TCP ID prefix: A/B/E→eebotcp, C/K→eccotcp, N→evanstcp
- See `lltk/corpus/earlyprint/README.md` for full field reference

### xml2txt_earlyprint

`xml2txt_earlyprint(xmlfn, use_reg=True)` extracts plain text from EarlyPrint TEI XML:
- Uses `<w>` element `reg` attribute (regularized/modernized spelling) when available, falls back to surface text
- Punctuation from `<pc>` elements attached without leading space
- Extracts from `<p>` (paragraph) and `<l>` (verse line) elements within `<body>`
- Uses lxml (not BeautifulSoup) — ~0.04-0.12s per document
- Example: "NOwe sithens we haue declared" → "Now sithence we have declared"
- `reg` coverage varies by text age: ~5-29% of words have reg (only where spelling differs from modern)

### .gz file support

`_open_file(path)` in text.py transparently handles `.gz` files. Applied to `BaseText.xml`, `text_plain()`, and `TextSection.txt`. Corpora can set `ext_xml = .xml.gz` or `ext_txt = .txt.gz` in manifest.

## Development notes

- `__getattr__` on BaseCorpus handles `path_*` attributes; raises `AttributeError` for everything else
- `BaseText.get(key)` does fuzzy metadata lookup (`ish=True`): searches for keys starting with `key`. Calls `_hydrate_meta()` first.
- `metadata_initial()` calls `_hydrate_meta()` so `t.meta` works on bare text shells
- `_corpus_meta_row()` tries DB lookup, then checks cached `_metadfd` before triggering expensive `load_metadata()`
- `iter_init()` pre-populates text._meta from DataFrame and sets `_meta_hydrated=True`
- `iter_texts()` uses objects from `_textd` directly (no `Text()` factory wrapping)
- `get_idx()` preserves spaces, `+`, `$` in IDs (no longer forces snake_case)
- `CORPUS_SOURCE_RANKS` in `metadb.py` defines preference order for dedup: chadwyck=1, earlyprint=2, eebo_tcp/ecco_tcp/evans_tcp=3, ...
- `MATCH_LINKS`: ID-based matching without metadata merge (separate from `LINKS`)
- `is_translated`: ESTC detection via MARC structural signals (relator, uniform title lang) + title/notes/subject keywords; inherited by linked corpora, core DB column
- `t.match_group_texts`: returns text objects for all match group members (falls back to `[self]`). Enables multi-version scoring.
- `path_freqs` in DB: relative to PATH_CORPUS, resolved during `ingest()` via `_resolve_freqs_paths()`. Enables bulk DuckDB queries for scoring without text object instantiation.
- `_open_file(path)` in text.py: helper that returns `gzip.open()` for `.gz` paths, `open()` otherwise. Used by `xml`, `text_plain()`, `TextSection.txt`.
- `_PmapCaller` class in tools.py: picklable replacement for closures in `pmap()`. Needed because `ProcessPoolExecutor` can't pickle closures.
- `preprocess_txt` uses `use_threads=True` to avoid pickle issues with corpus modules loaded dynamically via manifest (they get short `__module__` names that workers can't reimport).
- `ext_xml` manifest field: controls file extension for flat-directory XML path resolution via `get_path_old()`. E.g. earlyprint sets `ext_xml = .xml.gz`.
