# CLAUDE.md

## Project overview

LLTK (Literary Language Toolkit) — Python package for computational literary analysis and digital humanities. 60+ literary corpora, text processing, analysis methods.

**Author:** Ryan Heuser
**Package:** `lltk-dh` (PyPI) · **Version:** 0.7.0 · **Python:** >=3.8 · **License:** MIT

## Architecture

```
lltk/
├── imports.py               # constants, config, logger
├── __init__.py              # re-exports, lltk.db (MetaDBCH singleton)
├── text/                    # BaseText, TextSection, Text() factory, utils
├── corpus/                  # BaseCorpus, SectionCorpus, SyntheticCorpus, manifest.txt, per-corpus subpkgs (60+)
├── model/                   # preprocess.py, matcher.py, word2vec, characters, networks, booknlp
├── tools/
│   ├── baseobj.py           # BaseObject root class
│   ├── tools.py             # config, pmap, utils
│   ├── metadb.py            # legacy MetaDB (DuckDB, kept as test backend)
│   ├── metadb_ch.py         # MetaDBCH — the live ClickHouse singleton
│   ├── db_adapter.py        # DBAdapter + DuckDBAdapter + ClickHouseAdapter
│   ├── db_migrate.py        # DuckDB → ClickHouse one-shot migrator
│   ├── clickhouse_schema.py # CH schema (all lltk.* tables)
│   ├── clickhouse_rebuild.py     # db-rebuild → CH
│   ├── clickhouse_ingest.py      # db-freqs → CH
│   ├── clickhouse_text_words.py  # db-text-words → CH
│   ├── clickhouse_match.py       # db-match → CH
│   ├── clickhouse_enrich.py      # db-enrich-genres, db-detect-translations → CH
│   ├── clickhouse_detect_langs.py  # db-detect-langs → CH
│   └── clickhouse_wordindex.py     # db-wordindex → CH
└── web/app.py               # FastAPI + Svelte explorer
```

## Key patterns

- **Inheritance:** BaseObject → TextList → BaseCorpus → specific corpus classes
- **Factories:** `Text(id)` and `Corpus(id)` return cached objects
- **Lazy hydration:** `C.texts()` yields bare shells; `t.author`, `t.year`, `t.get(k)` trigger `_hydrate_meta()` (CH `lltk.db.get()` first, CSV fallback). One-time per text via `_meta_hydrated` flag.
- **Path resolution:** `corpus.path_*` via `__getattr__` → `get_path()`; relative + absolute supported.
- **Manifest:** `manifest.txt` (configparser); merged from package dir + `~/lltk_data/` + user config.
- **Metadata loading:** `C.meta` uses `load_metadata()` (CSV → parquet cache). Subclasses override to enrich.
- **Cross-corpus linking:** `LINKS = {target: (my_col, their_col)}` declared per-corpus; `merge_linked_metadata()` left-joins with prefixed columns.

## Data backend: ClickHouse

`lltk.db` is a `MetaDBCH` instance over a local ClickHouse server. CSV files + freqs JSONs remain the source of truth; CH is the analytical query engine.

### Install (native macOS via brew)

```bash
brew install clickhouse
sudo xattr -dr com.apple.quarantine /opt/homebrew/bin/clickhouse
clickhouse server --config-file=~/lltk_data/data/clickhouse-config/config.xml &
```

Config at `~/lltk_data/data/clickhouse-config/config.xml` sets `path=~/lltk_data/data/clickhouse/` (data on 2 TB disk). User `lltk` / password `lltk` on `localhost:8123`.

Linux: `apt-get install clickhouse-server clickhouse-client` from the official repo.

### Core tables (all in database `lltk`)

| Table | Engine | ORDER BY | Purpose |
|---|---|---|---|
| `texts` | ReplacingMergeTree | `(corpus, _id)` | corpus-stamped metadata per text |
| `corpus_info` | ReplacingMergeTree | `corpus` | ingested_at, n_texts |
| `matches` | ReplacingMergeTree | `(_id_a, _id_b)` | pairwise dedup links |
| `match_groups` | ReplacingMergeTree | `_id` | `group_id` + `rank` for each _id |
| `text_freqs` | ReplacingMergeTree | `_id` | `Map(String, UInt32)` per text — fast per-text retrieval |
| `text_words` | MergeTree | `(word, _id)` | **flat `(word, _id, count, corpus)`** — fast per-word analytics |
| `text_stats` | ReplacingMergeTree | `_id` | `n_words`, `n_unique_words` (cheap totals for JOINs) |
| `text_langs` | ReplacingMergeTree | `_id` | lang_detected, coverage, confidence |
| `text_genres` | ReplacingMergeTree | `_id` | enriched genre / genre_raw / genre_enriched_source |
| `text_translations` | ReplacingMergeTree | `_id` | `is_translated`, `original_lang` |
| `stopwords` | MergeTree | `word` | `word → lang` lookup for detect_langs |
| `word_year_corpus` | MergeTree | `(word, year, corpus)` | pre-aggregated ngram cache (optional) |
| `year_corpus_totals` | MergeTree | `(year, corpus)` | per-year denominator for normalization |

**Two representations of per-text word data:**

- `text_freqs` (Map per row) — optimized for **per-text retrieval**: `t.freqs()`, abstraction scoring, MinHash `map_keys(freqs)`.
- `text_words` (flat `(word, _id, count)`) — optimized for **per-word analytics**: `sum(count) WHERE word='virtue'` scans a contiguous index range (sub-second). Any query that aggregates across one word across many texts belongs here.

Both are derived from `freqs/*.json` on disk. Building both costs ~20 min one-time.

### Other storage

- **`~/lltk_data/data/metadb_passages.sqlite`** — SQLite + FTS5 passage search (~500-word chunks). Built by `lltk db-passages`, queried via `lltk search`. Stays SQLite for its mature `snippet()` / BM25; concurrent readers fine in WAL mode.
- **freqs JSONs** at `{corpus}/freqs/{id}.json` — source of truth, distributed via `corpus.publish()`.
- **Legacy DuckDB files** (`~/lltk_data/data/metadb*.duckdb`) — kept during migration; the `MetaDB` class still in `metadb.py` serves as emergency fallback and unit-test backend.

### Schema of `texts`

| Column | Type | Notes |
|---|---|---|
| `_id` | String | `_{corpus}/{id}` canonical address |
| `corpus` | LowCardinality(String) | indexed |
| `id` | String | text id within corpus |
| `title`, `author` | String | |
| `year` | Nullable(Int32) | parsed (handles ranges, circa dates) |
| `genre` | LowCardinality(String) | enriched via `db-enrich-genres` |
| `genre_raw` | String | specific label (e.g. "Epistolary fiction") |
| `title_norm`, `author_norm` | String | normalized for matching |
| `path_freqs` | Nullable(String) | relative to PATH_CORPUS |
| `n_words` | Nullable(Int32) | backfilled from text_stats |
| `lang`, `is_translated`, `original_lang` | | language + translation flags |
| `lang_detected`, `lang_coverage`, `lang_confidence` | | from `db-detect-langs` (joined from text_langs) |
| `meta` | String | JSON of corpus-specific extras |

### Title and author normalization

Computed at ingest, stored as indexed columns.

- **`normalize_title`**: HTML-unescape, Unicode-dash-normalize, strip `[]`, strip abbreviation periods (Mr./Mrs./Dr./St./Q. + single letters), modernize early-modern spelling via MorphAdorner (358K entries: u/v, vv/w, i/j, terminal -e), lowercase, strip subtitle after first `:;.([,!?`, strip title-end phrases ("a novel", "by the author"). `"Loues load-starre"` → `"loves loadstar"`.
- **`normalize_author`**: lowercase, text before first comma. `"Congreve, William, 1670-1729."` → `"congreve"`.

### Blacklist

`DB_BLACKLIST = {'hathi', 'bighist'}` — parent `hathi` has 17M government-document texts causing matching explosions. Use subcorpora (hathi_englit, hathi_novels, …).

### Standard metadata contract

Every `load_metadata()` returns a DataFrame with: `id` (index), `title`, `author`, `year`, `genre`, `genre_raw`.

**GENRE_VOCAB**: Fiction, Poetry, Drama, Periodical, Essay, Treatise, Letters, Sermon, Biography, History, Nonfiction, Legal, Speech, Spoken, Criticism, Academic, Almanac, Reference.

### Matching tiers (`db-match`)

| Tier | match_type | Constraint | Impl |
|---|---|---|---|
| 0 | `id_link` | Shared IDs from LINKS/MATCH_LINKS | SQL |
| 1a | `exact_norm` | title_norm + author_norm | SQL `lead()` chain-linking |
| 1b | `exact_norm_year` | title_norm + year (authorless, min len 10) | SQL `lead()` |
| 2a | `containment` | short title ⊆ long title, same author, min_sim=0.3 | Python (batch-pulled then grouped) |
| 2b | `containment_year` | same but by year (authorless, min len 15) | Python |
| 3 | `fuzzy_title` | Jaro-Winkler > 0.85 within author blocks (opt-in `--fuzzy`) | Python (rapidfuzz) |

**Chain linking**: `lead(_id) OVER (PARTITION BY title_norm, author_norm ORDER BY _id)` — N-1 edges for N duplicates instead of N*(N-1)/2. Connected components via NetworkX produce identical groups.

**Containment**: `min_sim = len(short) / len(long)` filters generic fragments ("the life" matching every biography). 0.3 keeps good matches ("pompey the little" vs "the history of pompey the little", sim=0.53).

**Authorless**: year substitutes for author in tiers 1b + 2b with stricter title-length floors.

**Match groups**: NetworkX connected components with `rank` by `CORPUS_SOURCE_RANKS` (chadwyck=1, earlyprint=2, eebo_tcp/ecco_tcp=3, hathi_englit=5, internet_archive=7). Rank 0 = preferred representative. `dedup_by='rank'` or `'oldest'`.

**Scale**: ~2.8M texts × 60 corpora → ~1.7M match pairs, ~1.3M texts in ~330K groups. Full `db-match` ~2 min (down from ~5 min in DuckDB).

### Genre enrichment (`db-enrich-genres`)

Writes to `lltk.text_genres` (separate table, not UPDATE on texts — CH ALTER is expensive). Baseline = corpus genre; authority corpora (`fiction_biblio`, `end`, `ravengarside`) override via match groups, highest-priority authority wins per group. `genre_corpus` preserves original.

After enrichment, `enrich_genres_ch` mirrors `genre_enriched_source` + `genre_corpus` back to `lltk.texts` via `INSERT INTO texts SELECT ... LEFT JOIN text_genres ... + OPTIMIZE TABLE texts FINAL`. ReplacingMergeTree on `(corpus, _id)` makes the newer row win on FINAL reads, same effect as ALTER UPDATE but synchronous and ~14s for 2.8M rows. `text_genres` stays the source of truth; the texts columns are a read-convenience mirror for callers that `SELECT genre_enriched_source FROM texts` directly (ArcFiction's conservative filter, web-app provenance queries). Without this sync the columns exist in schema but are empty — silent regression if skipped.

### Translation detection (`db-detect-translations`)

Writes to `lltk.text_translations`. Finds match groups with 2+ languages; earliest-year language wins (tie → text count), others get `is_translated=1` with `original_lang`. Top flows: en↔la, en↔fr, de↔en.

### Language detection (`db-detect-langs`)

NLTK stopwords (en/fr/de/it/es/pt/nl) + curated Latin (~150) + Greek (~50) = ~1,700 words, 9 langs. Per text, JOIN `text_words` with `stopwords` + GROUP BY → per-lang hit counts. Argmax in SQL with thresholds (coverage ≥ 0.05, confidence ≥ 2.0). Writes to `lltk.text_langs`. ~minutes for 2.2M texts (vs ~hours via per-row Map lookups before `text_words` existed).

### Word index / ngrams (`db-wordindex`)

Optional pre-aggregated cache. Native CH single INSERT:

```sql
INSERT INTO lltk.word_year_corpus
SELECT word, year, corpus, genre, sum(count) AS word_count, …
FROM lltk.text_words tw
JOIN lltk.texts t ON tw._id = t._id
GROUP BY word, year, corpus, genre
```

`_dedup` columns restrict to rank=0 match-group representatives.

**Often redundant now**: `text_words` alone answers any ngram query in <1 s thanks to the `(word, _id)` index. Only build this cache if you need sub-100-ms ngram UI at high QPS.

### CLI

```bash
lltk db-rebuild                        # corpus CSVs → lltk.texts
lltk db-freqs                          # freqs/*.json → lltk.text_freqs
lltk db-text-words                     # text_freqs → text_words (flat) + text_stats
lltk db-match [--fuzzy]                # dedup tiers 0-3
lltk db-enrich-genres                  # authority genre propagation
lltk db-detect-translations            # cross-lang match groups
lltk db-detect-langs                   # per-text language detection
lltk db-wordindex [--vocab-size 50000] # optional ngram pre-agg
lltk db-info                           # genre × corpus crosstab
lltk db-matches "Incognita"            # search match groups by title
lltk db-match-stats                    # matching statistics
lltk db-passages [-n 500]              # SQLite FTS5 passage index
lltk search "virtue" [--genre Fiction]
lltk app [--port 8899]                 # web explorer
lltk annotate <curated_corpus>         # annotation UI
```

### Python API

```python
import lltk
lltk.db.rebuild(['estc'])              # → lltk.texts
lltk.db.build_freqs_db(corpora=['estc'])  # → lltk.text_freqs
lltk.db.build_text_words()             # → lltk.text_words + text_stats
lltk.db.match(fuzzy=False)             # → lltk.matches, lltk.match_groups

# Reads
lltk.db.get('_estc/T012345')           # dict
lltk.db.query("SELECT * FROM texts WHERE year<1700 AND genre='Fiction'")
lltk.db.texts_df(sources=..., dedup=True, dedup_by='oldest')  # SyntheticCorpus filter → DataFrame
lltk.db.texts(sources=..., dedup=True)  # same but yields BaseText objects
lltk.db.read_freqs(ids=[...])          # per-text freqs for a batch (abstraction)
lltk.db.dedup_frame(df, by='rank')     # reduce any _id-keyed frame via match groups
lltk.db.ngram(['virtue', 'honor'], genre='Fiction', dedup=True)
lltk.db.find_matches('Incognita')
lltk.db.get_group('_estc/T012345')

# Virtual corpora
class BigFiction(SyntheticCorpus):
    SOURCES = {'chadwyck': {'genre': 'Fiction'}, 'estc': {'genre': 'Fiction'}, …}
    DEDUP = True; DEDUP_BY = 'oldest'
```

**Text objects keep their source-corpus reference** — `t.path_txt`, `t.freqs()` resolve through the originating corpus regardless of whether retrieved via virtual corpus or `lltk.db.texts()`.

### For the abstraction project

```python
from lltk.tools.db_adapter import get_adapter
ch = get_adapter('clickhouse://lltk:lltk@localhost:8123/lltk')
df = ch.query_df("SELECT _id, corpus, freqs FROM lltk.text_freqs WHERE _id IN (…)")
```

Or `lltk.db.read_freqs(ids=[...])` for the same thing, MetaDBCH-wrapped. `lltk.db.corpus_info()` exposes `ingested_at` for staleness tracking.

**Talk to abstraction-claude directly.** Another Claude Code session typically runs at `~/github/abslithists/abstraction` working on the sibling project. If you get stuck on something that project would know (scoring pipeline, allnorms shapes, their scores DB schema, how they consume lltk's CH), or you want a second opinion on a design call that affects both codebases, use the `send-peer` skill to message them rather than guessing or pinging the user. Prefix messages with `[from lltk-claude via send-peer]`. Same applies the other direction — they may ping you about CH schemas, `text_freqs` / `text_words` behavior, or migration status.

## Genre classification (per-corpus)

Three patterns in `load_metadata()`:

- **ESTC heuristic** (`parse_estc_genre.py`): `classify_genres(form_terms, subject_terms, title, title_sub)`. Tiers: form (MARC 655$a, most reliable), topic (650$a), title keywords (fallback). `FICTION_GENRES = {Fiction, Novel, Romance, Tale, Fable, Picaresque, Epistolary fiction, Imaginary voyage}`. `history` removed from title kw (too many novels). `detect_translation` uses MARC 700$e, 240$l, title/notes/subject keywords (~37% more catches than title-only).
- **Linked corpora** (ECCO, EEBO_TCP, ECCO_TCP): inherit `estc_genre` → `genre` via `merge_linked_metadata()`. EEBO/ECCO TCP rename own `genre`→`medium`; `medium=Verse`→Poetry, `=Drama`→Drama override inherited.
- **Simple/mapping**: single genre (gildedage/chicago/ravengarside/txtlab=Fiction, chadwyck_poetry=Poetry, sotu=Speech, oldbailey=Legal) or per-value maps (COCA: FIC→Fiction, ACAD→Academic, SPOK→Spoken; COHA: Film→Drama).

## Hathi ID normalization

`hathi_id_normalize()` collapses all variants to flat `{library}/{volume_id}`:

```
mdp/390/15009144422      → mdp/39015009144422       (3-char dir split)
bc/ark/+=13960=t0bv7v96f → bc/ark+=13960=t0bv7v96f  (split ark)
aeu/ark:/13960/t0000ds1j → aeu/ark+=13960=t0000ds1j (colon-slash ark)
```

Applied in `load_metadata()` for all Hathi corpora. Freqs index (`_build_freqs_index()`) maps `canonical_id → filepath`. Subcorpora share a freqs pool at `~/lltk_data/corpora/hathi/freqs` via manifest `path_freqs = ../hathi/freqs`.

## Corpus data location

- `~/lltk_data/corpora/<corpus_id>/`
- Each corpus has `metadata.csv`, `txt/`, optionally `xml/`, `freqs/`
- Text files: `txt/<text_id>.txt` (flat) or `texts/<text_id>/text.txt` (per-text dirs)
- Manifests searched: package `corpus/manifest.txt` + `~/lltk_data/manifest.txt` + user config
- ClickHouse data: `~/lltk_data/data/clickhouse/` (2 TB disk)
- Passages: `~/lltk_data/data/metadb_passages.sqlite`

## Performance

- **Parquet caching**: `BaseCorpus.load_metadata()` caches CSV as `.parquet` next to the CSV. 5-10× faster reads. Auto-regenerated if CSV is newer.
- **Enriched parquet**: ECCO, EEBO_TCP, ECCO_TCP cache full enrichment as `metadata_enriched.parquet`. `load_metadata(force=True)` bypasses. ESTC does all enrichment in `compile()`.
- **Pre-populated text metadata**: `iter_init()` passes DataFrame row directly to each text, sets `_meta_hydrated=True`.
- **pmap**: `concurrent.futures`-backed parallel map. `DEFAULT_NUM_PROC = cpu_count - 2`.
- **orjson** everywhere for freqs JSON reading (3-10× faster, releases GIL).

## Running tests

```bash
python -m pytest tests/ -v
python -m pytest tests/ --cov=lltk --cov-report=term
```

206 tests using `test_fixture` corpus (3 texts: Blake, Austen, Shelley) checked into repo — no external data. Tests cover corpus/text path resolution, metadata hydration, legacy MetaDB with temp DuckDB (ingest, get, query, match), normalize_title/author, xml2txt_earlyprint, fiction_biblio ID normalization, pmap, clean_text, tokenize. CI on push via GitHub Actions + Codecov.

## Annotation web app (`lltk annotate`)

FastAPI for browsing/annotating CuratedCorpus metadata: filterable table, detail panel, annotation form (genre dropdown, genre_raw datalist, is_translated/exclude/notes), bulk actions, manual duplicate linking. Saves to `{corpus}/annotations.json`.

## CuratedCorpus

Extends `SyntheticCorpus` with `annotations.json`:

```python
class ArcFiction(CuratedCorpus):
    SOURCES = {'chadwyck': {}, 'earlyprint': {'genre': 'Fiction'}, …}
    DEDUP = True; DEDUP_BY = 'oldest'
```

- `annotations.json` = list of dicts `{_id, genre_source, …}`. Multiple entries per `_id` from different sources.
- `SOURCE_HIERARCHY = ['human', 'fiction_biblio', 'llm:gemini-2.5-pro', …]` — per-column, highest-priority source wins.
- `exclude` field: any truthy value removes text. `__none__` sentinel explicitly clears.
- **Whitelist**: any `_id` in annotations.json is included, even if it doesn't match SOURCES genre filters (bibliography-corrected texts enter).
- `propagate_from(source)` adds entries from corpus ID / DataFrame / dict list. Idempotent (replaces same-source entries).
- Annotations propagate across match groups at read time.
- **Adding a new `ArcX` subclass is a two-file change**: the class in `lltk/corpus/arc_corpora/arc_corpora.py` AND a stanza in `lltk/corpus/manifest.txt`. Star-imports make the class directly importable, but `lltk.load('arc_x')` goes through manifest resolution — no stanza = returns None. Registered arcs: `arc_fiction`, `arc_fiction_fr`, `arc_fiction_de`, `arc_poetry`, `arc_periodical`, `arc_essays`, `arc_sermons`, `arc_biography`.
- **Annotation whitelist/propagation silently skips on CH** — `CuratedCorpus.load_metadata` uses DuckDB-only `lltk.db.conn.register(df)` / `unregister()` for the annotated-_ids whitelist + match-group override, wrapped in `try/except Exception: pass`. On CH those AttributeError and skip. SOURCES filter still works; annotation-driven whitelist + group propagation do not. Port if/when CH needs them.

## Explorer web app (`lltk app`)

FastAPI + Svelte 5, reads from `lltk.db` (MetaDBCH via legacy `.conn` shim that rewrites DuckDB SQL to CH on-the-fly).

Views: Dashboard (stats, corpus grid, genre timeline, heatmap), Texts (filterable table + detail), Ngrams (requires `db-wordindex` or uses `text_words` live), Matches, Corpora, Overlap.

Hash-based URL state (`#texts?search=Pamela&genre=Fiction`). JSON API at `/docs`.

## Fiction bibliography corpus (fiction_biblio)

Metadata-only corpus from 6 scholarly bibliographies (6,862 entries total):

| Bibliography | Period | Entries | ESTC link method | Match % |
|---|---|---|---|---|
| Mish 1967 | 1475-1700 | 1,497 | STC/Wing IDs | 85.2 |
| Odell 1954 | 1475-1700 | 1,024 | STC/Wing IDs | 88.2 |
| McBurney 1960 | 1700-1739 | 1,089 | Shelfmarks → bL/bO/nMH | 73.6 |
| Beasley 1972 | 1740-1749 | 494 | McBurney cross-refs | 60.1 |
| Raven 1987 | 1750-1770 | 1,357 | Direct ESTC IDs | 79.7 |
| Raven 2000 | 1770-1799 | 1,401 | Direct ESTC IDs | 83.4 |

All entries `genre='Fiction'`. No text files — genre reaches digitized texts via match groups + `db-enrich-genres`. `sources_parsed/*.csv` from Gemini Flash-parsed page images; `compile()` assigns `{biblio}_{NNNN}` IDs and auto-matches to ESTC.

**ESTC ID normalization**: strip `ESTC `, uppercase, strip leading zeros (`T068056`→`T68056`), multi-value → `id_estc`+`id_estc_all`, strip `[qualifiers]`, validate letter+digits.

### Early Novels Database (END)

2,002 MARCXML records (1660-1830) from Penn, at `~/lltk_data/corpora/fiction_biblio/sources/end-dataset-11282018-full.xml`. 1,168 have ESTC IDs (MARC 510). Rich metadata: narrative_form (592: Third/First-person/Epistolary), gender (599), paratexts (520), epigraphs (591), publishers (700/710), translation (596).

## ESTC corpus

481K bibliographic records — metadata-only, genre/metadata authority for linked corpora. `lltk compile estc` (~3 min) parses raw MARC JSON (`_json_estc/`, `_json_estc_holdings/`) via `estc_json_parser.py` into a wide 42-column pre-enriched `metadata.csv`. Raw data sharded 4096-ways.

Columns: id/title/author/year, language/place, physical (extent/format/volumes), genre (genre, genre_raw, genre_source, is_fiction, form=655$a, subject_topic=650$a), translation, references (id_stc, id_wing, 510 refs), holdings, notes, urls.

## EarlyPrint corpus

Combined EEBO/ECCO/Evans TCP with linguistic tagging ([earlyprint.org](https://earlyprint.org)). ~60K texts.

```bash
lltk compile earlyprint [--repos eccotcp]
lltk preprocess earlyprint --parts txt   # xml→txt with reg spelling (~20GB)
lltk preprocess earlyprint --parts freqs
```

- Shallow git clones, gzipped `xml/{ID}.xml.gz` (~10× smaller)
- TEI header parser (title, author, year, IDs, quality grades, word counts); medium from body tag counts
- `LINKS` to ESTC; `MATCH_LINKS` to eebo_tcp/ecco_tcp/evans_tcp
- `xml2txt_earlyprint(xmlfn, use_reg=True)`: lxml (~0.04-0.12 s/doc). Uses `<w reg="…">` (5-29% coverage by text age), else surface. `<pc>` attached without leading space. Extracts `<p>` + `<l>` within `<body>`.
- `.gz` transparent via `_open_file` in text.py. Manifest `ext_xml=.xml.gz` / `ext_txt=.txt.gz` selects extension.

## Non-English corpora

**French**: `artfl` (Frantext, 3.6K), `french_pd_books` (PleIAs/Gallica, 290K), `gallica_literary_fictions` (Zenodo, 15.5K), `paige` (Zenodo Gallagher/Paige, 3.2K).

**German**: `dta` (Deutsches Textarchiv, 3.3K), `german_pd` (PleIAs, 275K), `german_fiction` (Figshare/Gutenberg-DE, 3.2K + 484 translations), `de_corp` (Zenodo, ~5K fiction+non-fiction).

Compile patterns: HF streaming (`french_pd_books`), HF pyarrow (`german_pd` — schema-differing shards), Zenodo REST (`paige`, `gallica_literary_fictions`, `de_corp`), Figshare API (`german_fiction`).

Conservative genre keywords (precision over recall):
- French: `roman(s)`→Fiction, `poème(s)/poésie(s)`→Poetry, `comédie/tragédie/opéra`→Drama
- German: `roman(e)/novelle(n)`→Fiction, `gedicht(e)/lyrik`→Poetry, `komödie/tragödie/trauerspiel/lustspiel/schauspiel`→Drama

Excluded as noisy: histoire/Geschichte, nouvelle, mémoires, conte, vers, discours, lettre, erzählung, märchen, brief.

**`french_pd_books` and `german_pd` temporarily excluded from ArcFictionFr / ArcFictionDe** (see `lltk/corpus/arc_corpora/arc_corpora.py`) — title-keyword heuristics at their scale are unreliable, and detect-langs flagged 1,478 high-confidence English texts in german_pd's unclassified subset. Re-enable once bibliography authority or LLM `classify_genre` pass lands. They stay in the corpus data + `lltk.texts` (other paths may use them); only the arc `SOURCES` dicts exclude them.

## MinHash matching

`scripts/minhash_match.py` — near-duplicate detection by word-set overlap.

```bash
python scripts/minhash_match.py [--threshold 0.7] [--num-perm 128]
```

Reads `map_keys(freqs)` from `lltk.text_freqs`, builds MinHash signatures, LSH for candidate pairs, writes to `lltk.matches` as `match_type='minhash'`. Threshold 0.7 + 5K-word floor excludes formulaic-vocab mega-groups (proclamations, sermons).

## Language normalization

`lang` normalized to ISO 639-1 at ingest. Resolution order: `lang` / `language` / `language_1` / `estc_lang` column → manifest fallback → NULL. `normalize_lang()` maps 639-2/B (eng/fre/ger), 639-2/T (fra/deu), full names; pass-through 639-1.

`t.lang` on BaseText checks meta keys in order, returns None if missing. `t.sents()` + `PassageSectionCorpus.parse_sections()` use `_lang_to_punkt(lang)` (falls back to English).

## Passages DB (`db-passages`, `search`)

SQLite + FTS5 at `~/lltk_data/data/metadb_passages.sqlite`. ~500-word passages with sentence-aware chunking (NLTK punkt per lang). ~25-35 GB for fiction corpora.

```sql
passages(_id, seq, text, n_words, lang, PK(_id, seq))
passages_meta(_id PK, corpus, n_passages)
passages_fts USING fts5(_id, text, content='passages')
```

```python
lltk.db.search('virtue', genre='Fiction', year_min=1700, limit=20)
lltk.db.search('"virtue and honor"', corpus='chadwyck')
lltk.db.search('NEAR(virtue vice, 5)', lang='en')
```

Filters (genre/corpus/lang/year) resolved in CH → `_id` set → FTS5 search within set. Hathi excluded (freqs only, no txt).

WAL mode enables unlimited concurrent readers. Stays SQLite for mature `snippet()` / BM25; CH's `full_text` index works but lacks snippet generation. If corpus grows past current scale (1M+ texts, 200+ GB), consider partitioning per corpus: `{corpus}/data/passages.sqlite`.

## Prosodic integration

Optional dep (`prosodic>=3.1`). Per-text layout `{corpus.path_prosodic}/{text.id}/` with `syll.parquet`, `parsed.parquet`, `meta.json` (resume marker). `path_prosodic` resolves via `get_path` chain.

```bash
lltk prosodic-parse <corpus> [-j N] [--device cpu|gpu|auto] [--no-resume] [--limit N]
lltk prosodic-aggregate <corpus>     # streams per-text → {corpus.path}/prosodic.parquet
```

`--device auto` picks MPS/CUDA if available; GPU forces `n_workers=1`.

`t.prosodic(cached=True, **kwargs)`: cached → pre-parsed TextModel with full scansion; `cached=False` builds fresh from `t.txt`. kwargs forward to `prosodic.Text(...)`.

## BookNLP character analysis

`lltk/model/booknlp.py` wraps [BookNLP](https://github.com/booknlp/booknlp).

```python
t.booknlp.parse()           # → corpus/booknlp/en_small/{text_id}/
t.booknlp.chardata()        # character DataFrame
t.booknlp.quotes(); .tokens()
G = t.booknlp.comention_network(window=200, min_edge_count=5)
t.booknlp.plot_network(save_path='network.png')
```

Window=200 tokens is the sweet spot (100 too tight, 500+ too loose). `exclude_generic=True` filters collectives/abstractions.

### LLM character resolution

BookNLP NER/coref is noisy on early modern English. `CharacterTask` at `~/github/largeliterarymodels/largeliterarymodels/tasks/resolve_characters.py` cleans via Gemini Flash on top 30 clusters. Writes `{corpus}/booknlp/en_small/{text_id}/characters_resolved.json`.

## LLM tasks (`largeliterarymodels`)

External package at `~/github/largeliterarymodels/`. Tasks cached via hashstash (pay once per text per model).

- **GenreTask**: `genre, genre_raw, is_translated, author_first_name, year_estimated, confidence, reasoning`. Verification: send author last name + century; LLM returns first name + exact year to prove recognition.
- **FryeTask**: mode (myth/romance/high_mimetic/low_mimetic/ironic), mythos (comedy/romance/tragedy/irony), narration, referential_mode (nobody/somebody/pseudo_referential/ambiguous), confidence scores, free-text signals + displacement. Input: OPENING/MIDDLE/CLOSING passages.
- **CharacterTask**: see above.

## Development notes

- **Hydration chain**: `BaseText.get(key)` does fuzzy (`ish=True`) lookup after `_hydrate_meta()`. `_corpus_meta_row()` tries CH → cached `_metadfd` → `load_metadata()`.
- **Lazy sampling `C.t`**: CH via `metadb.conn` (shim) → `metadata.csv` id column → `_textd` → full load. `BaseText.__init__` calls `corpus.add_text(self)`, so `_textd` is never empty after one sample — fast-path must not gate on that alone.
- **Legacy compat**: `get_txt(force_xml=False)` only forwards `force_xml` when truthy, with `TypeError` fallback — keeps older corpus subclass signatures working.
- **Legacy `.conn` shim** on MetaDBCH rewrites DuckDB SQL for web-app compat: `match_db.*`→`lltk.*`, `json_extract_string(x,'$.y')`→`JSONExtractString(x,'y')`, `to_timestamp`→`toDateTime`, `random()`→`rand()`, positional `?` params inlined.
- **Misc**: `get_idx()` preserves spaces/`+`/`$` in IDs. `_open_file` handles `.gz`. `_PmapCaller` (picklable class) for `pmap()` under ProcessPoolExecutor. `corpus.zip()` avoids `os.chdir()` (macOS SIP). `PATH_CORPUS` wrapped in `os.path.expanduser()`.

### Dialect gotchas (CH vs DuckDB)

- `FROM texts FINAL AS t` (alias after FINAL, or wrap FINAL in a subquery)
- `lead()` window works the same; but CH doesn't accept `FROM t FINAL alias` without `AS`
- `INSERT OR IGNORE` → plain `INSERT` (ReplacingMergeTree handles dedup async; use `FINAL` on reads for exactness)
- `ALTER TABLE ... DELETE` + `SETTINGS mutations_sync=1` for synchronous deletes
- **No correlated subqueries in ALTER UPDATE.** CH rejects `UPDATE t SET col = (SELECT v FROM other WHERE other._id = t._id)`. To sync columns from another table onto a ReplacingMergeTree, `INSERT INTO target SELECT ... LEFT JOIN source` + `OPTIMIZE TABLE target FINAL`. The RMT ORDER BY key makes the newer row win on FINAL reads, same effect and synchronous (vs ALTER UPDATE's async mutation). See `enrich_genres_ch` for the pattern.
- `async_insert=1` for many small inserts to avoid "too many parts" throttling
- Streaming reads (`query_arrow_stream`) hold the primary session — use a separate client for concurrent writes
- CH returns booleans as `UInt8` with pd.NA. `.fillna(False)` chokes on the MaskedArray. Promote to pandas nullable `boolean` dtype before boolean ops (done automatically in `texts_df` for `is_translated`).
- IN-clause size: building `WHERE _id IN ({huge_tuple})` with >>10K values hits `max_query_size`. Insert to a `tmp.*` Memory table and JOIN instead.

## DB migration (one-shot)

`lltk/tools/db_migrate.py` migrates DuckDB → ClickHouse via staged parquet:

```bash
python -m lltk.tools.db_migrate --tables texts matches match_groups
```

## Future work

- **Web app**: saved queries, author page, text export, comparison/map/network views
- **OCR correction factors**: per-corpus multiplicative corrections + per-text quality scores from match-group n_words ratios
- **Global annotations**: CH table `lltk.annotations` for cross-corpus web-app annotation (currently per-CuratedCorpus only)
- **Deployment**: lltk.net on Hetzner. Options: (a) `clickhouse-backup` nightly to S3, (b) per-server CH with direct ingest, (c) dump+restore
- **Port wordcounts command**: largely redundant now (`arraySum(mapValues(freqs))` or `text_stats.n_words`); remove or keep as thin wrapper
- **Passages → ClickHouse** if corpus expands past single-SQLite-file comfort zone (200+ GB); use `full_text` index + Python-based snippet generation
