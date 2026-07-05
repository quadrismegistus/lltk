# CLAUDE.md

## Project overview

LLTK (Literary Language Toolkit) — Python package for computational literary analysis and digital humanities. 60+ literary corpora, text processing, analysis methods.

**Author:** Ryan Heuser | **Package:** `lltk-dh` (PyPI) | **Version:** 0.10.0 | **Python:** >=3.8

## Key patterns

- **Inheritance:** BaseObject -> TextList -> BaseCorpus -> specific corpus classes
- **Factories:** `Text(id)` and `Corpus(id)` return cached objects
- **Lazy hydration:** `C.texts()` yields bare shells; attribute access triggers `_hydrate_meta()` (CH first, CSV fallback). One-time via `_meta_hydrated` flag.
- **Path resolution:** `corpus.path_*` via `__getattr__` -> `get_path()`
- **Manifest:** `manifest.txt` (configparser); merged from package dir + `~/lltk_data/` + user config
- **Data-driven corpora:** a corpus with a manifest stanza but no python module loads as a plain `BaseCorpus` (`load_corpus` fallback). A `genre`/`genre_raw` manifest key stamps metadata via `_apply_manifest_genre` — no boilerplate class or `load_metadata` override needed for a constant-genre corpus.
- **Metadata:** `C.meta` uses `load_metadata()` (CSV -> parquet cache). Standard contract: `id` (index), `title`, `author`, `year`, `genre`, `genre_raw`.
- **Cross-corpus linking:** `LINKS = {target: (my_col, their_col)}` per-corpus; `merge_linked_metadata()` left-joins.

## Data backend: ClickHouse

`lltk.db` = `MetaDBCH` singleton. CSV + freqs JSONs = source of truth; CH = analytical query engine. Config: `~/lltk_data/data/clickhouse-config/config.xml`, data at `~/lltk_data/data/clickhouse/` (2 TB disk). User `lltk`/`lltk` on `localhost:8123`.

### Core tables (database `lltk`)

| Table | Engine | ORDER BY | Purpose |
|---|---|---|---|
| `texts` | RMT | `(corpus, _id)` | metadata per text |
| `corpus_info` | RMT | `corpus` | ingested_at, n_texts |
| `matches` | RMT | `(_id_a, _id_b)` | pairwise dedup links |
| `match_groups` | RMT | `_id` | `group_id` + `rank` per _id |
| `text_freqs` | RMT | `_id` | `Map(String, UInt32)` -- per-text retrieval |
| `text_words` | MT | `(word, _id)` | flat `(word, _id, count, corpus)` -- per-word analytics |
| `text_stats` | RMT | `_id` | `n_words`, `n_unique_words` |
| `text_langs` | RMT | `_id` | lang_detected, coverage, confidence |
| `text_genres` | RMT | `_id` | genre / genre_raw / genre_enriched_source |
| `text_translations` | RMT | `_id` | `is_translated`, `original_lang` |
| `stopwords` | MT | `word` | word -> lang for detect_langs |
| `word_year_corpus` | MT | `(word, year, corpus)` | optional pre-agg ngram cache |
| `year_corpus_totals` | MT | `(year, corpus)` | normalization denominators |
| `annotations` | MT | `(_id, field, source, annotated_at)` | append-only annotation log |
| `annotation_sources` | RMT | `source` | source -> priority |
| `annotations_latest` | VIEW | -- | argMax resolver; one winner per `(_id, field)` |

RMT = ReplacingMergeTree, MT = MergeTree.

**`text_freqs` vs `text_words`:** Both from `freqs/*.json`. freqs = per-text retrieval (`t.freqs()`, abstraction scoring, MinHash). words = per-word analytics (`sum(count) WHERE word='virtue'` via contiguous index scan, sub-second).

### Other storage

- **Passages:** `~/lltk_data/data/metadb_passages.sqlite` -- SQLite + FTS5, ~500-word chunks. Stays SQLite for `snippet()`/BM25.
- **Freqs JSONs:** `{corpus}/freqs/{id}.json` -- source of truth
- **Legacy DuckDB:** `~/lltk_data/data/metadb*.duckdb` -- test backend via `MetaDB` in `metadb.py`

### texts schema

`_id` (String, `_{corpus}/{id}`), `corpus` (LowCardinality), `id`, `title`, `author`, `year` (Nullable Int32), `genre` (LowCardinality), `genre_raw`, `title_norm`, `author_norm`, `path_freqs`, `n_words`, `lang`, `is_translated`, `original_lang`, `lang_detected`, `lang_coverage`, `lang_confidence`, `meta` (JSON string).

### Normalization

- **`normalize_title`**: HTML-unescape, dash-normalize, strip `[]`/abbreviation periods, MorphAdorner spelling modernization (358K entries: u/v, vv/w, i/j, terminal -e), lowercase, strip subtitle after `:;.([,!?`, strip "a novel"/"by the author". `"Loues load-starre"` -> `"loves loadstar"`.
- **`normalize_author`**: lowercase, text before first comma. `"Congreve, William, 1670-1729."` -> `"congreve"`.
- **Blacklist:** `DB_BLACKLIST = {'hathi', 'bighist'}` -- parent hathi has 17M govt docs causing matching explosions. Use subcorpora.

**GENRE_VOCAB**: Fiction, Poetry, Drama, Periodical, Essay, Treatise, Letters, Sermon, Biography, History, Nonfiction, Legal, Speech, Spoken, Criticism, Academic, Almanac, Reference.

### Matching (`db-match`)

| Tier | match_type | Constraint |
|---|---|---|
| 0 | `id_link` | Shared IDs from LINKS/MATCH_LINKS |
| 1a | `exact_norm` | title_norm + author_norm (SQL `lead()` chain-linking) |
| 1b | `exact_norm_year` | title_norm + year (authorless, min len 10) |
| 2a | `containment` | short title in long title, same author, min_sim=0.3 |
| 2b | `containment_year` | same by year (authorless, min len 15) |
| 3 | `fuzzy_title` | Jaro-Winkler > 0.85 within author blocks (opt-in `--fuzzy`) |

Chain linking: `lead(_id) OVER (PARTITION BY ...)` -- N-1 edges, NetworkX connected components. `rank` by `CORPUS_SOURCE_RANKS` (chadwyck=1, earlyprint=2, eebo/ecco_tcp=3, hathi_englit=5, internet_archive=7). `dedup_by='rank'` or `'oldest'`.

Scale: ~2.8M texts, ~1.7M match pairs, ~1.3M texts in ~330K groups. Full run ~2 min.

### Genre enrichment (`db-enrich-genres`)

Writes to `text_genres`. Authority corpora (`fiction_biblio`, `end`, `ravengarside`) override via match groups. Then mirrors back to `texts` via `INSERT INTO texts SELECT ... LEFT JOIN text_genres` + `OPTIMIZE TABLE texts FINAL` (RMT newer-row-wins pattern, ~14s for 2.8M rows). Without this sync, `genre_enriched_source` column stays empty -- silent regression.

### Language detection (`db-detect-langs`)

NLTK + curated stopwords (~1,700 words, 9 langs). JOINs `text_words` with `stopwords`, argmax with thresholds (coverage >= 0.05, confidence >= 2.0). Writes to `text_langs`. Minutes for 2.2M texts.

### Translation detection (`db-detect-translations`)

Match groups with 2+ languages; earliest-year language wins, others get `is_translated=1`. Writes to `text_translations`.

### Annotations (`lltk.tools.annotations`)

Cross-corpus canonical store. `annotations` MergeTree preserves history; `annotations_latest` VIEW picks winner via `argMax(value, (priority, annotated_at))`. HIGHER priority wins. Vocabs in `lltk.tools.vocabs` (no CH/DuckDB deps).

**Source priorities:** human=100, bibliography:*=90, authority_corpus=70, heuristic=50, llm:*=10 (auto-registered).

**Fields:** `genre` (GENRE_VOCAB), `genre_raw`, `is_translated` (bool), `original_lang` (ISO 639-1), `year_estimated` (int), `author_first_name` (str), `exclude` (bool). Extend via `register_field_spec()`.

**SDK:**
```python
from lltk.tools import annotations as A
A.ensure_schema()
A.write(source='llm:gemini-2.5-pro', rows=[{'_id': '_estc/T068056', 'field': 'genre', 'value': 'Fiction', 'confidence': 0.95}], run_id='gemini-pro:2026-04-19')
A.resolve(ids=['_estc/T068056'], fields=['genre'])
A.disagreements('genre', min_sources=2)
```

Values stored as String; `''` = "explicitly unknown" (distinct from no row). `confidence` default 1.0. `meta` JSON blob -- canonical LLM pattern: `{'prompt_variant': '...', 'sp_sha256_12': '85af95161e54', 'sp_len': 3068, 'n_examples': 5}`. `sp_len` = raw constant length. Known hash `85af95161e54` = SUBGENRE_SYSTEM_PROMPT v1.

**Mirror:** `A.mirror_genres_from_texts()` reads genre + genre_enriched_source, writes as `corpus:<id>` (priority 30) or `bibliography:...` (90). ~12s on 1.87M rows. Required for `A.disagreements()` to see lltk-internal vs LLM disagreements.

### CLI

```bash
lltk db-rebuild                        # corpus CSVs -> lltk.texts
lltk db-freqs                          # freqs/*.json -> text_freqs
lltk db-text-words                     # text_freqs -> text_words + text_stats
lltk db-match [--fuzzy]                # dedup tiers 0-3
lltk db-enrich-genres                  # authority genre propagation
lltk db-detect-translations            # cross-lang match groups
lltk db-detect-langs                   # per-text language detection
lltk db-wordindex [--vocab-size 50000] # optional ngram pre-agg
lltk db-info / db-matches / db-match-stats / db-passages / search / app / annotate
```

### Python API

```python
import lltk
lltk.db.rebuild(['estc'])
lltk.db.build_freqs_db(corpora=['estc'])
lltk.db.build_text_words()
lltk.db.match(fuzzy=False)
lltk.db.get('_estc/T012345')              # dict
lltk.db.query("SELECT ...")
lltk.db.texts_df(sources=..., dedup=True, dedup_by='oldest')
lltk.db.texts(sources=..., dedup=True)     # yields BaseText
lltk.db.read_freqs(ids=[...])              # per-text freqs batch
lltk.db.dedup_frame(df, by='rank')
lltk.db.ngram(['virtue', 'honor'], genre='Fiction', dedup=True)
lltk.db.find_matches('Incognita')
lltk.db.corpus_info()                      # ingested_at for staleness
```

Text objects keep source-corpus reference -- `t.path_txt`, `t.freqs()` resolve through originating corpus.

**For abstraction project:** `lltk.db.read_freqs(ids=[...])` or direct `get_adapter('clickhouse://lltk:lltk@localhost:8123/lltk')`. Use `send-peer` to message abstraction-claude at `~/github/abslithists/abstraction` for cross-repo questions.

## Genre classification

Three patterns in `load_metadata()`:
- **ESTC heuristic** (`parse_estc_genre.py`): tiers form (MARC 655$a) > topic (650$a) > title keywords. `FICTION_GENRES = {Fiction, Novel, Romance, Tale, Fable, Picaresque, Epistolary fiction, Imaginary voyage}`. `detect_translation` uses MARC 700$e, 240$l, title/subject keywords.
- **Linked corpora** (ECCO, EEBO_TCP, ECCO_TCP): inherit estc_genre via linked metadata. TCP `medium=Verse`->Poetry, `=Drama`->Drama override inherited.
- **Simple/mapping**: single genre or per-value maps (COCA FIC->Fiction, COHA Film->Drama, etc.).

## Corpus specifics

### Hathi ID normalization
`hathi_id_normalize()`: `mdp/390/15009144422` -> `mdp/39015009144422`, `bc/ark/+=13960=...` -> `bc/ark+=13960=...`. Subcorpora share freqs at `~/lltk_data/corpora/hathi/freqs`.

### fiction_biblio
6,862 metadata-only entries from 6 bibliographies (Mish/Odell 1475-1700, McBurney 1700-39, Beasley 1740-49, Raven 1750-99). All `genre='Fiction'`. Genre reaches digitized texts via match groups + `db-enrich-genres`. ESTC ID normalization: strip prefix, uppercase, strip leading zeros, validate letter+digits.

### END (Early Novels Database)
2,002 MARCXML records (1660-1830), 1,168 with ESTC IDs. Rich metadata: narrative_form, gender, paratexts, publishers, translation.

### ESTC
481K bibliographic records, 42-column `metadata.csv` parsed from MARC JSON. `lltk compile estc` ~3 min.

### EarlyPrint
Combined EEBO/ECCO/Evans TCP, ~60K texts. `xml2txt_earlyprint(use_reg=True)` uses `<w reg="...">` for spelling modernization. `.gz` transparent via `_open_file`. Manifest `ext_xml=.xml.gz`.

### Non-English
**French:** artfl (3.6K), french_pd_books (290K), gallica_literary_fictions (15.5K), paige (3.2K).
**German:** dta (3.3K), german_pd (275K), german_fiction (3.2K), de_corp (~5K).

Conservative genre keywords: FR roman->Fiction, poeme->Poetry, comedie->Drama. DE roman/novelle->Fiction, gedicht->Poetry, komodie->Drama.

**`french_pd_books` and `german_pd` excluded from ArcFiction{Fr,De}** -- title-keyword heuristics unreliable at scale. Re-enable after bibliography authority or LLM genre pass.

## CuratedCorpus

Extends SyntheticCorpus with `annotations.json`:
```python
class ArcFiction(CuratedCorpus):
    SOURCES = {'chadwyck': {}, 'earlyprint': {'genre': 'Fiction'}, ...}
    DEDUP = True; DEDUP_BY = 'oldest'
```

- `SOURCE_HIERARCHY` per-column priority. `exclude` removes text; `__none__` clears.
- Whitelist: any `_id` in annotations.json included regardless of SOURCES filters.
- **New ArcX = two-file change**: class in `arc_corpora.py` AND stanza in `manifest.txt`.
- **Annotation whitelist/propagation silently skips on CH** -- uses DuckDB-only `register(df)`, wrapped in try/except. SOURCES filter works; whitelist + group propagation do not.

## Other features

### Passages DB
SQLite FTS5, ~500-word sentence-aware chunks. `lltk.db.search('virtue', genre='Fiction')`. Filters resolved in CH -> _id set -> FTS5. `NEAR()` and phrase queries supported.

### MinHash matching
`lltk/db/minhash.py` (`lltk db-minhash`) -- word-set overlap via MinHashLSH (`datasketch`, `num_perm=128`), configurable `threshold` (default 0.5). Writes to `matches` as `match_type='minhash'`.

### Prosodic integration
Optional `prosodic>=3.1`. `lltk prosodic-parse <corpus>`, `t.prosodic(cached=True)`. Per-text `syll.parquet`/`parsed.parquet`.

### BookNLP
`t.booknlp.parse()`, `.chardata()`, `.comention_network(window=200)`. LLM resolution via `CharacterTask` in largeliterarymodels.

### LLM tasks (largeliterarymodels)
External at `~/github/largeliterarymodels/`. Cached via hashstash. GenreTask (genre + verification), FryeTask (mode/mythos/narration), CharacterTask.

## Corpus data location

`~/lltk_data/corpora/<corpus_id>/` with `metadata.csv`, `txt/`, optionally `xml/`, `freqs/`. Text files: `txt/{id}.txt` (flat) or `texts/{id}/text.txt` (per-dir). CH data: `~/lltk_data/data/clickhouse/`.

## Performance

Parquet caching (CSV -> .parquet, 5-10x). Enriched parquet for ECCO/EEBO_TCP/ECCO_TCP. `iter_init()` pre-populates text metadata. `pmap` (cpu_count-2). `orjson` for freqs (3-10x, releases GIL).

## Tests

```bash
python -m pytest tests/ -v
```

762 tests using `test_fixture` corpus (3 texts: Blake, Austen, Shelley) -- no external data. CI via GitHub Actions + Codecov.

## Development gotchas

- **Hydration chain**: `BaseText.get(key)` does fuzzy lookup after `_hydrate_meta()`. `_corpus_meta_row()` tries CH -> cached `_metadfd` -> `load_metadata()`.
- **Lazy sampling `C.t`**: `BaseText.__init__` calls `corpus.add_text(self)`, so `_textd` never empty after one sample.
- **Legacy `get_txt`**: `force_xml` only forwarded when truthy, with TypeError fallback.
- **Legacy `.conn` shim**: MetaDBCH rewrites DuckDB SQL for web app: `match_db.*`->`lltk.*`, `json_extract_string`->`JSONExtractString`, `to_timestamp`->`toDateTime`, `random()`->`rand()`, positional `?` inlined.
- **`get_idx()`** preserves spaces/`+`/`$` in IDs. `_open_file` handles `.gz`.

### CH dialect gotchas

- `FROM texts FINAL AS t` (alias after FINAL, or wrap in subquery)
- `INSERT OR IGNORE` -> plain `INSERT` (RMT dedup async; use `FINAL` on reads)
- `ALTER TABLE ... DELETE` + `SETTINGS mutations_sync=1` for sync deletes
- **No correlated subqueries in ALTER UPDATE.** Use `INSERT INTO target SELECT ... LEFT JOIN source` + `OPTIMIZE TABLE target FINAL` (RMT newer-row-wins). See `enrich_genres_ch`.
- `async_insert=1` for many small inserts (avoids "too many parts")
- Streaming reads (`query_arrow_stream`) hold primary session -- separate client for concurrent writes
- CH booleans = `UInt8` with pd.NA. Promote to pandas `boolean` dtype before boolean ops.
- IN-clause >10K values hits `max_query_size`. Insert to `tmp.*` Memory table and JOIN.

## Future work

- Web app: saved queries, author page, text export, comparison/map/network views
- OCR correction factors per-corpus
- Deployment: lltk.net on Hetzner
- Passages -> CH if >200 GB
