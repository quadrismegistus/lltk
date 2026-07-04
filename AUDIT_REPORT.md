# LLTK Repository Audit

**Date:** 2026-07-04
**Branch audited:** `llm2` (36 commits ahead of `master`; working tree has uncommitted changes to `cli.py`, `corpus/corpus.py`, `manifest.txt`)
**Method:** Six parallel audit passes (architecture, correctness, testing/CI, packaging/deps/docs, security, hygiene/features). Every high-severity claim below was re-verified against the source; items marked *(confirmed)* were reproduced directly.

---

## Status — updated 2026-07-04 (handoff for future agents)

This section tracks what has been fixed vs. what remains, so work can continue after this session. The detailed findings are unchanged below; this is the live checklist.

### ✅ Done this session (in this PR / branch `llm2`)
- **H1** — restored `download_file_tqdm` (`tools/tools.py`); corpus installs work again. Also aliased `overwrite`→`force` on `download()`.
- **P1** — fixed `IndentationError` in `corpus/internet_archive/internet_archive.py` (module now imports).
- **`text.py:240`** — broadened the metadb except to catch ClickHouse driver errors so the CH→CSV fallback works when CH is down (fixed 5 failing tests).
- **P2** — declared `pyarrow` in `requirements.txt`.
- **H4** — `db/ingest.py` freqs ingest now counts + logs unreadable/corrupt JSONs instead of silently dropping them; fixed `n_failed` over-count.
- **H5** — `tools/constants.py` + `db/passages.py` passage chunker returns a per-text error, logs failures, and **fails fast with a clear message if the NLTK `punkt` model is missing** (was silently producing "0 passages"). `tests/test_passages.py` updated for the new 4-tuple return.
- **CLI guard** — added `_load_or_die` so `info`/`compile`/`preprocess`/`install`/`clean-ocr`/`publish` print a friendly message instead of a traceback on an unknown corpus.
- **Tests:** full suite green — **727 passed, 35 skipped** (skips are env-gated destructive pipeline tests + the dead DuckDB class).
- **ClickHouse operational recovery** (infra, not code): the `/Volumes/chambers` NVMe had filled to 100% (CH exited: no space); freed by pruning the HuggingFace model cache. Then the brew upgrade to CH 26.3.9.8 null-pointered in `setClustersConfig` on the `<remote_servers>` block — replaced with explicit-empty `<remote_servers></remote_servers>`. CH is back up, data intact (3.13 M texts). Config backed up at `~/lltk_data/data/clickhouse-config/config.xml.bak-2026-07-04`. Details in the `project_clickhouse_ops` memory. **CH runs as a manual daemon — it will NOT auto-start on reboot** (no launchd/brew service); restart command is in that memory.

### ⏭️ Remaining — highest priority (NOT done)
1. **H2** — CuratedCorpus whitelist + match-group propagation are dead on ClickHouse (`corpus/synthetic.py:352-403`, call DuckDB-only `.register()`); rewrite as CH `tmp.*` JOINs → wrong ArcX membership silently. High.
2. **H3 / M3 / security S2–S3** — unescaped SQL in `db/wordindex.py` and the web layer; the backslash-escaping gap makes even "escaped" paths injectable on ClickHouse. Move web SQL to clickhouse-connect bound parameters. Do before any `lltk.net` deploy; the wordindex breakage (`o'er`, `don't`) is user-visible today.
3. **Web security before deploy (S1, S4, S5)** — no auth + `0.0.0.0` bind on both FastAPI apps; path traversal via `get_idx()`/`GET /api/text/{_id:path}`; rotate CH creds + bind loopback + read-only DB user. See §5.
4. **Branch consolidation** — merge this PR (`llm2`→master), then rebase the `no-stars` and `spring-cleaning2` cleanup branches (they'll conflict worse the longer they sit). Widen CI triggers to all branches (llm2 has never had CI).
5. **Test coverage** — `cli.py`, `db/minhash.py`, `db/ocr_accuracy.py` have 0%; un-`xfail` the freqs/text_words pipeline tests (fix empty-password worker auth).
6. **Docs** — reconcile version to 0.10.0 in CLAUDE.md; fix the three different test counts (762 actual); add a ClickHouse/docker setup section + the `chdb://` no-server quickstart to README.
7. **Cleanup** — delete dead code/files: `db/migrate.py`, `scripts/minhash_match.py`, `notebooks/ecoo2txt.ipynb` (typo, superseded), `manifest-test.txt`, the 58 MB `lltk/web/data/data.fields.pickle`; prune merged/dead branches. `requires-python` → `>=3.10`.
8. **Corpus boilerplate** — manifest-driven genre stamping to delete ~20 clone modules (§6).

### ⚠️ Open questions on the publish WIP committed in this PR
- `corpus/corpus.py` has **6 commented-out `log.debug` lines** in hot paths (`get_text`/`get_text_id`/`init_text`) — a perf hack. Replace with a lazy-logging guard (or delete) rather than leaving commented.
- `manifest.txt`: `EEBO_TCP` changed to `public = freqs` while its `url_metadata/txt/xml` remain — looks like a mid-publish intermediate state; **verify intent**.
- `_dropbox_cmd()` (`corpus.py:806`) falls back to `bin/dropbox_uploader.sh`, which doesn't exist on this branch — the uploader tool must be on `PATH`.

---

## Executive summary

LLTK is a large, mature, and — for a research package — unusually clean codebase (only 4 TODO/FIXME markers in the whole `lltk/` tree; zero bare `except:`; a real 762-test suite with a hermetic chdb harness). The problems are concentrated in three areas:

1. **A few shipping-broken spots** that block fresh-machine use — a missing function that crashes every corpus install, and a module that doesn't even compile.
2. **Systemic silent-failure patterns** in the ClickHouse data pipelines that turn corrupt/missing inputs into invisible undercounts rather than errors.
3. **A web layer that is safe on localhost but not deployable to `lltk.net` as-is** — no auth, SQL injection, and path traversal.

Plus normal-for-a-research-repo debt: three divergent long-lived branches that need consolidating, ~20 boilerplate corpus modules, and stale docs (three different test counts, a version off by three minor releases).

### Fix now (correctness — reachable today)

| # | Issue | Location | Confirmed |
|---|---|---|---|
| H1 | `download()` calls `download_file_tqdm`, which was deleted and is defined nowhere → every `Corpus.install()` that fetches a zip crashes with `NameError` | `lltk/tools/tools.py:873` | ✅ |
| P1 | `internet_archive.py` has an `IndentationError` — the module cannot be imported at all | `lltk/corpus/internet_archive/internet_archive.py:77` | ✅ |
| H4 | Freqs ingest silently drops unreadable JSONs; the resumable anti-join then re-skips them forever → permanent invisible undercount | `lltk/db/ingest.py:28-41,80` | reported |
| H5 | Passage chunker swallows all exceptions; a missing NLTK `punkt` model makes the whole passage build a silent no-op ("0 passages" reported as success) | `lltk/tools/constants.py:470-487` | reported |
| H2 | CuratedCorpus whitelist + match-group propagation are dead on ClickHouse (call DuckDB-only `.register()`, `AttributeError` swallowed) → wrong ArcX corpus membership, silently | `lltk/corpus/synthetic.py:352-403` | reported |
| A1 | `PATH_CORPUS` import swallowed during every `import lltk` (runs before the constant is defined) → `SOURCES` frozen at `['.']`, corpus dir never searched via that path | `lltk/tools/tools.py:1031` | ✅ |
| A2 | `warnings.filterwarnings('ignore')` at import time — the library globally silences all warnings for every downstream consumer | `lltk/imports.py:4` | ✅ |

### Fix before deploying `lltk.net` (security — currently masked by localhost-only use)

| # | Issue | Location |
|---|---|---|
| S1 | Both FastAPI apps bind `0.0.0.0` with **no authentication**; the annotate app exposes unauthenticated write/delete endpoints (`/api/match/link`, `/api/texts/bulk-annotate`) | `web/app.py:723`, `web/annotate.py:490` |
| S2 | Clean SQL injection: `corpus`/`genre` query params interpolated with **no escaping** | `web/app.py:150,202,204,276` |
| S3 | Systemic escaping gap: every "escaped" path doubles `'` but not `\`; ClickHouse honors backslash escapes, so even sanitized inputs (and `db.get()`) are injectable | `db/metadb_ch.py:91`, `db/passages.py:27`, +8 more |
| S4 | Path traversal: `get_idx()` preserves `/` and `.`, `GET /api/text/{_id:path}` reads arbitrary `*.txt` off the host | `text/utils.py:134`, `web/app.py:321` |
| S5 | ClickHouse `lltk`/`lltk` creds hardcoded in 3 code sites; docker-compose publishes 8123/9000 on all interfaces with `DEFAULT_ACCESS_MANAGEMENT=1` | `db/metadb_ch.py:33`, `cli.py:364,464` |

---

## 1. Correctness / bugs

### High severity

**H1 — Corpus installation is broken outright *(confirmed)*.**
`download_file_tqdm` was removed in commit `33af101` ("gut tools.py") but its only caller `download()` remained (`tools/tools.py:867-873`). It is defined nowhere. Every `Corpus.install()` path that downloads a zip (`corpus.py:1001`) plus `dta.py`, `hathi.py`, `hathi_bio.py`, `hathi_englit.py` raises `NameError`. This directly undercuts the new `publish` workflow in the working tree — publishing URLs users then can't consume. Secondary: `hathi_bio.py:114` passes `overwrite=False`, a kwarg `download()` doesn't accept. **Fix:** restore `download_file_tqdm` (requests + `copyfileobj` + the existing tqdm helper), alias `overwrite`→`force`.

**H2 — CuratedCorpus whitelist & propagation are dead code on ClickHouse.**
`synthetic.py:352-403` calls `lltk.db.conn.register(...)`, but `lltk.db` is `MetaDBCH` whose `conn` returns a `_LegacyConnShim` implementing only `execute()`. `.register` raises `AttributeError`, swallowed by `except Exception: pass` with no logging; the propagation SQL also references DuckDB-era `match_db.match_groups`. Effect: annotating a text into an ArcX corpus does nothing unless it already passed SOURCES filters, and annotations never propagate to match-group siblings. This matches the known gotcha documented in CLAUDE.md — now located and confirmed as fully inert. **Fix:** rewrite as CH `tmp.*` Memory-table JOINs (the pattern already used in `fetch_metadata`), or at minimum log when the block is skipped.

**H3 — Unescaped SQL in the ngram path, web-reachable.**
`db/wordindex.py:145` builds `'word'` lists with no escaping; `:265` interpolates `word` raw; `:93/:98` use Python `repr()` for quoting (which emits double quotes CH reads as identifiers). Reachable from `/api/ngram` and `/api/ngram/{word}/examples` (`web/app.py:481-616`). Searching `o'er`, `'tis`, `don't` — ubiquitous in the poetry corpora this index serves — produces malformed SQL / 500s, and a crafted `word` is an injection vector. **Fix:** route through a corrected `_sql_str()`; never use `repr()` for SQL quoting.

**H4 — Freqs ingest silently drops unreadable JSONs.**
`db/ingest.py:28-41` returns `None` on any exception (including `int(v)` on a non-numeric value); `:80` skips `None`. No log, and the "resumable" LEFT ANTI JOIN (`:166`) re-fails the same file every run forever. Downstream `text_words`, ngram counts, MFW, and lang detection silently miss those texts while the corpus reports as fully ingested. **Fix:** return a `(path, error)` sentinel, count and log skips, surface the total.

**H5 — Passage chunker swallows everything.**
`tools/constants.py:470-487` returns an empty passage list on any exception. If `punkt`/`punkt_tab` isn't downloaded, `sent_tokenize` raises `LookupError` for every text, `build_passages_ch` writes nothing and logs "0 passages from N texts" as success. Same for any decode error → silent coverage holes feeding passage exports, embeddings, and LLM tasks. **Fix:** let `LookupError` propagate (or probe punkt once before the pool), log per-text failures with a counter.

### Medium severity (selected)

- **M1 — `install()` retries a bad zip forever** (`corpus.py:1005`): on `BadZipFile` it deletes and recursively re-installs; a URL that persistently serves an HTML error page (e.g. a revoked Dropbox `rlkey` — exactly what the uncommitted manifest churn risks) loops to `RecursionError`. Add a retry counter.
- **M2 — `.conn` shim `?`-inlining corrupts SQL** (`metadb_ch.py:156-172`): `sql.replace('?', p, 1)` re-targets a `?` *inside* an already-inlined value. Question-mark titles ("Who's the Dupe?") break binding. Use server-side `{p:String}` params.
- **M3 — Backslash-blind escaping (repo-wide class)** (`metadb_ch.py:91`, `passages.py:27`, `embeddings.py:68`, `tools/annotations.py:418`, `match.py:406`, `text_words.py:127`, web layer ×4): quote-doubling without backslash-escaping is insufficient on ClickHouse. `adapter.py:275` does it correctly — replicate everywhere. (Same root as security S3.)
- **M4 — `export_passages_ch` violates the >10K IN-clause rule** (`passages.py:478,494,503`) and `export_passages_decile` slices metadata at `[:10_000]` (`:593`): the just-shipped `--ids-file` flag feeds large lists → `max_query_size` error, and decile texts past #10,000 export with empty title/author headers silently. Reuse the tmp-table JOIN.
- **M5 — Passage search errors on punctuated tokens** (`passages.py:55,62,66`): `hasTokenCaseInsensitive` rejects needles with separators, so `search("don't")` throws `BAD_ARGUMENTS`. Pre-split or fall back to `positionCaseInsensitive`.
- **M6 — `enrich_genres_ch` not idempotent** (`db/enrich.py:39-166`): re-running `db-enrich-genres` without a preceding `db-rebuild` re-reads the propagated `texts.genre` as baseline and overwrites `genre_corpus` provenance permanently. Also `:96` `astype(str).fillna('')` is dead (NaN→`'nan'` first); `:78` returns `None` instead of the stats frame. The documented genre-sync "silent regression" itself is present and correct — the fragility is the read-back loop.
- **M7 — Silent per-text/per-corpus drops** in `metadb_ch.texts()` (`:529`), passage-task resolution (`:887`, `passages.py:159`), and `rebuild.py:114,123` (logs at *debug*, so one broken loader silently drops a whole corpus from `lltk.texts` after a "successful" rebuild). Count and log skips at warning level.
- **M8 — ECCO reading**: FD leak (`ecco.py:269` `gzip.open` without `with`) + bare `except:` around strict UTF-8 decode (`:356`) turning one bad byte into an empty text (`n_words=0`). Use `with`, `errors='replace'`, typed excepts.
- **M10 — Web annotator unescaped f-string SQL from HTTP params** (`web/annotate.py:173-176`); doesn't escape LIKE wildcards either. Same fix as M3/S2.
- **M11 — `drop()`/`drop_matches()` swallow TRUNCATE failures** (`metadb_ch.py:342-353`): a failed truncate + rebuild appends into a non-empty table → duplicate rows, no error.
- **M12 — `text_words` resume treats partial ingests as complete** (`text_words.py:132`): a killed run leaves texts with a partial word set that are then permanently anti-join-skipped → silently low ngram counts.

### Low severity (selected)

`read_df(on_bad_lines='skip')` drops malformed CSV rows silently (`tools.py:529`); `get()` filters out legitimately-zero values so `is_translated=0`/`year=0` vanish (`metadb_ch.py:273`); `unzip()` and three `extractall` sites are zip-slip vulnerable (`tools.py:952`, `de_corp.py`, `gallica_literary_fictions`, `german_fiction`); `_ChDBClient.query` references `pa` without importing pyarrow (`adapter.py:252`, test backend only); `clean-ocr` miscounts cache-hits as "cleaned" (`cli.py:731`); `_update_manifest` uses interpolating `ConfigParser` that will raise on any future URL containing `%` (`corpus.py:915`).

**Non-findings (verified clean):** all 26 mutable-default-argument sites are read-only or copied before mutation — no real bugs in that class. The CLAUDE.md `get_txt`/`force_xml` "known-fragile spot" no longer exists (stale note). The genre-enriched-source sync is present and correct.

### Working-tree diff review

The uncommitted changes are one coherent, apparently-completed feature: a **corpus publishing workflow** (`publish` CLI subcommand wiring already-committed `corpus.publish/upload/share/_update_manifest` methods; `_collect_paths` now also bundles `metadata_enriched.*`; manifest regenerated with fresh Dropbox links — evidence it was run successfully). Two things to clean before committing: (1) six `log.debug` calls are **commented out** in hot paths (`get_text`/`init_text`) as a perf hack — replace with a cheap `log.isEnabledFor` guard rather than deleting logging; (2) the `EEBO_TCP` stanza changed `public = raw,metadata,freqs,txt,xml` → `public = freqs` while its other URLs remain — looks like a mid-publish intermediate state, worth confirming intent. **Do not publish these manifests until H1 (install can't download) and M1 (infinite retry on a bad URL) are fixed.**

---

## 2. Architecture / code quality

**A1 — `SOURCES`/`PATH_CORPUS` circular-import swallow *(confirmed live)*.** `tools/tools.py:1031` wraps `from lltk.imports import PATH_CORPUS` in `try/except Exception: pass`, but it runs while `imports.py` is only partway initialized (imports.py imports tools.py before it defines `PATH_CORPUS`), so it *always* fails during normal `import lltk`. Result: `SOURCES == ['.']` and the corpus directory is never searched via that path. **Fix:** resolve `PATH_CORPUS` lazily inside the function body.

**A2 — `warnings.filterwarnings('ignore')` at import time *(confirmed)*** (`imports.py:4`). A library must never globally silence warnings for its consumers. Delete; use targeted context managers at the noisy call sites.

**A3 — Star-import cascade.** The literal `from lltk.imports import *` now survives in just `lltk/__init__.py:1` (spring cleaning did most of the work), but the chained wildcard through it floods the root namespace with **387 public names** including stdlib modules. Finish the `no-stars` branch: explicit `__all__` in `lltk/__init__.py`, move path constants into the dependency-free `tools/constants.py` (the migration is half-done and explicitly commented as circularity-avoidance). This also lets ~160 function-body `from lltk...` imports move to module level.

**A4 — God objects.** `BaseText` = 121 methods / 1,142 lines (`text/text.py`), `BaseCorpus` = 85 methods / 1,383 lines (`corpus/corpus.py`). Highest-value extractions: the 12 date-binning methods and the NLP accessors off `BaseText`; the ~220-line zip/upload/share/publish/manifest block off `BaseCorpus` into a standalone `publish.py`. `metadb_ch.py` is big (1,070 lines) but a well-factored lazy facade — leave it.

**A5 — Dead code to delete.** `db/migrate.py` (149-line one-time DuckDB→CH migration, zero importers); `text/utils.py` skipgram/parse functions (`:384,421,493-539`) and `MaybeListDict`/`stamp_d`/`unstamp_d`; `tools.py` `get_word2pos`/`get_ocr_corrections`; the ZODB-era constants block in `imports.py:19-24`; **`to_corpus_and_id` defined twice verbatim** (`utils.py:241` & `:246` — *confirmed*, the second shadows the first). `db/metadb.py` is a misnamed "gutted DuckDB shim" that actually hosts live code — rename to `prepare.py`.

**A6 — Silent-except sites.** 0 bare `except:`, but 29 `except Exception:` immediately followed by `pass`/`continue`. Worst are the data-pipeline ones already listed as H4/H5/M7/M11. Add aggregate-and-log.

**A7 — Inconsistencies.** `cli.py` is **tab-indented** (658 tab lines) while every other core module is 4-space — add a formatter to CI and reindent. Duplicated utilities with divergent behavior: two `tokenize_fast` (`tools.py:822` always lowercases vs `text/utils.py:794` doesn't) — which you get depends on star-import order; two spelling-modernizer loaders; a copy-pasted CH progress-watcher thread. Accessor zoo (`meta`/`metadata()`/`_meta_`/`meta_()`/`meta_l()`/`meta_1()` on one class). `model/*` and `tools.py` still `print()` from library code (should be `log`). Three modules named annotate-ish (`lltk/annotate.py`, `tools/annotations.py`, `web/annotate.py`) — rename the root one to `tasks.py`.

---

## 3. Testing & CI

- **762 tests** (not the 206 CLAUDE.md claims, nor the 374 the README claims — all three numbers are stale). Real assertions, not smoke tests; good hermetic chdb harness for CH; env-gated real-server tests.
- **5 local failures**, all one root cause and a **real product bug**: `text/text.py:240` catches `(ImportError, AttributeError, RuntimeError, OSError)` but ClickHouse's `OperationalError` descends from `Exception`, not `OSError` — so when CH is down, `t.get()` raises instead of falling back to CSV, breaking the documented "CH first, CSV fallback" contract. **Fix:** add `clickhouse_connect.driver.exceptions.ClickHouseError` to the tuple. (Also a hermeticity smell: "fixture-only" tests were quietly querying the *production* CH until it went down.)
- **~40% coverage.** Zero-coverage important modules: `cli.py` (423 stmts, entirely untested — llm2 added +255 lines here), `corpus/synthetic.py` (285 — the H2 bug lives here), `db/match.py` (200, the 6-tier dedup core), `db/minhash.py` (new, 0%), `db/ocr_accuracy.py` (new, 0%), `db/embeddings.py` (235). Coverage config omits `lltk/web/*` and all corpus modules, hiding the new 521-line ECCO parser (which *does* have tests).
- **CI gaps:** `llm2` gets **no CI at all** — triggers are `push:[master,spring-cleaning]` / `pr:[master]`, and the branch isn't even pushed. A ~5K-line diff has never run through CI. No lint/type-check step. CI ClickHouse image is `:latest` while docker-compose pins `24.10` (drift). Two core pipeline stages (freqs, text_words) are **`xfail`-masked** in `test_ch_pipeline.py:119-137` over an empty-password worker-auth issue — they're never validated in CI.
- **Stale artifacts:** repo-root `coverage.json`/`.coverage` reference pre-move `lltk/tools/clickhouse_*.py` paths (regenerate or drop); `test_core.py:910` is a 14-test DuckDB class permanently `@skip`'d since 0.8.0 — delete.

---

## 4. Packaging, dependencies, docs

- **P1 — `internet_archive.py:77` IndentationError *(confirmed)*** — `with logmap(...)` followed by an unindented body; the module can't be imported, so `lltk.load('internet_archive')` fails. One-line fix.
- **P2 — `pyarrow` is a de-facto hard dependency declared nowhere.** Imported in 11 files; parquet caching is a headline feature; CI works around it with a manual `pip install chdb pyarrow`. End users don't get it. Add to `requirements.txt`.
- **P3 — `requires-python = ">=3.8"` is false; real floor is 3.10.** `str.removesuffix` (3.9), PEP 604 `str | None` in runtime-evaluated signatures with no `from __future__ import annotations` (`estc_json_parser.py`, 3.10), PEP 585 `list[dict]` (`ecco.py`). CI floor is already 3.9. Set `>=3.10`, add per-version classifiers, update README.
- **P4 — Version doc drift *(confirmed)*:** pyproject `0.10.0` (matches PyPI) vs CLAUDE.md `0.7.0`. No `lltk.__version__` exists — add `importlib.metadata.version('lltk-dh')`.
- **P5 — 58 MB dead pickle tracked:** `lltk/web/data/data.fields.pickle`, referenced by zero Python, is most of why `.git` is 141 MB. `git rm` it (consider `filter-repo`). Also a tracked `epistolary/fig.dynamic_graph.mp4` and several MB-scale notebooks inside the package tree (git bloat, not wheel bloat — none ship in the sdist).
- **P6 — README onboarding gap:** the word "docker" appears in **no** doc, despite `docker-compose.yml` being the intended CH setup path; `[clickhouse]` extra isn't mentioned before `lltk db-rebuild`; and the zero-infrastructure answer already in the code (`chdb://` in-process backend, `adapter.py:194`) is undocumented and `chdb` is undeclared. Add a "Database setup" section; declare `chdb` as an extra and document `LLTK_CLICKHOUSE_URL=chdb://...` as the quickstart.
- **Dep hygiene (medium/low):** missing extras for documented features (`prosodic`, `rapidfuzz` — which silently returns 0.0 similarity when absent, disabling tier-3 matching); unused declared deps (`unidecode`, `recordlinkage`, `plotnine`, `statsmodels`, `wptools`, `geopy`); core deps (`numpy`/`pandas`/`lxml`/`networkx`) entirely unpinned during the numpy-2/pandas-3 transition. `uv.lock` is gitignored yet present — two dependency workflows, neither authoritative. LICENSE year stale (2019); publish.yml uses a long-lived token rather than PyPI Trusted Publishing.

---

## 5. Security

Safe as a single-user localhost tool; **not** deployable to `lltk.net` as-is. Blocking issues (detailed in the Executive summary table S1–S5):

- **No auth on either FastAPI app**, both bind `0.0.0.0`; the annotate app exposes unauthenticated `POST /api/match/link|unlink` (corrupts the dedup graph + triggers expensive recompute) and bulk-annotate. Keep annotate bound to `127.0.0.1`, never expose it; put auth in front of the explorer or make it read-only-DB-user.
- **Clean SQL injection** via unescaped `corpus`/`genre` params (`app.py:150,202,204,276`) — `?corpus=x' UNION ALL SELECT ...`. Plus the **backslash escaping gap** (S3 / M3) that makes even the "escaped" search box and `db.get()` injectable on ClickHouse. **Fix both at once** by moving to clickhouse-connect server-side bound parameters.
- **Path traversal** (`get_idx()` keeps `/` and `.`; `GET /api/text/{_id:path}` → up to 50 KB of any `*.txt` on the host; corpora with `ext_txt=''` expose *any* file). Also triggerable by malicious corpus data even without the web app. Add a `realpath`-containment assertion in `get_path`/`path`.
- **Credentials / CH hardening:** rotate the `lltk`/`lltk` password out of the 3 code sites; bind CH to loopback; add a **read-only** DB user for the public app with `file()`/`url()` disabled (otherwise an injection can read server files / SSRF); CSV export currently leaks internal `path_freqs` columns via `SELECT t.*` — project a public-column whitelist.
- **Download paths (lower priority, operator-invoked):** HathiTrust tarballs fetched over **HTTP** (MITM); three `zf.extractall()` sites are zip-slip primitives (HTTPS-hosted, so trusted TLS is the only guard); no checksum verification anywhere. The ECCO reader's rebuild-path-from-parsed-fields approach (`ecco.py:501`) is the correct pattern to replicate.
- **No action needed:** pickle/`exec` usage is on tool-written caches and trusted local corpus-definition modules — not a remote vector under the current trust model.

---

## 6. Repo hygiene & feature opportunities

**Branch consolidation (highest-value housekeeping).** Three live branches that don't contain each other: `llm2` (current, +36 commits — annotate/OCR/minhash features), `no-stars` (+13, incl. "bump to 0.9.0" and dead-module deletion), `spring-cleaning2` (+27, incl. splitting `imports.py` constants out). The cleanup branches will conflict harder with llm2 the longer they sit. **Recommended order: land llm2 → master first, then rebase the two cleanups.** Also deletable: merged locals (`app2`, `claude`, `clickhouse-migration`, `spring-cleaning`), zero-unique-commit remotes (`newcharnet`, `v2`), and ~6 pre-2025 dead remotes (`graphdb2`, `nomorecassandra`, `newstyle`, `BaseClass`, `reverseback`, `textnotinittext`).

**Files to delete:** `scripts/minhash_match.py` (already promoted to `db/minhash.py` + `db-minhash`; the script even defaults to threshold 0.5 vs the documented 0.7); both `notebooks/ecco2txt*` files (parser promoted to `ecco.py`; note the untracked one has a **typo**, "ecoo"); `manifest-test.txt` (2021, referenced nowhere); the uncompressed `data/word2pos.json` (1.7 MB, has a `.gz` twin).

**CLI UX.** No error handling for unknown corpus — `lltk info nonexistent` prints a raw `AttributeError` traceback (a single `_load_or_die` helper fixes `info`/`compile`/`preprocess`/`install`/`publish`/`clean-ocr`). A 450-line `if/elif` dispatch in `main()` (convert to `set_defaults(func=...)`). Confusable command pairs (`db-match` vs `db-matches`; four `db-word*` commands). Inconsistent flags for the same concept (`-j`/`--num-proc`/`--num-workers`; `--force`/`--rebuild`/`--no-resume`/`--no-skip`). Stale help text ("Show DuckDB metadata store info" — it's ClickHouse). Hardcoded CH URL in `db-tag-genres` ignoring `LLTK_CLICKHOUSE_URL`. No `--version`. README omits ~11 real subcommands.

**Corpus module sprawl (medium effort, high value).** 61 corpus dirs / ~11,700 lines. Tiers: pure boilerplate (`clmet.py` = 5 lines of `pass`), and **~26 "genre-stamp clones"** that override `load_metadata` solely to set `df['genre'] = 'Fiction'`. Supporting `genre = Fiction` / `genre_raw = Novel` keys in `manifest.txt` (applied in `BaseCorpus.load_metadata`) plus a default `class_name` would delete ~20 modules (~300–400 lines) *and* collapse the memorized "ArcX two-file gotcha" into a one-file change. The enriched-parquet fast path is also copy-pasted verbatim 3× (`ecco.py:435`, `eebo_tcp.py:44`, `ecco_tcp.py:56`) — hoist into `BaseCorpus`.

**Feature opportunities.** Only 4 TODO/FIXME markers total (remarkably clean). Beyond fixing H2 (the half-built curated-corpus propagation), the highest-leverage adds are: a `lltk doctor` config/manifest validation command (catches the documented onboarding failures — bad `path_python`/`class_name`, unreachable URLs, missing data dirs); promoting `scripts/assemble_en_wordlist.py` to `lltk db-build-wordlist`; and hardening `_update_manifest` to targeted stanza edits rather than a whole-file `ConfigParser` rewrite.

---

## Prioritized action list

**This week (correctness, low effort, high impact):**
1. Restore `download_file_tqdm` (H1) — installs are broken.
2. Fix `internet_archive.py:77` indentation (P1) — module won't import.
3. Add `clickhouse_connect` errors to the `text.py:240` except tuple — restores CH→CSV fallback, fixes 5 test failures.
4. Add `pyarrow` to `requirements.txt` (P2).
5. Log-and-count instead of silent-drop in `ingest.py` (H4) and `constants.py` chunker (H5).
6. Commit the publish feature (clean up the commented-out `log.debug` first); add unknown-corpus CLI error handling.

**This month (structural):**
7. Land `llm2` → master, widen CI to all branches, then rebase `no-stars`/`spring-cleaning2`.
8. Rewrite CuratedCorpus whitelist/propagation for ClickHouse (H2).
9. Central escape helper (backslash-first) or bound params everywhere (M3 / security S3).
10. Add tests for `cli.py`, `db/minhash.py`, `db/ocr_accuracy.py`; un-`xfail` the freqs/text_words pipeline tests.
11. Bump `requires-python` to `>=3.10`; reconcile version to 0.10.0 across CLAUDE.md/README; fix the three test-count numbers; add a ClickHouse/docker setup section documenting the `chdb://` no-server path.
12. Delete dead code/files/branches per §6; add a ruff format+lint CI step (start by reindenting `cli.py`).

**Before `lltk.net` deploys (security — do not expose the web apps until all done):**
13. Convert all web SQL to server-side bound parameters (S2/S3); whitelist CSV export columns.
14. Add auth; default-bind both apps to `127.0.0.1`; never expose the annotate app.
15. `realpath`-containment check on id→path (S4).
16. Rotate CH creds, bind CH to loopback, add a read-only app user with `file()`/`url()` disabled (S5).
17. `git rm lltk/web/data/data.fields.pickle` (58 MB).
18. HTTPS + zip-slip sanitization + checksums on corpus downloads.

---

*Full per-dimension findings (with every file:line and failure scenario) were produced by the six audit passes; this document is the consolidated, verified summary.*
