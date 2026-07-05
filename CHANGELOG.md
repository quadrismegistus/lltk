# Changelog

## 0.11.0

Security hardening (the web explorer is now safe to deploy publicly), correctness
fixes, and packaging fixes. 61 commits since 0.10.0.

### Security
- **SQL-injection hardening** for the ClickHouse query paths: a single canonical
  `ch_quote()` escaper (backslash-safe, which quote-doubling alone was not on
  ClickHouse), applied everywhere; and **server-side bound parameters** for the
  web explorer's user-input queries — no user value is interpolated into SQL.
- **Path-traversal guard** on text id → filesystem resolution (`_safe_path`), so a
  crafted `_id` can't escape the corpus directory via the web app.
- **Web apps default to a loopback bind** (`127.0.0.1`); opt into `0.0.0.0`
  explicitly. Optional **HTTP Basic auth** via `LLTK_WEB_USER`/`LLTK_WEB_PASSWORD`.
- **Credentials are env-only**: `resolve_ch_url()` (env → dev fallback); the
  explorer connects via an optional **read-only** ClickHouse user
  (`LLTK_CLICKHOUSE_URL_READONLY`). See `docs/deploy-security.md`.
- Fixes a live `wordindex` bug: ngram queries used `repr()` and raw f-strings, so
  they 500'd on apostrophe words (`o'er`, `'tis`).

### Fixes
- CuratedCorpus whitelist + match-group propagation now work on ClickHouse (were
  silently no-ops calling DuckDB-only APIs).
- Corpus installs fixed (restored a deleted `download_file_tqdm`).
- `internet_archive` module now imports (was an `IndentationError`).
- Metadata hydration falls back to CSV when ClickHouse is unreachable.
- freqs ingest and passage chunking now surface unreadable/failed inputs instead
  of silently dropping them; passage builds fail fast if the NLTK `punkt` model
  is missing.
- CLI prints a friendly message (not a traceback) on an unknown corpus.

### Features
- **Data-driven corpora**: a corpus with a manifest stanza but no python module
  loads as a plain `BaseCorpus`, and a `genre`/`genre_raw` manifest key stamps
  metadata — no boilerplate class needed for a constant-genre corpus.

### Packaging
- Ship the web frontend build + templates in the wheel (`create_app()` no longer
  crashes on a clean install).
- Declare `pyarrow` as a dependency; `requires-python >= 3.9`.

### Tests
- +64 tests (SQL escaping, web security, data-driven corpora, cli, minhash,
  ocr_accuracy). Suite at 788 passing.
