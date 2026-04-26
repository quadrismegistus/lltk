# LLTK Call Chains

Traced from source code on 2026-04-25 (commit `2dcf59b`). File:line references are relative to the repo root.

---

## 1. `lltk.load('estc')` -- Corpus Loading

Entry point: `lltk.load` resolves to `lltk.corpus.utils.load` (via star imports: `lltk/__init__.py:1` -> `lltk/imports.py:285` -> `lltk/corpus/__init__.py:2` -> `lltk/corpus/corpus.py` which imports `load_corpus` from `lltk/corpus/utils.py`, but `load` itself is also defined at `lltk/corpus/utils.py:900`).

- `load('estc')` (`corpus/utils.py:900`)
  - Checks `CORPUSOBJD` global cache -- if `'estc'` already loaded and not `force`, returns it immediately
  - Otherwise calls `load_corpus('estc')` (`corpus/utils.py:705`)
    - `load_corpus_manifest('estc', make_path_abs=True)` (`corpus/utils.py:568`)
      - `load_manifest(force=True)` (`corpus/utils.py:633`)
        - Reads all manifest `.txt` files from `PATH_MANIFESTS` (13+ paths) via `configparser`
        - Merges `MANIFEST_DEFAULTS` with each section's config
        - Populates global `MANIFEST` dict, keyed by corpus name (CamelCase, the section header)
        - Returns the full `MANIFEST` dict
      - Looks up `'estc'` -- first as a section name, then iterates all sections checking `cd['id'] == 'estc'`
      - Resolves `path_root` to absolute path under `PATH_CORPUS`
      - Calls `get_python_path()` to find the `.py` file for the corpus class
      - Makes all `path_*` values absolute (relative to `path_root`)
      - Returns a dict with `id`, `path_python`, `class_name`, and all manifest keys
    - Checks `path_python` exists; returns `None` if not
    - `importlib.util.spec_from_file_location(id, path_python)` -- dynamically loads the corpus `.py` module
    - `getattr(module, class_name)` -- gets the corpus class (e.g., `Estc`)
    - Constructs `class_class(**inpd)` where `inpd` merges manifest dict with any input kwargs
      - This calls `BaseCorpus.__init__()` (`corpus/corpus.py:118`)
        - Sets `self.id`, `self.name`, `self._metadf = None`, `self._textd = defaultdict(lambda: None)`
        - Resolves `self._path` = `os.path.join(PATH_CORPUS, self.id)`
        - Iterates all kwargs: any key starting with `path_` gets prefixed with `_` and set as attribute
        - Does NOT call `self.init()` (because `_init=False` by default)
  - Stores result in `CORPUSOBJD[name_or_id]`
  - Returns corpus object

**Caching:**
- Two-level: `CORPUSOBJD` (in `corpus/utils.py:899`) caches by the exact string passed to `load()`.
- `CORPUS_CACHE` (in `corpus/corpus.py:1505`) caches by both `C.id` and `C.name`, but only when using `Corpus()` factory.
- `load()` and `Corpus()` have separate caches and do not share them.

**Pain points:**
- `load_manifest()` is called with `force=True` every time, re-reading all manifest files from disk on every `load_corpus()` call. The `MANIFEST` global provides no actual caching since `force=True` is hardcoded.
- `load_corpus_manifest()` has a mutable default argument `manifestd={}` at line 568 -- this is a classic Python bug. If ever called with a non-empty dict, subsequent calls with no `manifestd` arg see the mutated default. In practice, all callers pass no `manifestd`, so the first line `if not manifestd:` triggers every time and the bug is latent.
- Dynamic module import (`importlib.util.spec_from_file_location`) happens on every uncached `load_corpus()` call -- no module caching.
- The `except AssertionError` at line 731 references undefined variable `e` (should be `except AssertionError as e:`). This would cause a `NameError` inside the except block.

---

## 2. `lltk.Text('_estc/T012345')` -- Text Creation by Address

Entry point: `lltk.Text` resolves to `lltk.text.text.Text` (via `lltk/imports.py:288`: `T=Text`).

- `Text('_estc/T012345')` (`text/text.py:1143`)
  - `is_text_obj(text)` check -- False (it's a string)
  - `is_corpus_obj(text)` check -- False
  - `is_addr_str(text)` check -- True (starts with `_`, contains `/`)
  - Sets `taddr = '_estc/T012345'`
  - Checks `TEXT_CACHE` (global `defaultdict(type(None))`) -- if found and valid, returns cached text
  - `to_corpus_and_id('_estc/T012345')` (`text/utils.py:244`) -> `('estc', 'T012345')`
  - `Corpus('estc')` (`corpus/corpus.py:1507`)
    - Checks `CORPUS_CACHE` -- if found, returns cached corpus
    - Otherwise calls `load_corpus('estc')` (same chain as above)
    - Stores in `CORPUS_CACHE[C.id]` and `CORPUS_CACHE[C.name]`
  - `Corpus('estc').text(id='T012345')` (`corpus/corpus.py:398`)
    - `just_metadata(kwargs)` -- filters out `_`-prefixed keys
    - `is_textish(id)` -- False (plain ID string, not an address)
    - `self.get_text_id(id)` (`corpus/corpus.py:510`)
      - `get_idx('T012345')` (`text/utils.py:132`) -- normalizes via `zeropunc()` (strips punctuation except allowed chars)
      - Returns `'T012345'`
    - `self.get_text(id)` (`corpus/corpus.py:478`)
      - Looks up `self._textd.get('T012345')` -- returns `None` on first access (defaultdict)
    - Since `t is None`: `self.init_text('T012345')` (`corpus/corpus.py:551`)
      - `self.TEXT_CLASS(id='T012345', _corpus=self)` -- constructs a `BaseText` (or subclass)
        - `BaseText.__init__()` (`text/text.py:92`)
          - `Corpus(_corpus)` -- called on the already-constructed corpus object; `is_corpus_obj()` -> True, so returns it immediately
          - Sets `self._meta_hydrated = False`, `self._freqs = None`, etc.
          - `self.id = 'T012345'`
          - **`self.corpus.add_text(self)`** (`corpus/corpus.py:562`) -- stores `self._textd['T012345'] = t`
            - This is a side effect inside `__init__` -- the text registers itself with the corpus before the constructor finishes
          - `self._meta = self.ensure_id(merge_dict(TEXT_META_DEFAULT, self.META, self._meta, meta))`
            - Builds initial meta dict with `_id`, `_corpus`, `id` keys plus any passed metadata
    - Returns the text object
  - `TEXT_CACHE[t.addr] = t` -- caches by address `'_estc/T012345'`
  - Returns text

**Caching:**
- `TEXT_CACHE` (`text/text.py:1142`): global dict keyed by address string
- `corpus._textd`: per-corpus dict keyed by text ID (without corpus prefix)
- Both are populated on first creation

**Pain points:**
- `Corpus(_corpus)` is called inside `BaseText.__init__` at line 104. When `_corpus` is already a corpus object, `Corpus()` still goes through the factory function, checks `is_corpus_obj()`, and stores in `CORPUS_CACHE` -- minor overhead on every text creation.
- `add_text(self)` is called inside `__init__`, meaning partially-constructed text objects are registered with the corpus. If the constructor fails after `add_text`, the corpus holds a broken reference.
- `TEXT_CACHE` is a `defaultdict(type(None))` -- accessing a missing key inserts `None`. The check at line 1196 (`TEXT_CACHE.get(taddr)`) uses `.get()` which avoids this, but any accidental `TEXT_CACHE[key]` access elsewhere would silently insert `None`.

---

## 3. `C.meta` -- Corpus Metadata Access

Entry point: `C.meta` property on `BaseCorpus`.

- `C.meta` (`corpus/corpus.py:748`)
  - Calls `self.load_metadata()` (`corpus/corpus.py:301`)
    - If `self._metadf is not None` and not `force`: returns cached `self._metadf` immediately
    - If `self.path_metadata` doesn't exist: calls `self.install_metadata()` (attempts download)
    - Checks for parquet cache at `self.path_metadata_parquet`:
      - If parquet exists and is newer than CSV:
        - `pd.read_parquet(pq_path)` -> sets index to `col_id` -> caches in `self._metadf` -> returns
    - Falls back to CSV:
      - `read_df_anno(self.path_metadata, dtype=str)` (`text/utils.py:293`)
        - Checks for annotation overlay files (`.anno.xlsx`, `.anno.xls`, `.anno.csv`, `.xlsx`, `.xls`) before falling back to the original file
        - `read_df()` -- pandas CSV/Excel reader from `lltk/tools/tools.py`
      - Sets index to `col_id` (usually `'id'`)
      - `clean_meta(df)` (`corpus/utils.py:290`)
        - `fix_meta(df)` -- removes bad columns (`_llp_`, `corpus`, `index`), reorders `[id, author, title, year]` first
        - If `'year'` column exists: applies `_parse_year()` from `metadb.py` to normalize year values
          - **This import (`from lltk.db.metadb import _parse_year`) triggers loading the entire metadb module**, which imports `metadb_ch`, which instantiates `MetaDBCH()` -- potentially connecting to ClickHouse
      - Saves parquet cache: `df.to_parquet(pq_path)` (silently fails if write fails)
      - Caches in `self._metadf`
      - Returns DataFrame

**Caching:**
- `self._metadf`: per-corpus instance cache. Set to `None` on construction. Persists for lifetime of corpus object.
- Parquet file on disk: survives across sessions. Invalidated when CSV mtime > parquet mtime.

**Pain points:**
- `clean_meta()` imports `_parse_year` from `lltk.db.metadb`, which imports `lltk.db.metadb_ch`, which creates a `MetaDBCH()` singleton. This means simply loading corpus metadata from CSV triggers ClickHouse module initialization. If CH is not running, the import still succeeds (the adapter is lazy), but it's surprising coupling.
- The parquet cache stores the cleaned/year-parsed version. If `_parse_year` logic changes, stale parquet files will serve old results until the CSV is touched.
- `read_df_anno` checks 5 annotation file extensions before falling back to the actual CSV. On every uncached load, this means 5 `os.path.exists()` calls for files that rarely exist.
- `merge_linked_metadata()` is never called by `load_metadata()` -- it exists but must be called explicitly by corpus subclasses that override `load_metadata`. This means `LINKS` cross-corpus joins don't happen automatically.

---

## 4. `C.texts()` / `next(C.texts())` -- Text Iteration

Entry point: `C.texts()` method on `BaseCorpus`.

- `C.texts(*args, **kwargs)` (`corpus/corpus.py:661`)
  - Delegates to `self.iter_texts(*args, **kwargs)` (`corpus/corpus.py:663`)
    - If `texts` argument provided: yields from that list (with optional shuffle/lim)
    - If `self._init` is True: yields from `self._textd.values()` (already-loaded texts)
    - **Otherwise: yields from `self.iter_init()`** (`corpus/corpus.py:716`)
      - `self.load_metadata()` -- full CSV/parquet load (see chain #3 above)
      - If `df` is empty: returns (yields nothing)
      - Gets `ids = list(df.index)` -- all text IDs
      - For each ID:
        - `to_corpus_and_id(id)` -> strips corpus prefix if present, gets plain ID
        - If already in `self._textd`: yields cached text
        - Otherwise:
          - Gets metadata row from DataFrame: `df.loc[id]`
          - Filters out NaN/empty values
          - `self.TEXT_CLASS(id=id, _corpus=self, **row_meta)` -- constructs text with metadata
            - Inside `BaseText.__init__`: `self.corpus.add_text(self)` registers it in `_textd`
          - **Sets `t._meta_hydrated = True`** -- marks the text as already having its metadata, preventing later lazy hydration
          - Stores `self._textd[id] = t`
          - Yields `t`
      - Note: `iter_init` does NOT set `self._init = True`. Only `self.textd` (property) and `self.init()` do that.

**So calling `C.texts()` twice without calling `C.init()` first will:**
1. First call: runs `iter_init()`, constructs all text objects, populates `_textd`
2. Second call: `self._init` is still False, so runs `iter_init()` again
   - But this time, texts are found in `_textd` via the `if id in self._textd` check
   - So it yields cached texts without re-constructing them
   - Still re-loads metadata (since `_metadf` is cached, this is fast)

**Pain points:**
- `iter_init()` never sets `self._init = True`, but `iter_texts` checks `self._init` to decide whether to use `_textd` directly or re-enter `iter_init`. This means every `C.texts()` call re-enters `iter_init()` until someone calls `C.init()` or accesses `C.textd`.
- The `textd` property (line 654) calls `iter_init()` and exhausts the generator with `for t in self.iter_init(): pass`, then sets `self._init = True`. This means accessing `C.textd` loads everything eagerly.
- `iter_init` uses `logmap` progress bar, so repeated calls show the progress bar again (though it completes instantly on cached texts).

---

## 5. `C.t` -- Random Text Sampling

Entry point: `C.t` property on `BaseCorpus`.

- `C.t` (`corpus/corpus.py:594`)
  - **Strategy 1: ClickHouse** (lines 607-616)
    - `from lltk.db.metadb import metadb` -- imports the CH singleton
    - `metadb.conn.execute("SELECT id FROM texts WHERE corpus = ? ...")` 
    - **This uses `.conn` which is a DuckDB-compatibility shim, not the CH adapter directly.** The SQL uses `?` placeholders which are DuckDB-style.
    - If a row is returned: `self.TEXT_CLASS(id=row[0], _corpus=self)` -- constructs a bare text shell (no metadata)
    - Wrapped in try/except: silently falls through on any error
  - **Strategy 2: CSV ID list** (lines 618-623)
    - `self._cached_id_list()` (`corpus/corpus.py:633`)
      - Checks `self._cached_ids` (instance cache)
      - If not cached: `pd.read_csv(path, usecols=['id'], dtype=str)['id'].dropna().tolist()`
        - This reads ONLY the 'id' column from the CSV -- much cheaper than full `load_metadata()`
      - Caches in `self._cached_ids`
    - `self.TEXT_CLASS(id=random.choice(ids), _corpus=self)` -- bare text shell
  - **Strategy 3: In-memory _textd** (lines 626-628)
    - Filters `self._textd.values()` for non-None entries
    - `random.choice(loaded)` -- only if >1 loaded text
  - **Strategy 4: Full corpus load** (lines 630-631)
    - `list(self.texts(progress=False))` -- loads ALL texts
    - `random.choice(ol)` -- picks one
    - This is the nuclear option: loads entire corpus metadata

**Pain points:**
- Strategy 1 uses `metadb.conn` which is a DuckDB shim property. If CH is the only backend (as documented in CLAUDE.md), this likely fails and falls through silently. The SQL uses DuckDB-style `?` parameter placeholders rather than CH-style `{param}` or `%(param)s`.
- The text returned by strategies 1-2 is a bare shell (`_meta_hydrated = False`). Accessing any metadata attribute will trigger `_hydrate_meta()` -> `_corpus_meta_row()`, which tries CH first, then loads full CSV. So the "cheap" random sample has deferred cost.
- Strategy 4 (full load) is extremely expensive for large corpora like ESTC (481K texts) and would be triggered if both CH and CSV-id-column fail.
- `BaseText.__init__` calls `self.corpus.add_text(self)` -- so even a "quick sample" permanently registers the text in `corpus._textd`. Repeated `C.t` calls accumulate texts in memory.

---

## 6. `t.author` / `t.title` / `t.year` -- Metadata Attribute Access

Entry point: Python attribute access on a `BaseText` instance.

### 6a. `t.author` (explicit property)

- `t.author` (`text/text.py:537`)
  - `self._hydrate_meta()` (`text/text.py:246`)
    - If `self._meta_hydrated` is True: returns immediately (no-op)
    - Sets `self._meta_hydrated = True` (prevents re-entry)
    - `self._corpus_meta_row()` (`text/text.py:220`)
      - **Try 1: ClickHouse** (lines 223-229)
        - `from lltk.db.metadb import metadb`
        - `metadb.get(self.corpus.id, self.id)` -> CH query `SELECT * FROM lltk.texts WHERE _id = '...'`
        - If found: returns dict with all columns + unpacked meta JSON
        - Wrapped in try/except: falls through on any error
      - **Try 2: Corpus DataFrame** (lines 231-243)
        - Checks `self.corpus._metadf` (in-memory cache)
          - If cached and ID found: returns row as dict (filtered for non-NaN)
        - If not cached: `self.corpus.load_metadata()` -- full CSV/parquet load
          - If ID found in loaded df: returns row as dict
        - Returns `{}` if all lookups fail
    - If `crow` (corpus row) is non-empty:
      - `self._meta = self.ensure_id(merge_dict(TEXT_META_DEFAULT, self.META, crow, self._meta))`
        - Note the merge order: `TEXT_META_DEFAULT` < `self.META` (class defaults) < `crow` (corpus data) < `self._meta` (local overrides)
        - So local overrides (set at construction time) beat corpus data
  - `return str(self._meta.get('author', ''))` -- direct dict lookup

### 6b. `t.title` -- same pattern as `t.author` (explicit property at line 532)

### 6c. `t.year` (explicit property, more complex)

- `t.year` (`text/text.py:599`)
  - `self._hydrate_meta()` -- same as above
  - `v = self._meta.get('year')` -- tries direct lookup first
  - `pd.to_numeric(v, errors='coerce')` -- converts to number
  - If valid (not NaN): returns numeric year
  - **Fallback:** `self.years` property (`text/text.py:584`)
    - Iterates YEARKEYS = `['year', 'date']`
    - For each key: `self[key+'_l']` which calls `self.get('year_l')`
      - This triggers `__getitem__` -> `get()` -> `_hydrate_meta()` (already done, no-op)
      - The `_l` suffix triggers `self.meta_l('year')` -- fuzzy prefix search across all meta keys
    - Strips to 4-digit numeric values
    - Returns median-ish year from the list

### 6d. Arbitrary attribute: `t.some_field` (no explicit property)

- `t.some_field` triggers `BaseText.__getattr__('some_field')` (`text/text.py:204`)
  - `name.startswith('_')` check -- False
  - `name.startswith('path_')` check -- False (unless it's a path attribute)
  - `self.get('some_field')` (`text/text.py:261`)
    - Checks `self._gcache` -- fast return if cached
    - `self._hydrate_meta()` -- lazy load (see above)
    - Key normalization: checks for `_l`, `_1`, `_` suffixes
    - `meta = merge_dict(self._meta, self.__meta)` -- combines both meta dicts
    - `meta.get(key, default)` -- simple dict lookup
    - Caches result in `self._gcache[key]`
    - Returns value or `None`
  - If `get()` returns `None`: raises `AttributeError`
    - **This is a problem**: a field that legitimately has value `None` will raise `AttributeError` instead of returning `None`. The `__getattr__` at line 209 checks `if res is not None`.

**Caching:**
- `self._meta_hydrated` flag: one-time. Once True, `_hydrate_meta()` is a no-op forever.
- `self._gcache`: per-text dict, caches `get()` results. Never invalidated (even by `update()`).
- `self.corpus._metadf`: corpus-level DataFrame cache.

**Pain points:**
- `_gcache` is populated by `get()` but never cleared by `update()` (`text/text.py:446`). So `t.update({'author': 'New Author'})` changes `_meta` but `t.author` still returns the old cached value from `_gcache` until the text is recreated.
  - However, `t.author` uses the explicit property (line 537), which reads directly from `_meta` -- so explicit properties bypass `_gcache` and see updates correctly. Only `__getattr__`-based access (arbitrary keys) has this stale-cache bug.
- `_hydrate_meta` tries CH first, then CSV. For users without CH, every text's first attribute access incurs a failed CH import + query attempt (caught by except, ~ms overhead, but surprising).
- `merge_dict` at `text/utils.py:581` is defined twice -- the second definition at line 581 overwrites the first (line 572). The surviving version uses `safebool(d)` which returns `{}` for falsy inputs. This is fragile: `safebool(0)` returns `False`, not a dict.

---

## 7. `t.txt` -- Text Content Access

Entry point: `t.txt` property on `BaseText`.

- `t.txt` (`text/text.py:773`)
  - Calls `self.get_txt()` (`text/text.py:817`)
    - If `self._txt` is already set (not empty/None): returns `clean_text(self._txt)`
    - Otherwise calls `_call_text_plain()` (inner function, line 821):
      - If `force_xml` is truthy: tries `self.text_plain(force_xml=force_xml)` with TypeError fallback
      - Otherwise: `self.text_plain()` (`text/text.py:805`)
        - **Path 1: plain text file**
          - `self.path_txt` -> triggers `__getattr__('path_txt')` -> `self.get_path('path_txt')` (`text/text.py:322`)
            - `self.get_path_old('txt')` (`text/text.py:310`)
              - `getattr(t.corpus, 'path_txt', None)` -- gets corpus-level txt directory
                - This hits `BaseCorpus.__getattr__('path_txt')` -> `self.get_path('path_txt')` (`corpus/corpus.py:197`)
                  - Checks `self.__dict__` for `path_txt` or `_path_txt`
                  - Falls back to class attribute
                  - If relative path: joins with `self.path` (corpus root)
                  - Returns absolute path to txt directory (e.g., `~/lltk_data/corpora/estc/txt`)
              - Joins: `os.path.join(corpus_txt_dir, text_id)`
              - Appends extension: `getattr(t.corpus, 'ext_txt', None)` -> `.txt` (or `.txt.gz`)
              - Returns full path (e.g., `~/lltk_data/corpora/estc/txt/T012345.txt`)
            - If `get_path_old` returns a path: uses it
            - Otherwise falls back to `get_path_new('txt')` -> `os.path.join(self.path, 'text.txt')` (per-text directory layout)
          - If `self.path_txt` exists: `_open_file(self.path_txt)` (handles `.gz` transparently) -> reads content
        - **Path 2: XML conversion**
          - If `self.path_xml` exists: `self.XML2TXT.__func__(self.path_xml)` -- class-level XML-to-text converter
    - Caches result in `self._txt`
    - Returns `clean_text(self._txt)` if non-empty, else `''`
      - `clean_text()` (`text/utils.py:671`): runs `ftfy.fix_text()`, replaces HTML entities and special Unicode chars

**Caching:**
- `self._txt`: per-text instance cache. Set on first access. Persists for object lifetime.
- No disk cache for the text content itself (unlike metadata which has parquet cache).

**Pain points:**
- `self.path_txt` resolution goes through `__getattr__` -> `get_path` -> `get_path_old` -> `corpus.__getattr__` -> `corpus.get_path`. That's 5 levels of indirection to resolve a file path. Each text access traverses this chain.
- `get_path_old` calls `getattr(t.corpus, partattr)` which can trigger corpus's `__getattr__` -> `get_path()`. The corpus `get_path` method has its own `__dict__` lookup chain with `path_` and `_path_` prefixed variants. This is confusing and error-prone.
- `text_plain()` checks `self.path_txt` and then `self.path_xml` -- both trigger the full path resolution chain. If neither file exists, two full path resolutions happen for nothing.
- The `_open_file` helper at `text/text.py:52` handles `.gz` files, but the path resolution doesn't know whether the file is gzipped. The corpus manifest's `ext_txt` (e.g., `.txt.gz`) is appended at the `get_path_old` level, so gzip support depends on correct manifest configuration.

---

## 8. `t.freqs()` -- Frequency Data Access

Entry point: `t.freqs()` method on `BaseText`.

- `t.freqs(lower=True, modernize_spelling=None)` (`text/text.py:850`)
  - If `self._freqs` is set and truthy: skips loading
  - Otherwise:
    - `self.path_freqs` -- resolves via same path chain as `path_txt`:
      - `__getattr__('path_freqs')` -> `get_path('path_freqs')` -> `get_path_old('freqs')` -> `corpus.path_freqs / text_id + ext_freqs`
      - Typical result: `~/lltk_data/corpora/estc/freqs/T012345.json`
    - If file doesn't exist: `self.save_freqs_json()` (`text/text.py:844`)
      - Requires `self.path_txt` to exist -- reads full text, tokenizes, counts, saves JSON
      - If no text file either: returns `{}`
    - `orjson.loads(f.read())` -- fast JSON parse (orjson releases GIL, 3-10x faster than stdlib json)
    - `Counter(...)` -- wraps in Counter
    - Caches in `self._freqs`
  - `filter_freqs(self._freqs, modernize=..., lower=...)` (`text/utils.py:825`)
    - Creates new `Counter`, iterating all freq entries
    - If `lower`: lowercases each word
    - If `modernize`: applies MorphAdorner spelling modernization dict (358K entries)
    - Returns new Counter (does NOT update cache -- the cache stores the raw freqs)

**Caching:**
- `self._freqs`: per-text instance cache. Stores raw (unfiltered) Counter.
- The filtered result is NOT cached -- `filter_freqs()` creates a new Counter on every call.

**Pain points:**
- `filter_freqs` is called on every `t.freqs()` call, even when `lower=True` and `modernize=None` (the defaults). It creates a new Counter by iterating all words, lowercasing each one. For a text with 50K unique words, this is ~50K string operations per call. The raw cache stores pre-lowercased data (from `orjson.loads`), so this lowercasing pass often has no effect but still runs.
- `self.save_freqs_json()` at line 853 will silently try to generate freqs from text if the freqs file doesn't exist. For corpora without text files, this silently returns `{}` after checking two non-existent paths.
- The `not hasattr(self,'_freqs') or not self._freqs` check at line 851 is redundant -- `_freqs` is always set in `__init__` to `None`. The `not hasattr` branch can never be True.
- `save_freqs_json` takes a tuple `(path_txt, path_freqs, tokenizer_func)` -- an unusual API that suggests it was designed for `pmap` parallelism and not direct calling.

---

## Summary of Cross-Cutting Concerns

### Redundant Corpus() factory calls
`BaseText.__init__` calls `Corpus(_corpus)` even when `_corpus` is already a corpus object. The `Corpus()` factory checks `is_corpus_obj()`, stores in `CORPUS_CACHE`, and returns -- but this happens on every text construction.

### Three separate object caches
1. `CORPUSOBJD` (in `corpus/utils.py`) -- keyed by the exact string passed to `load()`
2. `CORPUS_CACHE` (in `corpus/corpus.py`) -- keyed by both `C.id` and `C.name`
3. `TEXT_CACHE` (in `text/text.py`) -- keyed by address string

These caches are independent and can hold different objects for the same logical entity if accessed through different paths (e.g., `lltk.load('estc')` vs `lltk.Corpus('estc')`).

### Hydration is always two-phase
Texts are created as bare shells, then metadata is loaded lazily on first attribute access. This means:
- The first attribute access on any text incurs the full hydration cost (CH query or CSV load)
- But `iter_init()` pre-hydrates texts (sets `_meta_hydrated = True`) when iterating through a corpus
- Texts created via `C.t` or `Text('_estc/T012345')` are NOT pre-hydrated

### `__getattr__` is the universal dispatcher
Both `BaseText.__getattr__` and `BaseCorpus.__getattr__` intercept attribute access for `path_*` resolution and (for texts) metadata lookup. This makes debugging difficult -- stack traces show `__getattr__` calls that are really metadata lookups or path resolutions.

### Silent exception swallowing
Multiple try/except blocks catch broad exceptions and silently fall through:
- `_corpus_meta_row()`: catches all exceptions from CH lookup
- `C.t`: catches all exceptions from CH and CSV strategies
- `load_metadata()`: catches exceptions from parquet loading
This makes it impossible to diagnose performance issues or configuration errors without enabling debug logging.
