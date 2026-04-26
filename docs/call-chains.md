# LLTK Call Chains

Traced from source code on 2026-04-25 (branch `no-stars`, commit `ba1b81a`). File:line references are relative to the repo root. This version reflects the spring-cleaning2 changes: explicit imports throughout, logmap-based logging, constants extracted to `lltk/tools/constants.py`, simplified `BaseText`/`BaseCorpus`, and unified corpus cache.

---

## 1. `lltk.load('estc')` -- Corpus Loading

Entry point: `lltk.load` resolves to `lltk.corpus.utils.load` (via explicit imports: `lltk/__init__.py` -> `lltk/imports.py:98` imports `from lltk.corpus import *` -> `lltk/corpus/__init__.py:2` imports `from .corpus import *`, and `lltk.corpus.utils.load` is the `load()` function).

- `load('estc')` (`corpus/utils.py:899`)
  - Imports `CORPUS_CACHE` from `lltk.corpus.corpus` (line 900)
  - Checks `CORPUS_CACHE` -- if `'estc'` already present and not `force`, returns it immediately (line 901-902)
  - Otherwise calls `load_corpus('estc')` (`corpus/utils.py:705`)
    - Decorated with `@logmap.fn` for automatic entry/exit logging
    - `load_corpus_manifest('estc', make_path_abs=True)` (`corpus/utils.py:568`)
      - Starts with a copy of `MANIFEST_DEFAULTS` (line 570)
      - `load_manifest('estc')` (`corpus/utils.py:633`)
        - If global `MANIFEST` dict is non-empty and not `force`: returns it immediately (line 634) -- **this is the caching gate**
        - Otherwise reads all manifest `.txt` files from `PATH_MANIFESTS` (13+ paths, defined in `imports.py:32-47`) via `configparser`
        - For each section: merges `MANIFEST_DEFAULTS` with section's config values
        - Stores in global `MANIFEST` dict keyed by corpus name (CamelCase section header)
        - Returns full `MANIFEST` or just the requested corpus's entry
      - Looks up `'estc'` -- first as a section name, then iterates all sections checking `cd['id'] == name_or_id` (line 575-577)
      - Resolves `path_root` to absolute path under `PATH_CORPUS` (line 585-586)
      - Calls `get_python_path(path_python, path_root)` (line 595) to find the `.py` file
        - Checks: `{path_root}/{path_python}`, `{path_root}/{module_name}/{path_python}`, `{PATH_TO_CORPUS_CODE}/{path_python}`, `{PATH_TO_CORPUS_CODE}/{module_name}/{path_python}`
      - Makes all `path_*` values absolute (relative to `path_root`) (lines 600-603)
      - Returns dict with `id`, `path_python`, `class_name`, and all manifest keys
    - Checks `path_python` exists; returns `None` if not (lines 715-717)
    - `merge_dict(manifestd, input_kwargs)` -- combines manifest config with any caller kwargs (line 720)
    - `importlib.util.spec_from_file_location(id, path_python)` -- dynamically loads the corpus `.py` module (lines 723-726)
    - `getattr(module, class_name)` -- gets the corpus class (e.g., `Estc`) (line 727)
    - `class_class(**inpd)` -- constructs corpus (line 728)
      - This calls `BaseCorpus.__init__()` (`corpus/corpus.py:116`)
        - Sets `self.id`, `self.name`, `self._metadf = None`, `self._textd = defaultdict(lambda: None)`, `self._init = False`
        - Resolves `self._path = os.path.join(PATH_CORPUS, self.id)` (line 157)
        - Merges `MANIFEST_DEFAULTS` into `attrs` (line 156): `attrs = {**MANIFEST_DEFAULTS, **attrs}`
        - Iterates all attrs: any key starting with `path_` gets prefixed with `_` and set as attribute (lines 158-160)
        - Does NOT call `self.init()` (because `_init=False` by default)
  - Stores result in `CORPUS_CACHE[C.id]` and `CORPUS_CACHE[C.name]` (line 905)
  - Returns corpus object

**Caching:**
- **Unified cache.** Both `load()` (in `corpus/utils.py:899`) and `Corpus()` (in `corpus/corpus.py:1345`) share the same `CORPUS_CACHE` dict (defined at `corpus/corpus.py:1343`). `load()` imports it directly (line 900). Keyed by both `C.id` and `C.name`.
- `MANIFEST` (global in `imports.py:57`): populated once on first `load_manifest()` call, reused on subsequent calls via the `if MANIFEST and not force` guard (line 634). This is a major improvement over the old code which called `load_manifest(force=True)` on every load.

**Remaining issues:**
- `load_corpus_manifest()` has a mutable default argument `manifestd={}` at line 568. In practice all callers pass no `manifestd`, so the first line `if not manifestd:` triggers every time and the bug is latent.
- Dynamic module import (`importlib.util.spec_from_file_location`) happens on every uncached `load_corpus()` call -- no module caching.
- The `except AssertionError` at line 731 references undefined variable `e` in the error message (should be `except AssertionError as e:`). This would cause a `NameError` inside the except block.

---

## 2. `lltk.Text('_estc/T012345')` -- Text Creation by Address

Entry point: `lltk.Text` resolves to `lltk.text.text.Text` (via `lltk/imports.py:102`: `T=Text`).

- `Text('_estc/T012345')` (`text/text.py:1025`)
  - `is_text_obj(id)` check (line 1044) -- False (it's a string)
  - `is_addr_str(id)` check (line 1048) -- True (starts with `_`, contains `/`)
  - Sets `taddr = '_estc/T012345'` (line 1049)
  - Checks `TEXT_CACHE.get(taddr)` (line 1059) -- plain dict, `.get()` returns `None` on miss
  - `to_corpus_and_id('_estc/T012345')` (`text/utils.py:244`) -> `('estc', 'T012345')`
  - `Corpus('estc')` (`corpus/corpus.py:1345`)
    - Checks `CORPUS_CACHE` (line 1358) -- if found, returns cached corpus
    - Otherwise calls `load_corpus('estc')` (same chain as above)
    - Stores in `CORPUS_CACHE[C.id]` and `CORPUS_CACHE[C.name]` (line 1366)
  - `Corpus('estc').text(id='T012345')` (`corpus/corpus.py:354`)
    - `just_metadata(kwargs)` -- filters out `_`-prefixed keys
    - `self.get_text_id(id)` (`corpus/corpus.py:463`)
      - `get_idx('T012345')` (`text/utils.py:132`) -- normalizes via `zeropunc()` (strips punctuation except allowed chars)
      - Returns `'T012345'`
    - `self.get_text(id)` (`corpus/corpus.py:431`)
      - Looks up `self._textd.get('T012345')` -- returns `None` on first access (defaultdict)
    - Since `t is None`: `self.init_text('T012345')` (`corpus/corpus.py:504`)
      - `self.TEXT_CLASS(id='T012345', _corpus=self)` -- constructs a `BaseText`
        - `BaseText.__init__()` (`text/text.py:87`)
          - `from lltk import Corpus` (line 98) -- local import
          - `self.corpus = Corpus(_corpus)` (line 99) -- when `_corpus` is already a corpus object, `Corpus()` checks `is_corpus_obj()` (line 1350), returns it, and caches in `CORPUS_CACHE`
          - Sets `self._meta_hydrated = False`, `self._freqs = None`, etc. (lines 103-114)
          - `self.id = 'T012345'` (line 118)
          - **`self.corpus.add_text(self)`** (`corpus/corpus.py:515`) -- stores `self._textd['T012345'] = t` (line 119)
            - Side effect inside `__init__` -- the text registers itself with the corpus before the constructor finishes
          - `self._meta = self.ensure_id(merge_dict(TEXT_META_DEFAULT, self.META, self._meta, meta))` (lines 120-125)
            - Builds initial meta dict with `_id`, `_corpus`, `id` keys plus any passed metadata
    - Returns the text object
  - `TEXT_CACHE[t.addr] = t` (line 1073) -- caches by address `'_estc/T012345'`
  - Returns text

**Caching:**
- `TEXT_CACHE` (`text/text.py:1022`): plain dict (no longer a `defaultdict`) keyed by address string. Accessing a missing key does NOT auto-insert `None`.
- `corpus._textd`: per-corpus `defaultdict(lambda: None)` keyed by text ID (without corpus prefix)
- Both are populated on first creation.

**Remaining issues:**
- `Corpus(_corpus)` is called inside `BaseText.__init__` at line 99. When `_corpus` is already a corpus object, `Corpus()` still goes through the factory function, checks `is_corpus_obj()`, stores in `CORPUS_CACHE`, and returns -- minor overhead on every text creation.
- `add_text(self)` is called inside `__init__`, meaning partially-constructed text objects are registered with the corpus. If the constructor fails after `add_text`, the corpus holds a broken reference.

---

## 3. `C.meta` -- Corpus Metadata Access

Entry point: `C.meta` property on `BaseCorpus`.

- `C.meta` (`corpus/corpus.py:705`)
  - Calls `self.load_metadata()` (`corpus/corpus.py:257`)
    - If `self._metadf is not None` and not `force`: returns cached `self._metadf` immediately (line 258-259)
    - If `self.path_metadata` doesn't exist: calls `self.install_metadata()` (line 260)
    - If still doesn't exist: returns empty DataFrame (line 261)
    - Checks for parquet cache at `self.path_metadata_parquet` (line 263-275):
      - `self.path_metadata_parquet` (line 253-255) derives from `self.path_metadata` -- replaces `.csv` extension with `.parquet`
      - If parquet exists and `pq_mtime >= csv_mtime`:
        - `pd.read_parquet(pq_path)` -> sets index to `col_id` -> caches in `self._metadf` -> returns
      - Catches `(OSError, IOError, ValueError, KeyError)` and falls through
    - Falls back to CSV (line 277):
      - `read_df_anno(self.path_metadata, dtype=str)` (`text/utils.py:293`)
        - Checks annotation overlay files (`.anno.xlsx`, `.anno.xls`, `.anno.csv`, `.xlsx`, `.xls`) before falling back to the original file
        - `read_df()` -- pandas CSV/Excel reader from `lltk/tools/tools.py`
      - Sets index to `col_id` (usually `'id'`) (lines 279-280)
      - `clean_meta(df)` (`corpus/utils.py:290`)
        - `fix_meta(df)` -- removes bad columns (`_llp_`, `_lltk_`, `corpus`, `index`), reorders `[id, author, title, year]` first
        - If `'year'` column exists: applies `_parse_year()` **from `lltk.tools.constants`** (line 293)
          - **Key change:** `_parse_year` now lives in `lltk/tools/constants.py:412`, not in `metadb.py`. This import does NOT trigger ClickHouse module loading.
      - Saves parquet cache: `df.to_parquet(pq_path)` (lines 284-287; silently fails on `OSError/IOError/PermissionError`)
      - Caches in `self._metadf` (line 289)
      - Returns DataFrame (line 290)

**Caching:**
- `self._metadf`: per-corpus instance cache. Set to `None` on construction. Persists for lifetime of corpus object.
- Parquet file on disk: survives across sessions. Invalidated when CSV mtime > parquet mtime.

**Improvements over old code:**
- `_parse_year` import moved from `lltk.db.metadb` to `lltk.tools.constants`. Loading metadata from CSV no longer triggers ClickHouse module initialization.

**Remaining issues:**
- The parquet cache stores the cleaned/year-parsed version. If `_parse_year` logic changes, stale parquet files will serve old results until the CSV is touched.
- `read_df_anno` checks 5 annotation file extensions before falling back to the actual CSV. On every uncached load, this means 5 `os.path.exists()` calls for files that rarely exist.
- `merge_linked_metadata()` exists but is not called by `load_metadata()` -- corpus subclasses that need cross-corpus joins must override and call it explicitly.

---

## 4. `C.texts()` / `next(C.texts())` -- Text Iteration

Entry point: `C.texts()` method on `BaseCorpus`.

- `C.texts(*args, **kwargs)` (`corpus/corpus.py:608`)
  - Delegates to `self.iter_texts(*args, **kwargs)` (`corpus/corpus.py:611`)
    - If `texts` argument provided: yields from that list (with optional shuffle/lim)
    - If `self._init` is True: yields from `self._textd.values()` (already-loaded texts)
    - **Otherwise: yields from `self.iter_init()`** (`corpus/corpus.py:662`)
      - If `self._init` is True: yields from `_textd` values directly and returns (short-circuit at lines 663-668)
      - `self.load_metadata()` -- full CSV/parquet load (see chain #3 above)
      - If `df` is empty: sets `self._init = True` and returns (lines 671-673)
      - Gets `ids = list(df.index)` -- all text IDs (line 675)
      - `full_init = (not lim or lim >= len(df.index))` (line 679) -- tracks whether this is a complete iteration
      - Uses `logmap` context manager for progress: `with logmap(desc) as lm:` (line 681)
      - For each ID (line 682):
        - `to_corpus_and_id(id)` -> strips corpus prefix if present, gets plain ID (line 683)
        - If already in `self._textd` and not None: yields cached text (line 684-685)
        - Otherwise (lines 686-692):
          - Gets metadata row from DataFrame: `df.loc[id]`
          - Filters out NaN/empty values
          - `self.TEXT_CLASS(id=id, _corpus=self, **row_meta)` -- constructs text with metadata
            - Inside `BaseText.__init__`: `self.corpus.add_text(self)` registers it in `_textd`
          - **Sets `t._meta_hydrated = True`** -- marks the text as already having its metadata, preventing later lazy hydration
          - Stores `self._textd[id] = t`
          - Yields `t`
      - **Sets `self._init = True` if `full_init`** (lines 693-694) -- this now happens at the end of iteration

**Improvement over old code:**
- `iter_init()` now sets `self._init = True` when a full iteration completes (line 693-694). Previously it never did, causing every `C.texts()` call to re-enter `iter_init()`.
- `iter_init()` has a short-circuit: if `self._init` is already True, it yields from `_textd` directly without touching `load_metadata()` again (lines 663-668).

**So calling `C.texts()` twice:**
1. First call: runs `iter_init()`, constructs all text objects, populates `_textd`, sets `self._init = True`
2. Second call: `self._init` is True, so `iter_texts` yields from `_textd.values()` directly (line 618-621). No re-entry into `iter_init`.

**Remaining issues:**
- The `textd` property (line 602) calls `iter_init()` and exhausts the generator, then sets `self._init = True`. This means accessing `C.textd` loads everything eagerly. But `num_texts` and `text_ids` (lines 627-629) both use `self.textd`, so accessing them forces a full load.
- `logmap` progress bar appears during first iteration. If lim is used (partial iteration), `self._init` stays False and subsequent calls re-enter `iter_init()` -- but cached texts are yielded from `_textd` without reconstruction.

---

## 5. `C.t` -- Random Text Sampling

Entry point: `C.t` property on `BaseCorpus`.

- `C.t` (`corpus/corpus.py:542`)
  - **Strategy 1: ClickHouse** (lines 554-564)
    - `from lltk.db.metadb import metadb` -- imports the CH singleton
    - `metadb.conn.execute("SELECT id FROM texts WHERE corpus = ? ORDER BY random() LIMIT 1", [self.id])`
    - **This uses `.conn` which is a DuckDB-compatibility shim.** The SQL uses `?` placeholders (DuckDB-style).
    - If a row is returned: `self.TEXT_CLASS(id=row[0], _corpus=self)` -- constructs a bare text shell
    - Wrapped in `except (ImportError, AttributeError, RuntimeError, OSError)` -- specific exception types, not bare `except`
  - **Strategy 2: CSV ID list** (lines 567-572)
    - `self._cached_id_list()` (`corpus/corpus.py:581`)
      - Checks `self.__dict__.get('_cached_ids')` -- instance cache (line 583)
      - If not cached: `pd.read_csv(path, usecols=['id'], dtype=str)['id'].dropna().tolist()` (line 592)
        - Reads ONLY the `id` column from the CSV -- much cheaper than full `load_metadata()`
      - Caches in `self._cached_ids` (line 595)
    - `self.TEXT_CLASS(id=random.choice(ids), _corpus=self)` -- bare text shell
  - **Strategy 3: In-memory _textd** (lines 574-576)
    - Filters `self._textd.values()` for non-None entries
    - `random.choice(loaded)` -- only if >1 loaded text
  - **Strategy 4: Full corpus load** (lines 578-579)
    - `list(self.texts(progress=False))` -- loads ALL texts
    - `random.choice(ol)` -- picks one
    - This is the nuclear option: loads entire corpus metadata

**Remaining issues:**
- Strategy 1 uses `metadb.conn` which is a DuckDB shim property. If CH is the only backend, this likely fails and falls through silently. The SQL uses DuckDB-style `?` parameter placeholders rather than CH-style `{param}`.
- The text returned by strategies 1-2 is a bare shell (`_meta_hydrated = False`). Accessing any metadata attribute will trigger `_hydrate_meta()` -> `_corpus_meta_row()`, which tries CH first, then loads full CSV. So the "cheap" random sample has deferred cost.
- Strategy 4 (full load) is extremely expensive for large corpora like ESTC (481K texts).
- `BaseText.__init__` calls `self.corpus.add_text(self)` -- so even a "quick sample" permanently registers the text in `corpus._textd`.

---

## 6. `t.author` / `t.title` / `t.year` -- Metadata Attribute Access

Entry point: Python attribute access on a `BaseText` instance.

### 6a. `t.author` (explicit property)

- `t.author` (`text/text.py:481`)
  - `return str(self.get('author') or '')` -- delegates to `self.get()`
  - `self.get('author')` (`text/text.py:254`)
    - `self._hydrate_meta()` (`text/text.py:239`)
      - If `self._meta_hydrated` is True: returns immediately (no-op) (line 241-242)
      - Sets `self._meta_hydrated = True` (prevents re-entry) (line 243)
      - `self._corpus_meta_row()` (`text/text.py:213`)
        - **Try 1: ClickHouse** (lines 216-221)
          - `from lltk.db.metadb import metadb`
          - `metadb.get(self.corpus.id, self.id)` -> CH query
          - Wrapped in `except (ImportError, AttributeError, RuntimeError, OSError)` -- specific exceptions
        - **Try 2: Corpus DataFrame** (lines 224-236)
          - Checks `self.corpus._metadf` (in-memory cache)
            - If cached and ID found: returns row as dict (filtered for non-NaN)
          - If not cached: `self.corpus.load_metadata()` -- full CSV/parquet load
            - If ID found in loaded df: returns row as dict
          - Returns `{}` if all lookups fail
      - If `crow` (corpus row) is non-empty (lines 245-252):
        - `self._meta = self.ensure_id(merge_dict(TEXT_META_DEFAULT, self.META, crow, self._meta))`
          - Merge order: `TEXT_META_DEFAULT` < `self.META` (class defaults) < `crow` (corpus data) < `self._meta` (local overrides)
          - So local overrides (set at construction time) beat corpus data
    - `meta = self._meta` (line 262)
    - `meta.get('author', None)` -- direct dict lookup (line 264)
  - Returns `str(result or '')` (line 481)

### 6b. `t.title` -- same pattern as `t.author` (explicit property at line 479)

### 6c. `t.year` (explicit property, more complex)

- `t.year` (`text/text.py:525`)
  - `v = self.get('year')` -- triggers `_hydrate_meta()` if needed (line 526)
  - `pd.to_numeric(v, errors='coerce')` -- converts to number (line 528)
  - If valid (not NaN): returns numeric year (lines 529-530)
  - **Fallback:** `self.years` property (`text/text.py:510`)
    - Iterates `YEARKEYS = ['year', 'date']` (defined in `tools/constants.py:190`)
    - For each key: `self[key+'_l']` which calls `self.get('year_l')` (line 513)
      - The `_l` suffix triggers `self.meta_l('year')` (line 258) -- prefix search across all meta keys
    - Strips to 4-digit numeric values
    - Returns median-ish year from the list

### 6d. Arbitrary attribute: `t.some_field` (no explicit property)

- `t.some_field` triggers `BaseText.__getattr__('some_field')` (`text/text.py:197`)
  - `name.startswith('_')` check -- raises `AttributeError` immediately for private attrs (line 198)
  - `name.startswith('path_')` check -- delegates to `self.get_path(name)` (line 199)
  - `self.get('some_field')` (`text/text.py:254`)
    - `self._hydrate_meta()` -- lazy load (see above)
    - `meta = self._meta` (line 262)
    - `meta.get(key, default)` -- direct dict lookup (line 264)
    - Returns value or `None`
  - If `get()` returns `None`: raises `AttributeError` (line 204)
    - **Caveat**: a field that legitimately has value `None` will raise `AttributeError` instead of returning `None`.

**Caching:**
- `self._meta_hydrated` flag: one-time. Once True, `_hydrate_meta()` is a no-op forever.
- `self._meta`: the dict itself, populated during hydration. No secondary cache layer.
- `self.corpus._metadf`: corpus-level DataFrame cache.

**Improvements over old code:**
- All explicit properties (`title`, `author`, `genre`, `genre_raw`, etc.) now go through `self.get()` (lines 479-499), which is the single hydration gateway. The old code had properties reading directly from `_meta` after calling `_hydrate_meta()` separately.
- No more `_gcache` dict. The old code had a separate per-text dict caching `get()` results that was never invalidated by `update()`. Now `get()` reads directly from `_meta` every time, so `t.update({'author': 'New Author'})` is immediately visible.
- `_hydrate_meta` tries CH first, then CSV, with specific exception types instead of bare `except`.

**Remaining issues:**
- `_hydrate_meta` tries CH first, then CSV. For users without CH, every text's first attribute access incurs a failed import + query attempt (caught by except, ~ms overhead, but surprising).
- `merge_dict` at `text/utils.py:572` uses `safebool(v)` which returns `False` for zero-valued inputs. So a metadata field with value `0` would be silently dropped during merge.

---

## 7. `t.txt` -- Text Content Access

Entry point: `t.txt` property on `BaseText`.

- `t.txt` (`text/text.py:698`)
  - If `self._txt` is falsy (line 699):
    - `self._txt = self.text_plain()` (line 700)
  - Returns `clean_text(self._txt) if self._txt else ''` (line 701)
  - `self.text_plain()` (`text/text.py:738`)
    - **Path 1: plain text file** (line 743):
      - `self.path_txt` -> triggers `__getattr__('path_txt')` (line 199) -> `self.get_path('path_txt')` (`text/text.py:295`)
        - **Simplified path resolution** (lines 296-302):
          - Strips `path_` prefix: `part = 'txt'`
          - `getattr(self.corpus, 'path_txt', None)` -- gets corpus-level txt directory
            - This hits `BaseCorpus.__getattr__('path_txt')` -> `self.get_path('path_txt')` (`corpus/corpus.py:195`)
              - Checks `self.__dict__` for `path_txt` or `_path_txt` (line 198)
              - Falls back to class attribute (lines 200-201)
              - If relative path: joins with `self.path` (corpus root) (lines 203-208)
              - Returns absolute path to txt directory (e.g., `~/lltk_data/corpora/estc/txt`)
          - `ext = getattr(self.corpus, 'ext_txt', None)` -- gets extension (`.txt` or `.txt.gz`)
          - Returns `os.path.join(corpus_txt_dir, self.id) + ext` (line 301)
      - If `self.path_txt` exists and not `force_xml`: `_open_file(self.path_txt)` (line 744) -- handles `.gz` transparently -> reads content
    - **Path 2: XML conversion** (line 747):
      - If `self.path_xml` exists: `self.XML2TXT.__func__(self.path_xml)` -- class-level XML-to-text converter
    - Returns `''` if neither path exists (line 748)
  - `clean_text()` (`text/utils.py:666`): runs `ftfy.fix_text()`, replaces HTML entities and special Unicode chars

**Caching:**
- `self._txt`: per-text instance cache. Set on first access. Persists for object lifetime.
- No disk cache for the text content itself (unlike metadata which has parquet cache).

**Improvements over old code:**
- Path resolution is now a single method `get_path()` (text/text.py:295-302) -- 3 lines instead of the old `get_path` -> `get_path_old` -> `get_path_new` chain. The old code had 5 levels of indirection.
- `text_plain()` is simplified to 10 lines (738-748), down from a nested inner function with try/except TypeError fallback.
- `t.txt` property is 3 lines (698-701), directly calling `text_plain()`.

**Remaining issues:**
- `text_plain()` checks both `self.path_txt` and `self.path_xml` -- each triggers the `__getattr__` -> `get_path` -> `corpus.__getattr__` chain. If neither file exists, two full path resolutions happen for nothing.
- The `_open_file` helper at `text/text.py:47` handles `.gz` files, but the path resolution doesn't know whether the file is gzipped. Gzip support depends on correct manifest `ext_txt` configuration (e.g., `.txt.gz`).

---

## 8. `t.freqs()` -- Frequency Data Access

Entry point: `t.freqs()` method on `BaseText`.

- `t.freqs(lower=True, modernize_spelling=None)` (`text/text.py:759`)
  - If `not hasattr(self, '_freqs') or not self._freqs` (line 760):
    - `self.path_freqs` -- resolves via same simplified path chain:
      - `__getattr__('path_freqs')` -> `get_path('freqs')` -> `corpus.path_freqs / self.id + ext_freqs`
      - Typical result: `~/lltk_data/corpora/estc/freqs/T012345.json`
    - If freqs file doesn't exist: `self.save_freqs_json()` (`text/text.py:753`)
      - Requires `self.path_txt` to exist -- reads full text, tokenizes, counts, saves JSON
      - If no text file either: returns `{}` (lines 754-755)
    - `orjson.loads(f.read())` -- fast JSON parse (line 765)
    - `Counter(...)` -- wraps in Counter
    - Caches in `self._freqs` (line 766)
  - `filter_freqs(self._freqs, modernize=modernize_spelling, lower=lower)` (`text/utils.py:820`)
    - Creates new `Counter`, iterating all freq entries (lines 822-828)
    - If `lower`: lowercases each word
    - If `modernize`: applies MorphAdorner spelling modernization dict (358K entries)
    - Returns new Counter (does NOT update cache -- the cache stores the raw freqs)

**Caching:**
- `self._freqs`: per-text instance cache. Stores raw (unfiltered) Counter.
- The filtered result is NOT cached -- `filter_freqs()` creates a new Counter on every call.

**Remaining issues:**
- `filter_freqs` is called on every `t.freqs()` call, even with defaults (`lower=True`, `modernize=None`). It creates a new Counter by iterating all words, lowercasing each one. For a text with 50K unique words, this is ~50K string operations per call. The raw cache stores pre-lowercased data, so lowercasing often has no effect.
- `save_freqs_json()` at line 762 will silently try to generate freqs from text if the freqs file doesn't exist. For corpora without text files, this silently returns `{}` after checking two non-existent paths.
- The `not hasattr(self, '_freqs') or not self._freqs` check at line 760 is redundant -- `_freqs` is always set in `__init__` to `None`. The `not hasattr` branch can never be True.

---

## Summary of Cross-Cutting Concerns

### Unified corpus cache
`CORPUS_CACHE` (in `corpus/corpus.py:1343`) is now the single source of truth. Both `load()` and `Corpus()` read from and write to it. The old dual-cache problem (`CORPUSOBJD` + `CORPUS_CACHE` holding different objects for the same entity) is eliminated.

### Manifest caching fixed
`load_manifest()` now checks `if MANIFEST and not force` (line 634) and reuses the global dict. The old code called `force=True` on every `load_corpus()`, re-reading all manifest files from disk each time.

### Constants extracted to `lltk/tools/constants.py`
`_parse_year`, `TEXT_META_DEFAULT`, `YEARKEYS`, `MANIFEST_DEFAULTS`, `CORPUS_SOURCE_RANKS`, etc. all live in `lltk/tools/constants.py`. This eliminates the accidental coupling where `clean_meta()` importing `_parse_year` from `metadb.py` triggered ClickHouse initialization.

### Logging via logmap
All logging uses `logmap` (`lltk/tools/logs.py:1-3`): `log = logmap('lltk')`. The `@logmap.fn` decorator on `load_corpus()` provides automatic entry/exit logging. Progress bars use `logmap` context managers (e.g., `with logmap(desc) as lm:` in `iter_init`).

### Simplified `get()` as single hydration gateway
All explicit `BaseText` properties (`title`, `author`, `genre`, etc.) now delegate to `self.get()` (lines 479-499), which calls `_hydrate_meta()` as needed. The old code had some properties calling `_hydrate_meta()` directly and then reading `_meta`, while others went through `get()` -> `_gcache`. The `_gcache` layer is removed entirely.

### Simplified path resolution
`BaseText.get_path()` is now 7 lines (295-302): look up corpus dir, append text ID and extension. The old code had `get_path` -> `get_path_old` -> `get_path_new` with corpus `__getattr__` chains at each level.

### Hydration is still two-phase
Texts are created as bare shells, then metadata is loaded lazily on first attribute access. This means:
- The first attribute access on any text incurs the full hydration cost (CH query or CSV load)
- But `iter_init()` pre-hydrates texts (sets `_meta_hydrated = True`) when iterating through a corpus
- Texts created via `C.t` or `Text('_estc/T012345')` are NOT pre-hydrated

### `__getattr__` is the universal dispatcher
Both `BaseText.__getattr__` and `BaseCorpus.__getattr__` intercept attribute access for `path_*` resolution and (for texts) metadata lookup. This makes debugging difficult -- stack traces show `__getattr__` calls that are really metadata lookups or path resolutions.

### Silent exception swallowing
Multiple try/except blocks catch specific exception types and silently fall through:
- `_corpus_meta_row()`: catches `(ImportError, AttributeError, RuntimeError, OSError)` from CH lookup
- `C.t`: catches `(ImportError, AttributeError, RuntimeError, OSError)` from CH, `(FileNotFoundError, OSError, KeyError, ValueError)` from CSV
- `load_metadata()`: catches `(OSError, IOError, ValueError, KeyError)` from parquet loading

This is improved from the old bare `except` blocks but still makes it hard to diagnose performance issues or configuration errors without enabling debug logging.
