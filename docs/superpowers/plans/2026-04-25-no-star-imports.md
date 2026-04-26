# No Star Imports Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove all 67 `from lltk.imports import *` statements and ~10 internal cross-module star imports, replacing each with explicit imports.

**Architecture:** A Python analysis script scans each file's AST to determine which of the 456 star-imported names are actually used, categorizes them (stdlib / third-party / lltk-specific), and generates Option B replacement imports. Phase 1 breaks the circular dependency chain in `tools/`, Phase 2 fixes core `text/` and `corpus/` modules, Phase 3 dispatches parallel agents for the ~50 leaf files.

**Tech Stack:** Python `ast` module for analysis; `isort`-style grouping for output.

**Import style (Option B):**
```python
import os                                    # stdlib: import directly
import pandas as pd                          # third-party: import directly
from lltk.imports import PATH_CORPUS, log    # lltk-specific: explicit from imports
```

**Key constraint — circular imports:** `imports.py` lines 38-39 star-import from `tools/logs.py` and `tools/tools.py`, which themselves star-import from `imports.py`. When these files load, they only see imports.py lines 1-37 (constants, `os`, `sys`, etc.). Replacing their star imports with explicit `from lltk.imports import X, Y` still works because Python finds partially-loaded modules in `sys.modules`. The explicit names just need to be defined before line 38 in imports.py.

---

## File Map

**Phase 1 — Circular chain (modify):**
- `lltk/tools/logs.py` — only needs `os`, `sys` from stdlib
- `lltk/tools/tools.py` — needs stdlib + ~8 constants from imports.py (defined before line 38)
- `lltk/tools/baseobj.py` — needs `os` + explicit imports from tools
- `lltk/tools/freqs.py` — needs stdlib + lltk-specific
- `lltk/tools/stats.py` — needs `pd` + lltk-specific
- `lltk/imports.py` — replace lines 38-39 and 291 with explicit imports

**Phase 2 — Core modules (modify):**
- `lltk/text/utils.py` — star import + many lltk names
- `lltk/text/text.py` — star import + `from .utils import *`
- `lltk/text/textlist.py` — star import + `from .utils import *` + `from .text import *`
- `lltk/corpus/utils.py` — star import
- `lltk/corpus/corpus.py` — star import + `from lltk.text import *` + `from .utils import *`
- `lltk/corpus/tcp/tcp.py` — star import (parent for TCP corpora)

**Phase 3 — Leaf files (modify, parallelizable):**
- 43 corpus files + 8 model files (see batch assignments below)

**Analysis tooling (create):**
- `scripts/fix_star_imports.py` — analysis + transformation script

---

## Task 1: Write the analysis/transformation script

**Files:**
- Create: `scripts/fix_star_imports.py`

This script is the engine for the entire refactoring. It analyzes files and generates or applies replacement imports.

- [ ] **Step 1: Write the script**

```python
#!/usr/bin/env python3
"""Analyze and fix star imports from lltk.imports.

Usage:
    python scripts/fix_star_imports.py analyze lltk/corpus/chadwyck/chadwyck.py
    python scripts/fix_star_imports.py analyze --all
    python scripts/fix_star_imports.py apply lltk/corpus/chadwyck/chadwyck.py
    python scripts/fix_star_imports.py apply --batch corpus_a
"""
import ast
import sys
import os
import json
import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
LLTK = REPO / "lltk"

# Names that are stdlib modules importable directly (import X)
STDLIB_MODULES = {
    'os', 'sys', 'json', 'random', 'gzip', 'time', 'inspect', 'pickle',
    're', 'configparser', 'urllib', 'tempfile', 'shutil', 'tarfile',
    'logging', 'math', 'warnings', 'csv', 'codecs', 'statistics',
}

# Third-party modules (import X or import X as Y)
THIRD_PARTY = {
    'np': 'import numpy as np',
    'pd': 'import pandas as pd',
    'nx': 'import networkx as nx',
    'requests': 'import requests',
    'orjson': 'import orjson',
}

# from X import Y — grouped by source
FROM_IMPORTS = {
    'xopen': ('from xopen import xopen', {'xopen'}),
    'b64decode': ('from base64 import b64decode, b64encode', {'b64decode', 'b64encode'}),
    'b64encode': ('from base64 import b64decode, b64encode', {'b64decode', 'b64encode'}),
    'pprint': ('from pprint import pprint', {'pprint'}),
    'partial': ('from functools import partial', {'partial'}),
    'datetime': ('from datetime import datetime', {'datetime'}),
    'expanduser': ('from os.path import expanduser', {'expanduser'}),
    'Namespace': ('from argparse import Namespace', {'Namespace'}),
    'HTTPError': ('from urllib.error import HTTPError', {'HTTPError'}),
    'ZipFile': ('from zipfile import ZipFile', {'ZipFile'}),
    'Path': ('from pathlib import Path', {'Path'}),
    'quote_plus': ('from urllib.parse import quote_plus', {'quote_plus'}),
    'StringIO': ('from io import StringIO', {'StringIO'}),
}

# collections imports
COLLECTIONS = {
    'defaultdict', 'Counter', 'OrderedDict', 'UserList',
}

# collections.abc imports
COLLECTIONS_ABC = {
    'MutableMapping',
}

# typing imports
TYPING = {
    'Callable', 'Dict', 'Iterable', 'Iterator', 'List', 'Mapping',
    'Optional', 'Union', 'cast', 'Tuple',
}

# All stdlib/third-party names — everything else is lltk-specific
ALL_EXTERNAL = (
    STDLIB_MODULES
    | set(THIRD_PARTY.keys())
    | set(FROM_IMPORTS.keys())
    | COLLECTIONS
    | COLLECTIONS_ABC
    | TYPING
    | {'mp', 'mp_cpu_count', 'multiprocessing', 'stats'}  # edge cases
)


def get_used_names(filepath):
    """Parse a Python file and return all names referenced (loaded) at module scope and in functions."""
    with open(filepath) as f:
        source = f.read()
    tree = ast.parse(source)
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
            names.add(node.id)
        elif isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
            names.add(node.value.id)
    return names


def get_defined_names(filepath):
    """Get names defined (assigned/imported explicitly) in a file, excluding the star import line."""
    with open(filepath) as f:
        source = f.read()
    tree = ast.parse(source)
    defined = set()
    for node in ast.iter_child_nodes(tree):
        if isinstance(node, ast.FunctionDef) or isinstance(node, ast.AsyncFunctionDef):
            defined.add(node.name)
        elif isinstance(node, ast.ClassDef):
            defined.add(node.name)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    defined.add(target.id)
                elif isinstance(target, ast.Tuple):
                    for elt in target.elts:
                        if isinstance(elt, ast.Name):
                            defined.add(elt.id)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                defined.add(alias.asname or alias.name)
        elif isinstance(node, ast.ImportFrom):
            if any(alias.name == '*' for alias in node.names):
                continue  # skip star imports — that's what we're replacing
            for alias in node.names:
                defined.add(alias.asname or alias.name)
    return defined


def get_star_import_lines(filepath):
    """Return list of (line_number, module) for star imports."""
    results = []
    with open(filepath) as f:
        for i, line in enumerate(f, 1):
            m = re.match(r'from\s+([\w.]+)\s+import\s+\*', line.strip())
            if m:
                results.append((i, m.group(1)))
    return results


def classify_name(name):
    """Classify a name into its import category."""
    if name in STDLIB_MODULES:
        return 'stdlib_module'
    if name in THIRD_PARTY:
        return 'third_party'
    if name in FROM_IMPORTS:
        return 'from_import'
    if name in COLLECTIONS:
        return 'collections'
    if name in COLLECTIONS_ABC:
        return 'collections_abc'
    if name in TYPING:
        return 'typing'
    return 'lltk_specific'


def generate_imports(needed_names):
    """Generate import lines grouped by category."""
    lines = []

    # 1. stdlib modules
    stdlib = sorted(n for n in needed_names if n in STDLIB_MODULES)
    for mod in stdlib:
        lines.append(f'import {mod}')

    # 2. third-party
    tp = sorted(n for n in needed_names if n in THIRD_PARTY)
    for name in tp:
        lines.append(THIRD_PARTY[name])

    # 3. from imports (stdlib)
    seen_from = set()
    from_names = sorted(n for n in needed_names if n in FROM_IMPORTS)
    for name in from_names:
        stmt, group = FROM_IMPORTS[name]
        if stmt not in seen_from:
            actual = sorted(group & needed_names)
            mod = stmt.split('import')[0].strip()
            lines.append(f"{mod}import {', '.join(actual)}")
            seen_from.add(stmt)

    # 4. collections
    coll = sorted(n for n in needed_names if n in COLLECTIONS)
    if coll:
        lines.append(f"from collections import {', '.join(coll)}")

    # 5. collections.abc
    abc = sorted(n for n in needed_names if n in COLLECTIONS_ABC)
    if abc:
        lines.append(f"from collections.abc import {', '.join(abc)}")

    # 6. typing
    typ = sorted(n for n in needed_names if n in TYPING)
    if typ:
        lines.append(f"from typing import {', '.join(typ)}")

    # 7. lltk-specific
    lltk = sorted(n for n in needed_names if classify_name(n) == 'lltk_specific')
    if lltk:
        if len(lltk) <= 4:
            lines.append(f"from lltk.imports import {', '.join(lltk)}")
        else:
            items = ',\n    '.join(lltk)
            lines.append(f"from lltk.imports import (\n    {items},\n)")

    return lines


def analyze_file(filepath):
    """Analyze a single file and return its import replacement plan."""
    used = get_used_names(filepath)
    defined = get_defined_names(filepath)
    star_lines = get_star_import_lines(filepath)

    # Names from star import = used but not locally defined
    from_star = used - defined

    # Generate replacement
    needed = from_star  # the script doesn't filter against the 456-name list;
                        # the agent/human reviews the output
    import_lines = generate_imports(needed)

    return {
        'file': str(filepath),
        'star_imports': [(ln, mod) for ln, mod in star_lines],
        'from_star': sorted(from_star),
        'stdlib': sorted(n for n in from_star if n in STDLIB_MODULES),
        'third_party': sorted(n for n in from_star if n in THIRD_PARTY),
        'lltk_specific': sorted(n for n in from_star if classify_name(n) == 'lltk_specific'),
        'import_lines': import_lines,
    }


def apply_file(filepath, dry_run=False):
    """Replace star imports in a file with explicit imports."""
    analysis = analyze_file(filepath)
    if not analysis['star_imports']:
        print(f"  SKIP {filepath}: no star imports found")
        return False

    with open(filepath) as f:
        lines = f.readlines()

    # Find the star import lines to replace
    star_line_nums = {ln for ln, _ in analysis['star_imports']
                      if 'lltk.imports' in _}
    if not star_line_nums:
        print(f"  SKIP {filepath}: no lltk.imports star import")
        return False

    # Build new file content
    new_lines = []
    replaced = False
    for i, line in enumerate(lines, 1):
        if i in star_line_nums and not replaced:
            # Insert replacement imports at first star import location
            for imp_line in analysis['import_lines']:
                new_lines.append(imp_line + '\n')
            replaced = True
        elif i in star_line_nums:
            # Skip additional star import lines (already replaced)
            continue
        else:
            new_lines.append(line)

    if dry_run:
        print(f"\n--- {filepath} ---")
        for imp in analysis['import_lines']:
            print(f"  {imp}")
        return True

    with open(filepath, 'w') as f:
        f.writelines(new_lines)
    print(f"  FIXED {filepath}")
    return True


# Batch definitions for parallel agents
BATCHES = {
    'phase1': [
        'lltk/tools/logs.py',
        'lltk/tools/tools.py',
        'lltk/tools/baseobj.py',
        'lltk/tools/freqs.py',
        'lltk/tools/stats.py',
    ],
    'phase2': [
        'lltk/text/utils.py',
        'lltk/text/text.py',
        'lltk/text/textlist.py',
        'lltk/corpus/utils.py',
        'lltk/corpus/corpus.py',
        'lltk/corpus/tcp/tcp.py',
    ],
    'corpus_a': [
        'lltk/corpus/chadwyck/chadwyck.py',
        'lltk/corpus/chadwyck_drama/chadwyck_drama.py',
        'lltk/corpus/earlyprint/earlyprint.py',
        'lltk/corpus/eebo_tcp/eebo_tcp.py',
        'lltk/corpus/ecco_tcp/ecco_tcp.py',
        'lltk/corpus/evans_tcp/evans_tcp.py',
        'lltk/corpus/ecco/ecco.py',
        'lltk/corpus/end/end.py',
        'lltk/corpus/estc/estc.py',
        'lltk/corpus/fiction_biblio/fiction_biblio.py',
    ],
    'corpus_b': [
        'lltk/corpus/hathi/hathi.py',
        'lltk/corpus/hathi_englit/hathi_englit.py',
        'lltk/corpus/internet_archive/internet_archive.py',
        'lltk/corpus/blbooks/blbooks.py',
        'lltk/corpus/bpo/bpo.py',
        'lltk/corpus/oldbailey/oldbailey.py',
        'lltk/corpus/gale_amfic/gale_amfic.py',
        'lltk/corpus/litlab/litlab.py',
        'lltk/corpus/chicago/chicago.py',
        'lltk/corpus/markmark/markmark.py',
    ],
    'corpus_c': [
        'lltk/corpus/artfl/artfl.py',
        'lltk/corpus/gallica_literary_fictions/gallica_literary_fictions.py',
        'lltk/corpus/french_pd_books/french_pd_books.py',
        'lltk/corpus/paige/paige.py',
        'lltk/corpus/dta/dta.py',
        'lltk/corpus/german_fiction/german_fiction.py',
        'lltk/corpus/german_pd/german_pd.py',
        'lltk/corpus/de_corp/de_corp.py',
        'lltk/corpus/impact_es/impact_es.py',
        'lltk/corpus/spanish_pd_books/spanish_pd_books.py',
        'lltk/corpus/coha/coha.py',
    ],
    'corpus_d': [
        'lltk/corpus/clmet/clmet.py',
        'lltk/corpus/dialogues/dialogues.py',
        'lltk/corpus/epistolary/epistolary.py',
        'lltk/corpus/gildedage/gildedage.py',
        'lltk/corpus/long_arc_prestige/long_arc_prestige.py',
        'lltk/corpus/ravengarside/ravengarside.py',
        'lltk/corpus/semantic_cohort/semantic_cohort.py',
        'lltk/corpus/sotu/sotu.py',
        'lltk/corpus/spectator/spectator.py',
        'lltk/corpus/tedjdh/tedjdh.py',
        'lltk/corpus/txtlab/txtlab.py',
        'lltk/corpus/canon_fiction/canon_fiction.py',
        'lltk/corpus/test_fixture/test_fixture.py',
        'lltk/corpus/test_fixture_linked/test_fixture_linked.py',
        'lltk/corpus/default/new_corpus.py',
    ],
    'model': [
        'lltk/model/model.py',
        'lltk/model/booknlp.py',
        'lltk/model/characters.py',
        'lltk/model/charnet.py',
        'lltk/model/classifier.py',
        'lltk/model/networks.py',
        'lltk/model/ner.py',
        'lltk/model/preprocess.py',
        'lltk/model/word2vec.py',
    ],
}


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['analyze', 'apply'])
    parser.add_argument('target', nargs='?', help='file path or --all or --batch NAME')
    parser.add_argument('--all', action='store_true')
    parser.add_argument('--batch', type=str)
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    os.chdir(REPO)

    if args.all:
        files = []
        for batch in BATCHES.values():
            files.extend(batch)
    elif args.batch:
        files = BATCHES.get(args.batch, [])
        if not files:
            print(f"Unknown batch: {args.batch}")
            sys.exit(1)
    elif args.target:
        files = [args.target]
    else:
        parser.print_help()
        sys.exit(1)

    for f in files:
        if args.action == 'analyze':
            result = analyze_file(f)
            print(f"\n=== {f} ===")
            print(f"  Star imports: {result['star_imports']}")
            print(f"  stdlib: {result['stdlib']}")
            print(f"  third_party: {result['third_party']}")
            print(f"  lltk_specific: {result['lltk_specific']}")
            print(f"  Replacement imports:")
            for line in result['import_lines']:
                print(f"    {line}")
        else:
            apply_file(f, dry_run=args.dry_run)
```

- [ ] **Step 2: Test the script on a few files**

```bash
cd /Users/rj416/github/lltk
python scripts/fix_star_imports.py analyze lltk/corpus/chadwyck/chadwyck.py
python scripts/fix_star_imports.py analyze lltk/tools/logs.py
python scripts/fix_star_imports.py analyze lltk/model/model.py
```

Review output to verify correct classification. Adjust the script's name lists if any names are miscategorized.

- [ ] **Step 3: Commit**

```bash
git add scripts/fix_star_imports.py
git commit -m "add analysis script for star import removal"
```

---

## Task 2: Fix tools/logs.py (break circular chain)

**Files:**
- Modify: `lltk/tools/logs.py`

`logs.py` currently does `from lltk.imports import *` but only uses `os` and `sys` from it (everything else is from `loguru`). The star import is the first link in the circular chain.

- [ ] **Step 1: Analyze the file**

```bash
python scripts/fix_star_imports.py analyze lltk/tools/logs.py
```

- [ ] **Step 2: Replace the star import**

Replace line 1 (`from lltk.imports import *`) with:
```python
import os
import sys
```

These are the only two names from `lltk.imports` that `logs.py` uses. `Logger.start_file()` already does a late import: `from lltk.tools.tools import ensure_dir_exists, backup_fn, rmfn`.

- [ ] **Step 3: Verify import works**

```bash
.venv/bin/python -c "from lltk.tools.logs import Logger, Log; print('OK')"
```

---

## Task 3: Fix tools/tools.py (break circular chain)

**Files:**
- Modify: `lltk/tools/tools.py`

`tools.py` is loaded during `imports.py` line 39. At that point, only imports.py lines 1-37 are executed. The star import gives tools.py: `os`, `sys`, `warnings`, `shutil`, `HOME`, `ROOT`, `PATH_DEFAULT_LLTK_HOME`, `PATH_DEFAULT_CONF`, `META_KEY_SEP`, `mp`, `mp_cpu_count`, `DEFAULT_NUM_PROC`, and other early constants.

- [ ] **Step 1: Analyze the file**

```bash
python scripts/fix_star_imports.py analyze lltk/tools/tools.py
```

- [ ] **Step 2: Replace the star import**

Replace line 1 (`from lltk.imports import *`) with direct stdlib imports + explicit lltk.imports for the early constants:

```python
import os
import sys
import re
import shutil
import warnings
import time
import csv
import multiprocessing as mp
import numpy as np
from collections import UserList, defaultdict
from collections.abc import MutableMapping
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from io import StringIO
from xopen import xopen
import orjson
from lltk.imports import (
    HOME, ROOT, PATH_DEFAULT_LLTK_HOME, PATH_DEFAULT_CONF,
    META_KEY_SEP, DEFAULT_NUM_PROC, mp_cpu_count,
)
```

**Important:** The `from lltk.imports import ...` works because all these names are defined in imports.py before line 38 (where the circular import triggers). Python finds the partially-loaded `lltk.imports` in `sys.modules` and resolves the names.

Also remove duplicate imports already in the file (line 4 `import numpy as np`, line 281 `import numpy as np`, line 730 `from io import StringIO`, line 731 `import sys`).

Late imports inside functions (e.g., `from lltk.imports import log` at line 338, `from lltk.imports import PATH_CORPUS` at line 1703) should stay — those access names defined after the circular import resolves.

- [ ] **Step 3: Verify import works**

```bash
.venv/bin/python -c "from lltk.tools.tools import config, pmap, read_json; print('OK')"
```

---

## Task 4: Fix tools/baseobj.py, tools/freqs.py, tools/stats.py

**Files:**
- Modify: `lltk/tools/baseobj.py`
- Modify: `lltk/tools/freqs.py`
- Modify: `lltk/tools/stats.py`

- [ ] **Step 1: Analyze all three**

```bash
python scripts/fix_star_imports.py analyze lltk/tools/baseobj.py
python scripts/fix_star_imports.py analyze lltk/tools/freqs.py
python scripts/fix_star_imports.py analyze lltk/tools/stats.py
```

- [ ] **Step 2: Fix baseobj.py**

`baseobj.py` is tiny (31 lines). Replace lines 1-3:
```python
from lltk.imports import *
from lltk.tools import ensure_dir_exists,get_tqdm,just_metadata
from lltk.tools.logs import *
```
with:
```python
import os
from lltk.tools.tools import ensure_dir_exists, get_tqdm, just_metadata
from lltk.tools.logs import Log, log_hidden, log_shown
```

- [ ] **Step 3: Fix freqs.py**

Replace `from lltk.imports import *` with the explicit imports the analysis script identifies. Likely just a handful of names.

- [ ] **Step 4: Fix stats.py**

Replace `from lltk.imports import *` with explicit imports. `stats.py` uses `pd` (via `smf`) and a few functions.

- [ ] **Step 5: Verify all three**

```bash
.venv/bin/python -c "from lltk.tools.baseobj import BaseObject; print('OK')"
.venv/bin/python -c "from lltk.tools.freqs import measure_fields; print('OK')"
.venv/bin/python -c "from lltk.tools.stats import regressions; print('OK')"
```

---

## Task 5: Fix imports.py internal star imports

**Files:**
- Modify: `lltk/imports.py`

- [ ] **Step 1: Replace lines 38-39**

Replace:
```python
from lltk.tools.logs import *
from lltk.tools.tools import *
```
with explicit imports of just the names `imports.py` actually uses between lines 40-290:
```python
from lltk.tools.logs import Log, log_hidden, log_shown, hide_log, show_log, Logger, LOGGER
from lltk.tools.tools import (
    config, remove_duplicates, in_jupyter, pmap, pmap_iter,
    safebool, safeget, safejson, read_json, read_df, save_df,
    readgen, readgen_csv, read_ld, writegen, writegengen, write_ld,
    get_tqdm, ensure_dir_exists, rmfn, backup_fn,
    tokenize, tokenize_fast, tokenize_agnostic, tokenize_agnostic0,
    is_iterable, is_hashable, is_dictish,
    SetList, OrderedSetDict, Bunch, Capturing,
    snake2camel, to_camel_case, ensure_snake, zeropunc, noPunc,
    fillna, human_format, ppath, rpath, mask_home_dir,
    yank, product, zfy, linreg, bigrams, ngram,
    slice, unzip, download, extract, symlink,
    check_make_dir, check_make_dirs, get_path_abs,
    variant2standard, standard2variant, modernize_spelling_in_txt,
    gleanPunc2, get_wordlist, get_spelling_modernizer, get_word2pos,
    get_ocr_corrections, get_num_lines,
    to_bs64, serialize, deserialize, compressed,
    llmap, crunch, escape_linebreaks, datatype, write2,
    read, header, tsv2ld, ld2dl, ld2dd, ld2dld,
    worddb, printm, get_ideal_cpu_count, gethtml,
    just_metadata, just_meta_no_id, no_id, to_numeric_dict,
    camel_case_split, get_backup_fn, iter_move, iter_filename,
    download_wget, copyfileobj, get_url_or_path,
    which, Bunch, load_english, now, ENGLISH, MDETOK,
    MaybeListDict, get_passkey, get_pkey,
    get_user_info, get_user_email, check_move_file, check_move_link_file,
    cloud_list, get_config_file_location, passages, index,
    register_cell_magic,
)
```

**Note:** This list is long. An alternative is to use `__all__` in tools.py and keep `from lltk.tools.tools import *` — but since we're eliminating star imports, enumerate explicitly. The agent should verify this list by running the analysis script on imports.py to identify which tools.py names are actually re-exported/used.

A more practical approach: keep a single star import from tools.tools ONLY inside imports.py (since imports.py IS the re-export hub), but add `__all__` to tools.py to make it well-defined. This is a judgment call — discuss with user.

- [ ] **Step 2: Delete line 291**

Remove:
```python
from lltk.tools import *
```
This is redundant — lines 38-39 already import what's needed, and the public API goes through lines 300-302.

- [ ] **Step 3: Verify**

```bash
.venv/bin/python -c "import lltk; print(lltk.Text, lltk.Corpus)"
```

---

## Task 6: Run tests — Phase 1 checkpoint

- [ ] **Step 1: Run full test suite**

```bash
cd /Users/rj416/github/lltk
.venv/bin/python -m pytest tests/ -x -v 2>&1 | tail -30
```

- [ ] **Step 2: Fix any failures**

If tests fail, check for missing imports. The most likely issue is a name that was available via the star import but not included in the explicit replacement.

- [ ] **Step 3: Commit Phase 1**

```bash
git add lltk/tools/logs.py lltk/tools/tools.py lltk/tools/baseobj.py \
       lltk/tools/freqs.py lltk/tools/stats.py lltk/imports.py
git commit -m "break circular star-import chain in tools/ and imports.py"
```

---

## Task 7: Fix text/ modules

**Files:**
- Modify: `lltk/text/utils.py`
- Modify: `lltk/text/text.py`
- Modify: `lltk/text/textlist.py`

- [ ] **Step 1: Analyze all three**

```bash
python scripts/fix_star_imports.py analyze lltk/text/utils.py
python scripts/fix_star_imports.py analyze lltk/text/text.py
python scripts/fix_star_imports.py analyze lltk/text/textlist.py
```

- [ ] **Step 2: Fix text/utils.py**

Replace `from lltk.imports import *` with explicit imports. Key names used:
- stdlib: `os`, `re`, `json`, `random`
- third-party: `pd`, `np`
- lltk: `log`, `safebool`, `readgen`, `read_df`, `read_json`, `get_wordlist`, `get_spelling_modernizer`, `zeropunc`, `ensure_snake`, `is_hashable`, `OrderedSetDict`, `SetList`, `Bunch`, `ANNO_EXTS`, `META_KEY_SEP`, `IDSEP_START`, `IDSEP`, `MINIMETAD`, `BROKENSTATE`
- collections: `Counter`, `defaultdict`, `MutableMapping`

- [ ] **Step 3: Fix text/text.py**

Replace `from lltk.imports import *` with explicit imports. Also replace `from .utils import *` with explicit imports of the names from utils that text.py actually uses (check the analysis output).

Key names from lltk.imports: `os`, `json`, `pd`, `np`, `log`, `safebool`, constants (`DIR_SECTION_NAME`, `COL_ID`, `META_KEY_SEP`, etc.)
Key names from .utils: `tokenize`, `xml2txt_default`, `filter_freqs`, `save_freqs_json`, `clean_text`, `remove_bad_tags`, `get_wordlist`, `noPunc`, `zeropunc`, `ensure_snake`, `SetList`, `is_text_obj`, `is_addr_str`, `is_corpus_obj`, `to_corpus_and_id`, `get_addr_str`, `get_idx`, `merge_dict`, `just_meta_no_id`, `get_imsg`, etc.

- [ ] **Step 4: Fix text/textlist.py**

Replace `from lltk.imports import *`, `from .utils import *`, and `from .text import *` with explicit imports.

Key names: `random`, `nx`, `log`, `get_tqdm`, `llmap`, `is_iterable`, `to_corpus_and_id`, `IDSEP_START`, `IDSEP`, `TMP_CORPUS_ID`, `CORPUS_SOURCE_RANKS`, `BaseObject`, `UserList`, `Text`

- [ ] **Step 5: Verify**

```bash
.venv/bin/python -c "from lltk.text import BaseText, TextList; print('OK')"
```

---

## Task 8: Fix corpus/utils.py, corpus/corpus.py, corpus/tcp/tcp.py

**Files:**
- Modify: `lltk/corpus/utils.py`
- Modify: `lltk/corpus/corpus.py`
- Modify: `lltk/corpus/tcp/tcp.py`

- [ ] **Step 1: Analyze all three**

```bash
python scripts/fix_star_imports.py analyze lltk/corpus/utils.py
python scripts/fix_star_imports.py analyze lltk/corpus/corpus.py
python scripts/fix_star_imports.py analyze lltk/corpus/tcp/tcp.py
```

- [ ] **Step 2: Fix corpus/utils.py**

Replace `from lltk.imports import *` with explicit imports. Key names used: `os`, `shutil`, `pd`, `log`, `get_tqdm`, `read_df`, `pmap`, `rpath`, `zeropunc`, `in_jupyter`, `printm`, `Bunch`, `MANIFEST`, `MANIFEST_DEFAULTS`, `PATH_CORPUS`, `PATH_TO_CORPUS_CODE`, `PATH_MANIFEST`, `PATH_MANIFESTS`, `CORPUS_FUNCS`, `EMPTY_GROUP`, etc.

- [ ] **Step 3: Fix corpus/corpus.py**

Replace three star imports:
1. `from lltk.imports import *` → explicit (see analysis)
2. `from lltk.text import *` → `from lltk.text import BaseText, TextList, NullText` (and any other names actually used)
3. `from .utils import *` → `from .utils import load_manifest, load_corpus, load_metadata, fix_meta, get_inducted_corpus_ids, ...` (list names actually used)

This file has the most dependencies (~25 lltk constants, ~20 functions, ~5 classes). Use the analysis script output.

- [ ] **Step 4: Fix corpus/tcp/tcp.py**

Replace `from lltk.imports import *` with explicit imports. TCP is the parent class for eebo_tcp, ecco_tcp, evans_tcp.

- [ ] **Step 5: Verify**

```bash
.venv/bin/python -c "from lltk.corpus import BaseCorpus; from lltk.corpus.tcp import TCP; print('OK')"
```

---

## Task 9: Run tests — Phase 2 checkpoint

- [ ] **Step 1: Run full test suite**

```bash
.venv/bin/python -m pytest tests/ -x -v 2>&1 | tail -30
```

- [ ] **Step 2: Fix any failures**

- [ ] **Step 3: Commit Phase 2**

```bash
git add lltk/text/utils.py lltk/text/text.py lltk/text/textlist.py \
       lltk/corpus/utils.py lltk/corpus/corpus.py lltk/corpus/tcp/tcp.py
git commit -m "replace star imports in core text/ and corpus/ modules"
```

---

## Task 10: Fix leaf corpus files — Batch A

**Files:** `chadwyck.py`, `chadwyck_drama.py`, `earlyprint.py`, `eebo_tcp.py`, `ecco_tcp.py`, `evans_tcp.py`, `ecco.py`, `end.py`, `estc.py`, `fiction_biblio.py`

**Can run in parallel with Tasks 11-14.**

- [ ] **Step 1: Run analysis**

```bash
python scripts/fix_star_imports.py analyze --batch corpus_a
```

- [ ] **Step 2: Apply replacements**

For each file in the batch: read the file, determine what names from `lltk.imports` it actually uses (cross-reference with analysis output), replace the star import with explicit imports following Option B style.

For files that also have Category C stars (`from lltk.corpus.tcp import *` in eebo_tcp, ecco_tcp, evans_tcp): replace with explicit imports of the TCP names they use (typically `TextTCP`, `TCP`).

For `epistolary.py` which has `from lltk.model.charnet import *`: replace with explicit imports.

- [ ] **Step 3: Verify batch imports**

```bash
.venv/bin/python -c "
from lltk.corpus.chadwyck import ChadwyckCorpus
from lltk.corpus.earlyprint import EarlyPrintCorpus
from lltk.corpus.eebo_tcp import EEBO_TCP_Corpus
from lltk.corpus.estc import ESTCCorpus
print('Batch A OK')
"
```

---

## Task 11: Fix leaf corpus files — Batch B

**Files:** `hathi.py`, `hathi_englit.py`, `internet_archive.py`, `blbooks.py`, `bpo.py`, `oldbailey.py`, `gale_amfic.py`, `litlab.py`, `chicago.py`, `markmark.py`

**Can run in parallel with Tasks 10, 12-14.**

- [ ] **Step 1-3:** Same pattern as Task 10. Run analysis, apply replacements, verify.

```bash
python scripts/fix_star_imports.py analyze --batch corpus_b
```

---

## Task 12: Fix leaf corpus files — Batch C

**Files:** `artfl.py`, `gallica_literary_fictions.py`, `french_pd_books.py`, `paige.py`, `dta.py`, `german_fiction.py`, `german_pd.py`, `de_corp.py`, `impact_es.py`, `spanish_pd_books.py`, `coha.py`

**Can run in parallel with Tasks 10-11, 13-14.**

- [ ] **Step 1-3:** Same pattern as Task 10.

```bash
python scripts/fix_star_imports.py analyze --batch corpus_c
```

---

## Task 13: Fix leaf corpus files — Batch D

**Files:** `clmet.py`, `dialogues.py`, `epistolary.py`, `gildedage.py`, `long_arc_prestige.py`, `ravengarside.py`, `semantic_cohort.py`, `sotu.py`, `spectator.py`, `tedjdh.py`, `txtlab.py`, `canon_fiction.py`, `test_fixture.py`, `test_fixture_linked.py`, `default/new_corpus.py`

**Can run in parallel with Tasks 10-12, 14.**

- [ ] **Step 1-3:** Same pattern as Task 10.

```bash
python scripts/fix_star_imports.py analyze --batch corpus_d
```

---

## Task 14: Fix model files — Batch E

**Files:** `model.py`, `booknlp.py`, `characters.py`, `charnet.py`, `classifier.py`, `networks.py`, `ner.py`, `preprocess.py`, `word2vec.py`

**Can run in parallel with Tasks 10-13.**

Also fix Category C star import: `characters.py` line 2 `from lltk.model.networks import *` → replace with explicit imports of the network names it uses.

- [ ] **Step 1: Run analysis**

```bash
python scripts/fix_star_imports.py analyze --batch model
```

- [ ] **Step 2: Apply replacements**

Note: `model/model.py` uses ZERO names from the star import — just delete the line entirely.

- [ ] **Step 3: Verify**

```bash
.venv/bin/python -c "
from lltk.model import BaseModel
from lltk.model.booknlp import BookNLP
print('Model batch OK')
"
```

---

## Task 15: Final verification

- [ ] **Step 1: Run full test suite**

```bash
.venv/bin/python -m pytest tests/ -x -v
```

- [ ] **Step 2: Verify public API**

```bash
.venv/bin/python -c "
import lltk
print('Text:', lltk.Text)
print('Corpus:', lltk.Corpus)
print('log:', lltk.log)
t = lltk.Text('_test_fixture/blake')
print('Text ID:', t.id)
"
```

- [ ] **Step 3: Verify no star imports remain (except Category B)**

```bash
grep -rn 'from .* import \*' lltk/ --include='*.py' | grep -v '__init__.py'
```

This should return zero results.

- [ ] **Step 4: Commit Phase 3 + final**

```bash
git add -A lltk/
git commit -m "replace star imports in all corpus and model leaf files"
```

- [ ] **Step 5: Verify test count unchanged**

```bash
.venv/bin/python -m pytest tests/ -v 2>&1 | tail -5
```

Should still show 206 tests passing.
