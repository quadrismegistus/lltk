# Remove Star Imports from lltk

## Problem
67 files do `from lltk.imports import *`, dumping 456 names into every module namespace. This makes dead code detection impossible, slows imports, and obscures real dependencies.

## Scope
- **Category A (67 files):** Replace `from lltk.imports import *` with explicit imports
- **Category C (~10 files):** Replace internal cross-module stars (`from .utils import *`, `from lltk.corpus.tcp import *`, `from lltk.model.networks import *`)
- **imports.py itself:** Replace its internal star imports from tools with explicit imports; remove redundant `from lltk.tools import *`
- **Out of scope:** `__init__.py` re-exports (Category B) — standard Python pattern, kept as-is

## Import Style (Option B)
```python
import os                                    # stdlib: direct
import pandas as pd                          # third-party: direct
from lltk.imports import PATH_CORPUS, log    # lltk-specific: explicit from imports
```

## Phases

### Phase 0: Analysis script
Python script using `ast` to analyze each file, determine which of the 456 names are actually used, categorize them (stdlib / third-party / lltk-specific), and output a JSON replacement plan.

### Phase 1: Break circular chain (sequential)
Fix the 5 `tools/` files + `imports.py` — these form a circular import chain that must be resolved first.
1. `tools/tools.py` — already defines `config`; import stdlib directly
2. `tools/logs.py` — only needs `os`, `sys`
3. `tools/baseobj.py` — stdlib direct + explicit from tools/logs
4. `tools/freqs.py`, `tools/stats.py` — same pattern
5. `imports.py` — replace lines 38-39 with explicit imports (~6 names); delete line 291

Test after.

### Phase 2: Core modules (sequential)
Fix `text/` and `corpus/corpus.py` — other corpus files inherit from these.
- `text/utils.py`, `text/text.py`, `text/textlist.py`
- `corpus/utils.py`, `corpus/corpus.py` (also fix Category C: `from lltk.text import *` and `from .utils import *`)

Test after.

### Phase 3: Leaf files (5 parallel agents, ~12 files each)
Corpus files and model files are leaves — no interdependency. Apply pre-computed replacements from the Phase 0 script. Also fix Category C stars in each batch.

### Phase 4: Verification
- Full test suite
- `import lltk` still works
- `lltk.Text`, `lltk.Corpus` still accessible

## Risk Mitigation
- Analysis script validates before any edits
- Sequential phases 1-2 tested before parallel blast
- Leaf files can't break each other
- Working on `no-stars` branch; master is safe
