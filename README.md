# Literary Language Toolkit (LLTK)

A Python package for computational literary analysis and digital humanities research. Provides 50+ literary corpora, text processing tools, and analysis methods including word frequencies, document-term matrices, cross-corpus linking, and duplicate detection.

**Package:** `lltk-dh` on PyPI | **License:** MIT | **Python:** >=3.8

## Install

```bash
pip install -U lltk-dh

# or latest from source:
pip install -U git+https://github.com/quadrismegistus/lltk
```

## Quick start

```python
import lltk

# Load a corpus
c = lltk.load('ecco_tcp')

# Metadata as a pandas DataFrame
c.meta
c.meta.query('1770 < year < 1830')

# Iterate texts
for t in c.texts():
    print(t.id, t.author, t.title, t.year)
    print(t.txt[:200])
    print(t.freqs())       # word frequencies (Counter)
```

## Corpora

```python
lltk.show()                # list available corpora
c = lltk.load('ecco_tcp')  # load by name or ID
```

Corpora live at `~/lltk_data/corpora/<corpus_id>/`. Each has: `metadata.csv`, `txt/`, and optionally `xml/`, `freqs/`. Some corpora are freely downloadable; others require institutional access. See [Available Corpora](#available-corpora) below.

## Texts

```python
# Access text objects
for t in c.texts():
    t.id                    # text identifier
    t.author                # metadata attributes
    t.title
    t.year

    t.txt                   # plain text as string
    t.xml                   # XML source (if available)
    t.freqs()               # word frequencies (Counter)

# Direct access by ID
t = c.text('some_text_id')
```

### Sections

Texts can be split into structural sections (chapters, letters, etc.) from XML, or into paragraphs and fixed-length passages:

```python
# Chapters from XML (auto-detects <div>, <chapter>, <letter>, etc.)
for ch in t.chapters.texts():
    print(ch.get('title'), ch.txt[:100])

# Paragraphs (split on blank lines)
for p in t.paragraphs.texts():
    print(p.id, len(p.txt))

# Passages of ~500 words (respects sentence boundaries)
for p in t.passages(n=500).texts():
    print(p.id, p.get('num_words'))
    print(p.freqs())
```

Sections are `TextSection` objects inside a `SectionCorpus` — they support all the same methods as regular texts (`txt`, `freqs()`, `meta`, etc.).

## Corpus-level analysis

### Most frequent words

```python
mfw = c.mfw(n=10000)       # top 10K words across corpus (list)
```

### Document-term matrix

```python
dtm = c.dtm(n=10000)               # raw counts (DataFrame)
dtm = c.dtm(n=10000, tf=True)      # term frequencies
dtm = c.dtm(n=10000, tfidf=True)   # TF-IDF weighted
```

Returns a pandas DataFrame: rows = text IDs, columns = words.

### Duplicate detection

Find near-duplicate texts within a corpus using cosine similarity on TF-IDF word frequency vectors. Works even on corpora with only precomputed `freqs/` (no raw text needed).

```python
dupes = c.find_duplicates(
    n=5000,            # number of MFW features
    threshold=0.85,    # minimum cosine similarity
    k=10,              # max neighbors per text
)
# Returns DataFrame: id_1, id_2, similarity (sorted descending)
```

## Metadata

### Loading metadata

`c.meta` returns a pandas DataFrame loaded from the corpus's `metadata.csv`. Corpus subclasses can override `load_metadata()` to enrich columns without altering the CSV:

```python
c = lltk.load('estc')
c.meta  # includes enriched columns: format_std, num_pages, is_fiction, etc.
```

### Custom metadata loading

Override `load_metadata()` in a corpus subclass:

```python
class MyCorpus(BaseCorpus):
    def load_metadata(self):
        meta = super().load_metadata()
        meta['genre'] = 'Fiction'
        meta['decade'] = (meta['year'] // 10) * 10
        return meta
```

Results are cached — subsequent calls to `c.meta` or `c.load_metadata()` return the cached DataFrame.

## Cross-corpus linking

Corpora can declare shared-ID relationships for linking texts across collections. This supports two patterns:

### Metadata merging (many-to-one)

When many texts in corpus A map to one record in corpus B (e.g., many ECCO texts → one ESTC catalogue record), corpus A can merge B's metadata as prefixed columns:

```python
c = lltk.load('ecco')
c.meta
# DataFrame includes: ESTCID, estc_author, estc_format_std, estc_is_fiction, ...
```

This is configured declaratively on the corpus class:

```python
class ECCO(BaseCorpus):
    LINKS = {'estc': ('ESTCID', 'id_estc')}  # my_col → their_col

    def load_metadata(self):
        meta = super().load_metadata()
        return self.merge_linked_metadata(meta)
```

### Text traversal (one-to-many)

When one record maps to many texts in another corpus (e.g., one ESTC record → multiple ECCO editions), use `t.linked()`:

```python
c = lltk.load('estc')
t = c.text('T089174')

# Find all ECCO texts for this ESTC record
ecco_texts = t.linked('ecco')
for et in ecco_texts:
    print(et.id, et.title, et.year)

# Find all EEBO texts
eebo_texts = t.linked('eebo_tcp')
```

Linked texts are returned with full metadata. Lookup indices are built once and cached for O(1) traversal.

### ID normalization

When ID formats differ between corpora, use `LINK_TRANSFORMS`:

```python
class EEBO_TCP(BaseCorpus):
    LINKS = {'estc': ('id_stc', 'id_estc')}
    LINK_TRANSFORMS = {'id_stc': _normalize_estc_id}
    # 'ESTC S115782' → 'S115782'
```

### Currently linked corpora

| Source | Target | Link column | Direction |
|--------|--------|-------------|-----------|
| ECCO | ESTC | `ESTCID` → `id_estc` | metadata merge |
| EEBO_TCP | ESTC | `id_stc` → `id_estc` | metadata merge |
| ESTC | ECCO | `id_estc` → `ESTCID` | text traversal |
| ESTC | EEBO_TCP | `id_estc` → `id_stc` | text traversal |

## Architecture

```
lltk/
├── imports.py          # Constants, config, third-party imports
├── __init__.py         # Package entry point
├── text/
│   ├── text.py         # BaseText, TextSection, MemoryText, Text() factory
│   ├── textlist.py     # TextList collection class
│   └── utils.py        # Tokenization, XML parsing, text utilities
├── corpus/
│   ├── corpus.py       # BaseCorpus, SectionCorpus, Corpus() factory
│   ├── utils.py        # load_corpus(), manifest loading
│   ├── manifest.txt    # Corpus registry (configparser format)
│   └── <corpus_name>/  # Per-corpus implementations (50+)
├── model/
│   ├── preprocess.py   # Preprocessing (XML→TXT, TXT→freqs)
│   ├── matcher.py      # Text matching/dedup
│   └── ...             # word2vec, doc2vec, networks, etc.
└── tools/
    ├── baseobj.py      # BaseObject (root class)
    ├── tools.py        # Config, utilities, parallel processing
    ├── db.py           # Local DB backends
    └── logs.py         # Logging
```

### Key patterns

- **Inheritance:** `BaseObject` → `TextList` → `BaseCorpus` → corpus subclasses
- **Factories:** `Text(id)` and `Corpus(id)` return cached objects
- **Lazy loading:** Metadata loaded on first access via `load_metadata()`
- **Path resolution:** `corpus.path_*` attributes resolved via `__getattr__` → `get_path()`
- **Manifest:** Corpora registered in `manifest.txt` (configparser). Multiple manifest files merged from package dir, `~/lltk_data/`, and user config.
- **Metadata enrichment:** Override `load_metadata()` → call `super()` → transform DataFrame → return
- **Cross-corpus links:** `LINKS` dict + `merge_linked_metadata()` for joins, `t.linked()` for traversal

## Development

### Running tests

```bash
pip install pytest
python -m pytest tests/ -v
```

Tests use the `test_fixture` corpus (Blake, Austen, Shelley) checked into the repo — no external data needed.

### Adding a new corpus

1. Create `lltk/corpus/my_corpus/my_corpus.py`:

```python
from lltk.imports import *

class TextMyCorpus(BaseText):
    pass

class MyCorpus(BaseCorpus):
    TEXT_CLASS = TextMyCorpus

    def load_metadata(self):
        meta = super().load_metadata()
        # add/transform columns here
        return meta
```

2. Register in `lltk/corpus/manifest.txt`:

```ini
[MyCorpus]
id = my_corpus
name = MyCorpus
desc = Description of the corpus
path_python = my_corpus/my_corpus.py
class_name = MyCorpus
```

3. Place data at `~/lltk_data/corpora/my_corpus/`:
   - `metadata.csv` — with `id` column + any metadata columns
   - `txt/` — text files as `<text_id>.txt`
   - `freqs/` — (optional) precomputed word frequencies as JSON

## Available corpora

| Name | Description | License |
|:-----|:------------|:--------|
| ARTFL | [American and French Research on the Treasury of the French Language](https://artfl-project.uchicago.edu) | Academic |
| BPO | [British Periodicals Online](https://proquest.libguides.com/britishperiodicals) | Commercial |
| CLMET | [Corpus of Late Modern English Texts](https://perswww.kuleuven.be/~u0044428/clmet3_0.htm) | Academic |
| Chadwyck | [Chadwyck-Healey Fiction Collections](http://collections.chadwyck.co.uk/marketing/list_of_all.jsp) | Mixed |
| ChadwyckPoetry | [Chadwyck-Healey Poetry Collections](http://collections.chadwyck.co.uk/marketing/list_of_all.jsp) | Mixed |
| DTA | [Deutsches Text Archiv](http://www.deutschestextarchiv.de) | Free (CC BY-SA) |
| ECCO | [Eighteenth Century Collections Online](https://www.gale.com/intl/primary-sources/eighteenth-century-collections-online) | Commercial |
| ECCO_TCP | [ECCO Text Creation Partnership](https://textcreationpartnership.org/tcp-texts/ecco-tcp-eighteenth-century-collections-online/) | Free |
| EEBO_TCP | [Early English Books Online (TCP)](https://textcreationpartnership.org/tcp-texts/eebo-tcp-early-english-books-online/) | Free |
| ESTC | [English Short Title Catalogue](http://estc.ucr.edu/) | Academic |
| EvansTCP | [Early American Fiction (TCP)](https://textcreationpartnership.org/tcp-texts/evans-tcp-evans-early-american-imprints/) | Free |
| GaleAmericanFiction | [Gale American Fiction, 1774-1920](https://www.gale.com/c/american-fiction-1774-1920) | Academic |
| HathiBio | [Biographies from HathiTrust](https://www.ideals.illinois.edu/handle/2142/99554) | Academic |
| HathiEngLit | [Fiction, drama, verse from HathiTrust](https://wiki.htrc.illinois.edu/display/COM/Word+Frequencies+in+English-Language+Literature) | Academic |
| InternetArchive | [19th Century Novels (Internet Archive)](https://archive.org/details/19thcennov?tab=about) | Free |
| OldBailey | [Old Bailey Online](https://www.oldbaileyonline.org/) | Free (CC BY-NC) |
| Spectator | [The Spectator (1711-1714)](http://www.gutenberg.org/ebooks/12030) | Free |
| TxtLab | [Multilingual dataset of 450 novels](https://txtlab.org/2016/01/txtlab450-a-data-set-of-multilingual-novels-for-teaching-and-research) | Free |

And 30+ more — run `lltk.show()` for the complete list.
