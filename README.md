# Literary Language Toolkit (LLTK)

A Python package for computational literary analysis and digital humanities research. Provides 50+ literary corpora, text processing tools, and analysis methods including word frequencies, document-term matrices, cross-corpus linking, deduplication, and a centralized DuckDB metadata store for querying 2M+ texts across all corpora.

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

# List available corpora
lltk.show()

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

# Corpus-level analysis
mfw = c.mfw(n=10000)              # top 10K words across corpus
dtm = c.dtm(n=10000)              # document-term matrix (DataFrame)
dtm = c.dtm(n=10000, tfidf=True)  # TF-IDF weighted
```

### Installing corpus data

Corpora live at `~/lltk_data/corpora/<corpus_id>/`. Each has: `metadata.csv`, `txt/`, and optionally `xml/`, `freqs/`. Some corpora are freely downloadable; others require institutional access.

```bash
# Download a corpus (metadata + freqs ≈ 150 MB for ecco_tcp)
lltk install ecco_tcp --parts metadata,freqs

# Full texts (adds ~600 MB)
lltk install ecco_tcp --parts txt
```

## Texts

```python
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

### Currently linked corpora

| Source | Target | Link column | Direction |
|--------|--------|-------------|-----------|
| ECCO | ESTC | `ESTCID` → `id_estc` | metadata merge |
| EEBO_TCP | ESTC | `id_stc` → `id_estc` | metadata merge |
| ESTC | ECCO | `id_estc` → `ESTCID` | text traversal |
| ESTC | EEBO_TCP | `id_estc` → `id_stc` | text traversal |

## MetaDB (centralized DuckDB metadata store)

`lltk.db` is a DuckDB-backed metadata cache that indexes all corpora into a single queryable store. It enables fast single-row lookups, cross-corpus queries, title/author matching for deduplication, genre enrichment from bibliography corpora, and virtual corpus construction.

### Building the database

```bash
lltk db-rebuild                          # ingest all corpora (~4 min)
lltk db-rebuild estc ecco                # re-ingest specific corpora
lltk db-info                             # genre × corpus crosstab
lltk db-match                            # cross-corpus dedup matching (~5 min)
lltk db-enrich-genres                    # propagate genre from bibliographies
lltk db-wordcounts                       # compute word counts from freqs
```

### Querying

```python
import lltk

# Single-row lookup
lltk.db.get('_estc/T012345')

# SQL queries on the texts table
lltk.db.query("SELECT * FROM texts WHERE year < 1700 AND genre = 'Fiction'")
lltk.db.query("SELECT corpus, COUNT(*) as n FROM texts GROUP BY corpus")

# Iterate text objects with filters + dedup
for t in lltk.db.texts(genre='Fiction', year_min=1600, year_max=1800, dedup=True):
    print(t.corpus.id, t.title, t.year)
    print(t.freqs())   # resolves through source corpus

# As DataFrame (no text objects)
df = lltk.db.texts_df(genre='Fiction', dedup=True)

# As a corpus object (supports .mfw, .dtm, .meta)
fiction = lltk.db.corpus(genre='Fiction', dedup=True)
```

Text objects returned by `lltk.db.texts()` keep their original corpus reference, so `t.txt`, `t.freqs()`, and file paths all resolve through the source corpus.

### Cross-corpus matching

Matching finds duplicate/reprint texts across corpora via multiple tiers: exact title+author, containment (short title within long title), and optional fuzzy matching (Jaro-Winkler). Connected components are grouped and ranked by corpus source preference.

```python
lltk.db.match()                          # exact + containment matching
lltk.db.find_matches('Incognita')        # search match groups by title
lltk.db.get_group('_estc/T012345')       # all texts in same match group
```

### SyntheticCorpus (virtual corpora from DB queries)

Declarative corpus class that pulls texts from multiple source corpora, deduplicated:

```python
from lltk.corpus.synthetic import SyntheticCorpus

class BigFiction(SyntheticCorpus):
    ID = 'big_fiction'
    NAME = 'BigFiction'
    SOURCES = {
        'chadwyck': {'genre': 'Fiction'},
        'ecco': {'genre': 'Fiction'},
        'hathi_englit': {'genre': 'Fiction'},
    }
    DEDUP = True
    DEDUP_BY = 'oldest'

C = BigFiction()
C.meta                    # DataFrame — all fiction, deduplicated
for t in C.texts():
    t.txt[:100]           # resolves through source corpus paths
```

## Architecture

```
lltk/
├── imports.py          # Constants, config, third-party imports
├── __init__.py         # Package entry point
├── text/
│   ├── text.py         # BaseText, TextSection, Text() factory
│   ├── textlist.py     # TextList collection class
│   └── utils.py        # Tokenization, XML parsing, text utilities
├── corpus/
│   ├── corpus.py       # BaseCorpus, SectionCorpus, Corpus() factory
│   ├── synthetic.py    # SyntheticCorpus — virtual corpora from DuckDB queries
│   ├── utils.py        # load_corpus(), manifest loading, corpus discovery
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
    ├── metadb.py       # DuckDB centralized metadata store (lltk.db)
    └── logs.py         # Logging
```

### Key patterns

- **Inheritance:** `BaseObject` → `TextList` → `BaseCorpus` → corpus subclasses
- **Factories:** `Text(id)` and `Corpus(id)` return cached objects
- **Lazy loading:** Metadata loaded on first access via `load_metadata()`. Text metadata hydrated lazily on first attribute access.
- **Path resolution:** `corpus.path_*` attributes resolved via `__getattr__` → `get_path()`
- **Manifest:** Corpora registered in `manifest.txt` (configparser). Multiple manifest files merged from package dir, `~/lltk_data/`, and user config.
- **Metadata enrichment:** Override `load_metadata()` → call `super()` → transform DataFrame → return
- **Cross-corpus links:** `LINKS` dict + `merge_linked_metadata()` for joins, `t.linked()` for traversal
- **Parquet caching:** Metadata CSVs cached as `.parquet` for 5-10x faster subsequent reads

## CLI reference

```bash
lltk show                                # list available corpora
lltk install <corpus> [--parts ...]      # download corpus data
lltk compile <corpus>                    # compile corpus from raw sources
lltk preprocess <corpus> --parts txt     # XML→TXT conversion
lltk preprocess <corpus> --parts freqs   # TXT→word frequencies

lltk db-rebuild [corpus ...]             # rebuild DuckDB metadata store
lltk db-info                             # genre × corpus crosstab
lltk db-match [--fuzzy]                  # cross-corpus dedup matching
lltk db-enrich-genres                    # propagate genre from bibliographies
lltk db-wordcounts [-j N]               # compute word counts from freqs
lltk db-matches "title"                  # search matches by title
lltk db-match-stats                      # matching statistics

lltk annotate <corpus> [--port N]        # launch annotation web app
```

## Development

### Running tests

```bash
pip install pytest
python -m pytest tests/ -v
python -m pytest tests/ --cov=lltk --cov-report=term   # with coverage
```

199 tests using the `test_fixture` corpus (Blake, Austen, Shelley) checked into the repo — no external data needed.

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

LLTK has built in functionality for the following corpora. Some (🌞) are freely downloadable from the links below or the LLTK interface. Some of them (☂) require first accessing the raw data through your institutional or other subscription. Some corpora have a mixture, with some data open through fair research use (e.g. metadata, freqs) and some closed (e.g. txt, xml, raw).

| name                | desc                                                                                                                                                                      | license                                                                     | metadata                                                                              | freqs                                                                              | txt                                                                           | xml                                                                    | raw                                                                    |
|:--------------------|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:----------------------------------------------------------------------------|:--------------------------------------------------------------------------------------|:-----------------------------------------------------------------------------------|:------------------------------------------------------------------------------|:-----------------------------------------------------------------------|:-----------------------------------------------------------------------|
| ARTFL               | [American and French Research on the Treasury of the French Language](https://artfl-project.uchicago.edu)                                                                 | Academic                                                                    | ☂️                                                                                     | ☂️                                                                                  |                                                                               |                                                                        |                                                                        |
| BPO                 | [British Periodicals Online](https://proquest.libguides.com/britishperiodicals)                                                                                           | Commercial                                                                  | ☂️                                                                                     |                                                                                    |                                                                               |                                                                        | ☂️                                                                      |
| CLMET               | [Corpus of Late Modern English Texts](https://perswww.kuleuven.be/~u0044428/clmet3_0.htm)                                                                                 | [Academic](https://ota.bodleian.ox.ac.uk/repository/xmlui/page/licence-ota) | [🌞](https://www.dropbox.com/s/m1jxj45al7b17cv/clmet_metadata.zip?dl=1)               | [🌞](https://www.dropbox.com/s/lnvwnbzskvqsu9p/clmet_freqs.zip?dl=1)               | ☂️                                                                             | ☂️                                                                      |                                                                        |
| COCA                | [Corpus of Contemporary American English](https://www.english-corpora.org/coca/)                                                                                          | Commercial                                                                  | ☂️                                                                                     | ☂️                                                                                  | ☂️                                                                             |                                                                        | ☂️                                                                      |
| COHA                | [Corpus of Historical American English](https://www.english-corpora.org/coha/)                                                                                            | Commercial                                                                  | ☂️                                                                                     | ☂️                                                                                  | ☂️                                                                             |                                                                        | ☂️                                                                      |
| Chadwyck            | [Chadwyck-Healey Fiction Collections](http://collections.chadwyck.co.uk/marketing/list_of_all.jsp)                                                                        | Mixed                                                                       | [🌞](https://www.dropbox.com/s/byqbi8sik255469/chadwyck_metadata.zip?dl=1)            | [🌞](https://www.dropbox.com/s/syluxyz1mcx5495/chadwyck_freqs.zip?dl=1)            | ☂️                                                                             | ☂️                                                                      | ☂️                                                                      |
| ChadwyckDrama       | [Chadwyck-Healey Drama Collections](http://collections.chadwyck.co.uk/marketing/list_of_all.jsp)                                                                          | Mixed                                                                       | ☂️                                                                                     | ☂️                                                                                  | ☂️                                                                             | ☂️                                                                      | ☂️                                                                      |
| ChadwyckPoetry      | [Chadwyck-Healey Poetry Collections](http://collections.chadwyck.co.uk/marketing/list_of_all.jsp)                                                                         | Mixed                                                                       | ☂️                                                                                     | ☂️                                                                                  | ☂️                                                                             | ☂️                                                                      | ☂️                                                                      |
| Chicago             | [U of Chicago Corpus of C20 Novels](https://textual-optics-lab.uchicago.edu/us_novel_corpus)                                                                              | Academic                                                                    | [🌞](https://www.dropbox.com/s/oba29ymlg7arhdu/chicago_metadata.zip?dl=1)             | [🌞](https://www.dropbox.com/s/w29o1urthijbxgn/chicago_freqs.zip?dl=1)             | ☂️                                                                             |                                                                        |                                                                        |
| DTA                 | [Deutsches Text Archiv](http://www.deutschestextarchiv.de)                                                                                                                | [Free](https://creativecommons.org/licenses/by-sa/4.0/)                     | [🌞](https://www.dropbox.com/s/294h2suvtu6sing/dta_metadata.zip?dl=1)                 | [🌞](https://www.dropbox.com/s/nb1u0e77ng2d5mh/dta_freqs.zip?dl=1)                 | [🌞](https://www.dropbox.com/s/8ez1tpa7awfb100/dta_txt.zip?dl=1)              | [🌞](https://www.dropbox.com/s/jy0o1cy37wioqqv/dta_xml.zip?dl=1)       | [🌞](http://media.dwds.de/dta/download/dta_komplett_2019-06-05.zip)    |
| DialNarr            | [Dialogue and Narration separated in Chadwyck-Healey Novels](https://doi.org/10.1093/llc/fqx031)                                                                          | Academic                                                                    | [🌞](https://www.dropbox.com/s/jw53k1mba6eumna/dialnarr_metadata.zip?dl=1)            | [🌞](https://www.dropbox.com/s/rgduzqatl4j0x5s/dialnarr_freqs.zip?dl=1)            | ☂️                                                                             |                                                                        |                                                                        |
| EarlyPrint          | [EarlyPrint Project: EEBO/ECCO/Evans TCP with linguistic tagging](https://earlyprint.org)                                                                                 | Free                                                                        | 🌞                                                                                     | 🌞                                                                                  | 🌞                                                                             | 🌞                                                                      |                                                                        |
| ECCO                | [Eighteenth Century Collections Online](https://www.gale.com/intl/primary-sources/eighteenth-century-collections-online)                                                  | Commercial                                                                  | ☂️                                                                                     | ☂️                                                                                  | ☂️                                                                             | ☂️                                                                      | ☂️                                                                      |
| ECCO_TCP            | [ECCO (Text Creation Partnership)](https://textcreationpartnership.org/tcp-texts/ecco-tcp-eighteenth-century-collections-online/)                                         | Free                                                                        | [🌞](https://www.dropbox.com/s/xh991n4sohulczb/ecco_tcp_metadata.zip?dl=1)            | [🌞](https://www.dropbox.com/s/sdf5pdyifnrulyk/ecco_tcp_freqs.zip?dl=1)            | [🌞](https://www.dropbox.com/s/8sa4f6yqpz6ku3d/ecco_tcp_txt.zip?dl=1)         | [🌞](https://www.dropbox.com/s/vtv2iw7ujtivqss/ecco_tcp_xml.zip?dl=1)  | [🌞](https://www.dropbox.com/s/aubdaixvc59d8o9/ecco_tcp_raw.zip?dl=1)  |
| EEBO_TCP            | [Early English Books Online (curated by the Text Creation Partnership)](https://textcreationpartnership.org/tcp-texts/eebo-tcp-early-english-books-online/)               | Free                                                                        | [🌞](https://www.dropbox.com/s/th2i7jvuxksb0ma/eebo_tcp_metadata.zip?dl=1)            | [🌞](https://www.dropbox.com/s/n2oocs233wh5edo/eebo_tcp_freqs.zip?dl=1)            | [🌞](https://www.dropbox.com/s/otgqbs0vdli3gvb/eebo_tcp_txt.zip?dl=1)         | [🌞](https://www.dropbox.com/s/1wui9qjhkzy8fnm/eebo_tcp_xml.zip?dl=1)  |                                                                        |
| ESTC                | [English Short Title Catalogue](http://estc.ucr.edu/) (481K bibliographic records, metadata-only)                                                                         | Academic                                                                    | ☂️                                                                                     |                                                                                    |                                                                               |                                                                        |                                                                        |
| EnglishDialogues    | [A Corpus of English Dialogues, 1560-1760](https://ota.bodleian.ox.ac.uk/repository/xmlui/handle/20.500.12024/2507)                                                       | [Academic](https://ota.bodleian.ox.ac.uk/repository/xmlui/page/licence-ota) | [🌞](https://www.dropbox.com/s/lcudgwmxdpspsc9/dialogues_metadata.zip?dl=1)           | [🌞](https://www.dropbox.com/s/tji67pv89e61wd6/dialogues_freqs.zip?dl=1)           |                                                                               | [🌞](https://www.dropbox.com/s/u07u3mrrom3i9f5/dialogues_xml.zip?dl=1) |                                                                        |
| EvansTCP            | [Early American Fiction](https://textcreationpartnership.org/tcp-texts/evans-tcp-evans-early-american-imprints/)                                                          | Free                                                                        | [🌞](https://www.dropbox.com/s/jr1j9i7wbz5uh0f/evans_tcp_metadata.zip?dl=1)           | [🌞](https://www.dropbox.com/s/4r426a5f6jk3tq8/evans_tcp_freqs.zip?dl=1)           | [🌞](https://www.dropbox.com/s/ezen3zxyt9hzxxp/evans_tcp_txt.zip?dl=1)        | [🌞](https://www.dropbox.com/s/yg7hjf536klg04c/evans_tcp_xml.zip?dl=1) | [🌞](https://www.dropbox.com/s/05qtu8r2xejqpkh/evans_tcp_raw.zip?dl=1) |
| GaleAmericanFiction | [Gale American Fiction, 1774-1920](https://www.gale.com/c/american-fiction-1774-1920)                                                                                     | Academic                                                                    | [🌞](https://www.dropbox.com/s/9ysabqrrx05832u/gale_amfic_metadata.zip?dl=1)          | [🌞](https://www.dropbox.com/s/7tbwfcgbcincdi1/gale_amfic_freqs.zip?dl=1)          | ☂️                                                                             |                                                                        | ☂️                                                                      |
| GildedAge           | [U.S. Fiction of the Gilded Age](https://doi.org/10.1093/llc/fqv066)                                                                                                      | Academic                                                                    | [🌞](https://www.dropbox.com/s/fg605k0cnebf70i/gildedage_metadata.zip?dl=1)           | [🌞](https://www.dropbox.com/s/i5zjhil743rm907/gildedage_freqs.zip?dl=1)           | [🌞](https://www.dropbox.com/s/qnwbx488ftepuno/gildedage_txt.zip?dl=1)        |                                                                        |                                                                        |
| HathiBio            | [Biographies from Hathi Trust](https://www.ideals.illinois.edu/handle/2142/99554)                                                                                         | Academic                                                                    | [🌞](https://www.dropbox.com/s/wth2i53gg0tq18a/hathi_bio_metadata.zip?dl=1)           | [🌞](https://www.dropbox.com/s/3jq8rjtpec4a6g9/hathi_bio_freqs.zip?dl=1)           |                                                                               |                                                                        |                                                                        |
| HathiEngLit         | [Fiction, drama, verse word frequencies from Hathi Trust](https://wiki.htrc.illinois.edu/display/COM/Word+Frequencies+in+English-Language+Literature)                     | Academic                                                                    | [🌞](https://www.dropbox.com/s/gnwuwkpy4jybr5r/hathi_englit_metadata.zip?dl=1)        | [🌞](https://www.dropbox.com/s/jm858ej78x7h0vk/hathi_englit_freqs.zip?dl=1)        |                                                                               |                                                                        |                                                                        |
| HathiEssays         | [Hathi Trust volumes with "essay(s)" in title](https://catalog.hathitrust.org/Search/Home)                                                                                | Academic                                                                    | [🌞](https://www.dropbox.com/s/9s7vpe7bhqo86ic/hathi_essays_metadata.zip?dl=1)        | [🌞](https://www.dropbox.com/s/j1kppdj2h9t20fp/hathi_essays_freqs.zip?dl=1)        |                                                                               |                                                                        |                                                                        |
| HathiLetters        | [Hathi Trust volumes with "letter(s)" in title](https://catalog.hathitrust.org/Search/Home)                                                                               | Academic                                                                    | [🌞](https://www.dropbox.com/s/11lajafcunypul2/hathi_letters_metadata.zip?dl=1)       | [🌞](https://www.dropbox.com/s/e7b07d5tlkfj9sw/hathi_letters_freqs.zip?dl=1)       |                                                                               |                                                                        |                                                                        |
| HathiNovels         | [Hathi Trust volumes with "novel(s)" in title](https://catalog.hathitrust.org/Search/Home)                                                                                | Academic                                                                    | [🌞](https://www.dropbox.com/s/hurtnwujziwusqz/hathi_novels_metadata.zip?dl=1)        | [🌞](https://www.dropbox.com/s/yxpur1zvvbp7cwp/hathi_novels_freqs.zip?dl=1)        |                                                                               |                                                                        |                                                                        |
| HathiProclamations  | [Hathi Trust volumes with "proclamation(s)" in title](https://catalog.hathitrust.org/Search/Home)                                                                         | Academic                                                                    | [🌞](https://www.dropbox.com/s/enksc8u5bqukc24/hathi_proclamations_metadata.zip?dl=1) | [🌞](https://www.dropbox.com/s/0wzzfcd4qeb17ed/hathi_proclamations_freqs.zip?dl=1) |                                                                               |                                                                        |                                                                        |
| HathiSermons        | [Hathi Trust volumes with "sermon(s)" in title](https://catalog.hathitrust.org/Search/Home)                                                                               | Academic                                                                    | [🌞](https://www.dropbox.com/s/9s70xbcwrqad88c/hathi_sermons_metadata.zip?dl=1)       | [🌞](https://www.dropbox.com/s/gchqmnt1yhhephz/hathi_sermons_freqs.zip?dl=1)       |                                                                               |                                                                        |                                                                        |
| HathiStories        | [Hathi Trust volumes with "story/stories" in title](https://catalog.hathitrust.org/Search/Home)                                                                           | Academic                                                                    | [🌞](https://www.dropbox.com/s/sfzs0t1hodb9r9d/hathi_stories_metadata.zip?dl=1)       | [🌞](https://www.dropbox.com/s/g004z8lgyxxhwip/hathi_stories_freqs.zip?dl=1)       |                                                                               |                                                                        |                                                                        |
| HathiTales          | [Hathi Trust volumes with "tale(s)" in title](https://catalog.hathitrust.org/Search/Home)                                                                                 | Academic                                                                    | [🌞](https://www.dropbox.com/s/hig9r9igcxp95sy/hathi_tales_metadata.zip?dl=1)         | [🌞](https://www.dropbox.com/s/b31o13d6l5do1kk/hathi_tales_freqs.zip?dl=1)         |                                                                               |                                                                        |                                                                        |
| HathiTreatises      | [Hathi Trust volumes with "treatise(s)" in title](https://catalog.hathitrust.org/Search/Home)                                                                             | Academic                                                                    | [🌞](https://www.dropbox.com/s/az903wuhx1b8zu1/hathi_treatises_metadata.zip?dl=1)     | [🌞](https://www.dropbox.com/s/hafinhgc8u77vpz/hathi_treatises_freqs.zip?dl=1)     |                                                                               |                                                                        |                                                                        |
| InternetArchive     | [19th Century Novels, curated by the U of Illinois and hosted on the Internet Archive](https://archive.org/details/19thcennov?tab=about)                                  | Free                                                                        | [🌞](https://www.dropbox.com/s/yymc8t060eik7bt/internet_archive_metadata.zip?dl=1)    | [🌞](https://www.dropbox.com/s/eofh9npy5x7qn5o/internet_archive_freqs.zip?dl=1)    | [🌞](https://www.dropbox.com/s/bs1ec7k9kk2jkrt/internet_archive_txt.zip?dl=1) |                                                                        |                                                                        |
| LitLab              | [Literary Lab Corpus of 18th and 19th Century Novels](https://litlab.stanford.edu/LiteraryLabPamphlet11.pdf)                                                              | Academic                                                                    | [🌞](https://www.dropbox.com/s/ruur7jrckhm8nqz/litlab_metadata.zip?dl=1)              | [🌞](https://www.dropbox.com/s/itoj9a8n4vrjot9/litlab_freqs.zip?dl=1)              | ☂️                                                                             |                                                                        |                                                                        |
| MarkMark            | [Mark Algee-Hewitt's and Mark McGurl's 20th Century Corpus](https://litlab.stanford.edu/LiteraryLabPamphlet8.pdf)                                                         | Academic                                                                    | [🌞](https://www.dropbox.com/s/y5r316u8fzorx3g/markmark_metadata.zip?dl=1)            | [🌞](https://www.dropbox.com/s/xbjugeqndquph55/markmark_freqs.zip?dl=1)            | ☂️                                                                             |                                                                        |                                                                        |
| OldBailey           | [Old Bailey Online](https://www.oldbaileyonline.org/)                                                                                                                     | [Free](https://creativecommons.org/licenses/by-nc/4.0/)                     | [🌞](https://www.dropbox.com/s/zc6osrvsgp0n1m4/oldbailey_metadata.zip?dl=1)           | [🌞](https://www.dropbox.com/s/rwgt7q1f6pl65jh/oldbailey_freqs.zip?dl=1)           | [🌞](https://www.dropbox.com/s/yjsjnk4eyprifem/oldbailey_txt.zip?dl=1)        | [🌞](https://www.dropbox.com/s/90bsbu7re5tnbtp/oldbailey_xml.zip?dl=1) |                                                                        |
| RavenGarside        | [Raven & Garside's Bibliography of English Novels, 1770-1830](https://catalog.hathitrust.org/Record/004098100)                                                            | Academic                                                                    | ☂️                                                                                     |                                                                                    |                                                                               |                                                                        |                                                                        |
| SOTU                | [State of the Union Addresses](https://www.kaggle.com/rtatman/state-of-the-union-corpus-1989-2017)                                                                        | Free                                                                        | [🌞](https://www.dropbox.com/s/6gyueael6smbxyg/sotu_metadata.zip?dl=1)                | [🌞](https://www.dropbox.com/s/34gz1aifsot65fw/sotu_freqs.zip?dl=1)                | [🌞](https://www.dropbox.com/s/w73qio0thhfzdpx/sotu_txt.zip?dl=1)             |                                                                        |                                                                        |
| Sellers             | [19th Century Texts compiled by Jordan Sellers](http://journalofdigitalhumanities.org/1-2/the-emergence-of-literary-diction-by-ted-underwood-and-jordan-sellers/)         | Free                                                                        | [🌞](https://www.dropbox.com/s/7mos2k5qx8bdc1l/sellers_metadata.zip?dl=1)             | [🌞](https://www.dropbox.com/s/k293ip4wrswhl8j/sellers_freqs.zip?dl=1)             | [🌞](https://www.dropbox.com/s/j7e5my3s20n3xq4/sellers_txt.zip?dl=1)          |                                                                        |                                                                        |
| SemanticCohort      | [Corpus used in "Semantic Cohort Method" (2012)](https://litlab.stanford.edu/LiteraryLabPamphlet8.pdf)                                                                    | Free                                                                        | [🌞](https://www.dropbox.com/s/f6imhtfzgpf7tvz/semantic_cohort_metadata.zip?dl=1)     |                                                                                    |                                                                               |                                                                        |                                                                        |
| Spectator           | [The Spectator (1711-1714)](http://www.gutenberg.org/ebooks/12030)                                                                                                        | Free                                                                        | [🌞](https://www.dropbox.com/s/3cw2lcza68djzj1/spectator_metadata.zip?dl=1)           | [🌞](https://www.dropbox.com/s/sil5q31833rz4n0/spectator_freqs.zip?dl=1)           | [🌞](https://www.dropbox.com/s/goj6xbom3qnv5u5/spectator_txt.zip?dl=1)        |                                                                        |                                                                        |
| TedJDH              | [Corpus used in "Emergence of Literary Diction" (2012)](http://journalofdigitalhumanities.org/1-2/the-emergence-of-literary-diction-by-ted-underwood-and-jordan-sellers/) | Free                                                                        | [🌞](https://www.dropbox.com/s/ibjl7x0eyyz5zm6/tedjdh_metadata.zip?dl=1)              | [🌞](https://www.dropbox.com/s/igoxb4y7buctm5o/tedjdh_freqs.zip?dl=1)              | [🌞](https://www.dropbox.com/s/8ug3h24h5bggnx7/tedjdh_txt.zip?dl=1)           |                                                                        |                                                                        |
| TxtLab              | [A multilingual dataset of 450 novels](https://txtlab.org/2016/01/txtlab450-a-data-set-of-multilingual-novels-for-teaching-and-research)                                  | Free                                                                        | [🌞](https://www.dropbox.com/s/eh33qy6bcm7rvcp/txtlab_metadata.zip?dl=1)              | [🌞](https://www.dropbox.com/s/56azeswx0omjum2/txtlab_freqs.zip?dl=1)              | [🌞](https://www.dropbox.com/s/q4bm4yf76zgumi6/txtlab_txt.zip?dl=1)           |                                                                        | [🌞](https://github.com/christofs/txtlab450/archive/master.zip)        |
