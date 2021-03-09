# lltk

Literary Language Tool Kit (LLTK): corpora, models, and tools for the study of complex language.

## Quickstart

1) Install:

Open a terminal and type:

```
pip install git+https://github.com/literarylab/lltk  
```

2) Download an existing corpus...

```
lltk status                            # show which corpora/data are available
lltk download ECCO_TCP                 # download a corpus
```

...or import your own:

```
lltk import                            # use the "import" command \
  -path_txt mycorpus/txts             # a folder of txt files  (use -path_xml for xml) \
  -path_metadata mycorpus/meta.xls    # a metadata csv/tsv/xls about those txt files \
  -col_fn filename                    # filename in the metadata corresponding to the .txt filename
```

...or start a new one:

```
lltk create                            # then follow the interactive prompt
```

3) Then you can load the corpus in Python:

```python
import lltk                            # import lltk as a python module
corpus = lltk.load('ECCO_TCP')         # load the corpus by name or ID
```

...and play with convenient Corpus objects...

```python
df = corpus.metadata                       # get corpus metadata as a pandas dataframe
df_sample=df.query('1740 < year < 1780')   # do a quick query on the metadata

texts = corpus.texts()                     # get a convenient Text object for each text
texts_sample = corpus.texts(df_sample.id)  # get Text objects for a specific list of IDs
```

...and Text objects:

```python
for text in texts_sample:             # loop over Text objects
    text_meta = text.meta             # get text metadata as dictionary
    author = text.author              # get common metadata as attributes    

    txt = text.txt                    # get plain text as string
    xml = text.xml                    # get xml as string

    tokens = text.tokens              # get list of words (incl punct)
    words  = text.words               # get list of words (excl punct)
    counts = text.word_counts         # get word counts as dictionary (from JSON if saved)
    ocracc = text.ocr_accuracy        # get estimate of ocr accuracy

    spacy_obj = text.spacy            # get a spacy text object
    nltk_obj = text.nltk              # get an nltk text object
    blob_obj = text.blob              # get a textblob object
```

## Corpus magic

Each corpus object can generate data about itself:

```python
corpus.save_metadata()                # save metadata from xml files (if possible)
corpus.save_plain_text()              # save plain text from xml (if possible)
corpus.save_mfw()                     # save list of all words in corpus and their total  count
corpus.save_freqs()                   # save counts as JSON files
corpus.save_dtm()                     # save a document-term matrix with top N words
```

You can also run these commands in the terminal:

```
lltk install my_corpus                 # this is equivalent to python above
lltk install my_corpus -parallel 4     # but can access parallel processing with MPI/Slingshot
lltk install my_corpus dtm             # run a specific step
```

Generating this kind of data allows for easier access to things like:

```python
mfw = corpus.mfw(n=10000)             # get the 10K most frequent words
dtm = corpus.freqs(words=mfw)         # get a document-term matrix as a pandas dataframe
```

You can also build word2vec models:

```python
w2v_model = corpus.word2vec()         # get an lltk word2vec model object
w2v_model.model()                     # run the modeling process
w2v_model.save()                      # save the model somewhere
gensim_model = w2v_model.gensim       # get the original gensim object
```


## Available corpora

Key: ...


| name                                                        | desc                                                                                                       | metadata   | freqs   | txt   | xml   | raw   |
|:------------------------------------------------------------|:-----------------------------------------------------------------------------------------------------------|:-----------|:--------|:------|:------|:------|
| [ARTFL](lltk/lltk/corpus/ARTFL)                             | American and French Research on the Treasury of the French Language                                        | ✓          | ✓       |       |       |       |
| [BPO](lltk/lltk/corpus/BPO)                                 | British Periodicals Online                                                                                 |            |         |       |       | ☂     |
| [CLMET](lltk/lltk/corpus/CLMET)                             | Corpus of Late Modern English Texts                                                                        | ✓          | ✓       | ↓     | ↓     |       |
| [COCA](lltk/lltk/corpus/COCA)                               | Corpus of Contemporary American English                                                                    |            |         |       |       | ☂     |
| [COHA](lltk/lltk/corpus/COHA)                               | Corpus of Historical American English                                                                      |            |         |       |       | ☂     |
| [CanonFiction](lltk/lltk/corpus/CanonFiction)               |                                                                                                            | ✓          | ✓       |       |       |       |
| [Chadwyck](lltk/lltk/corpus/Chadwyck)                       | Chadwyck-Healey Fiction Collections                                                                        | ✓          | ✓       |       |       |       |
| [ChadwyckDrama](lltk/lltk/corpus/ChadwyckDrama)             | Chadwyck-Healey Drama Collections                                                                          | ✓          | ✓       |       |       |       |
| [ChadwyckPoetry](lltk/lltk/corpus/ChadwyckPoetry)           | Chadwyck-Healey Poetry Collections                                                                         | ✓          | ✓       |       |       |       |
| [Chicago](lltk/lltk/corpus/Chicago)                         | U of Chicago Corpus of C20 Novels                                                                          | ✓          | ✓       |       |       |       |
| [DTA](lltk/lltk/corpus/DTA)                                 | Deutsches Text Archiv                                                                                      | ✓          | ✓       | ✓     | ✓     | ↓     |
| [DialNarr](lltk/lltk/corpus/DialNarr)                       | Dialogue/Narration in Chadwyck-Healey Novels                                                               | ✓          | ✓       |       |       |       |
| [ECCO](lltk/lltk/corpus/ECCO)                               | Eighteenth Century Collections Online                                                                      | ✓          |         |       |       |       |
| [ECCO_TCP](lltk/lltk/corpus/ECCO_TCP)                       | ECCO (curated by the Text Creation Partnership)                                                            | ✓          | ✓       | ✓     | ↓     | ✓     |
| [EEBO_TCP](lltk/lltk/corpus/EEBO_TCP)                       | Early English Books Online (curated by the Text Creation Partnership)                                      | ✓          | ✓       | ✓     | ✓     |       |
| [ESTC](lltk/lltk/corpus/ESTC)                               | English Short Title Catalogue                                                                              | ✓          |         |       |       |       |
| [EnglishDialogues](lltk/lltk/corpus/EnglishDialogues)       | A Corpus of English Dialogues, 1560-1760                                                                   | ✓          | ↓       |       | ↓     |       |
| [FanFic](lltk/lltk/corpus/FanFic)                           |                                                                                                            |            |         |       |       | ☂     |
| [GaleAmericanFiction](lltk/lltk/corpus/GaleAmericanFiction) | Gale American Fiction, 1774-1920                                                                           | ✓          | ✓       |       |       |       |
| [GildedAge](lltk/lltk/corpus/GildedAge)                     | U.S. Fiction of the Gilded Age                                                                             | ✓          | ✓       | ✓     |       |       |
| [Hathi](lltk/lltk/corpus/Hathi)                             | Hathi Trust Research Center                                                                                |            | ✓       |       |       |       |
| [HathiBio](lltk/lltk/corpus/HathiBio)                       | Biographies from Hathi Trust                                                                               | ✓          | ✓       |       |       |       |
| [HathiEngLit](lltk/lltk/corpus/HathiEngLit)                 | English Literature corpus from Hathi Trust                                                                 | ✓          | ✓       |       |       |       |
| [HathiEssays](lltk/lltk/corpus/HathiEssays)                 | Volumes with "essay(s)" in title                                                                           | ✓          | ✓       |       |       |       |
| [HathiLetters](lltk/lltk/corpus/HathiLetters)               | Volumes with "letter(s)" in title                                                                          | ✓          | ✓       |       |       |       |
| [HathiNovels](lltk/lltk/corpus/HathiNovels)                 | Volumes with "novel(s)" in title                                                                           | ✓          | ✓       |       |       |       |
| [HathiProclamations](lltk/lltk/corpus/HathiProclamations)   | Volumes with "proclamation(s)" in title                                                                    | ✓          | ✓       |       |       |       |
| [HathiSermons](lltk/lltk/corpus/HathiSermons)               | Volumes with "sermon(s)" in title                                                                          | ✓          | ✓       |       |       |       |
| [HathiStories](lltk/lltk/corpus/HathiStories)               | Volumes with "story/stories" in title                                                                      | ✓          | ✓       |       |       |       |
| [HathiTales](lltk/lltk/corpus/HathiTales)                   | Volumes with "tale(s)" in title                                                                            | ✓          | ✓       |       |       |       |
| [HathiTreatises](lltk/lltk/corpus/HathiTreatises)           | Volumes with "treatise(s)" in title                                                                        | ✓          | ✓       |       |       |       |
| [InternetArchive](lltk/lltk/corpus/InternetArchive)         | 19th Century Novels, curated by the U of Illinois and hosted on the Internet Archive                       | ✓          | ✓       | ✓     |       |       |
| [LitLab](lltk/lltk/corpus/LitLab)                           | Literary Lab Corpus of 18th and 19th Century Novels                                                        | ✓          | ✓       |       |       |       |
| [MarkMark](lltk/lltk/corpus/MarkMark)                       | Mark Algee-Hewitt's and Mark McGurl's 20th Century Corpus                                                  | ✓          | ✓       |       |       |       |
| [NewYorker](lltk/lltk/corpus/NewYorker)                     | New Yorker archives, 1925-2017                                                                             |            |         |       |       | ☂     |
| [OldBailey](lltk/lltk/corpus/OldBailey)                     | Old Bailey Online                                                                                          | ✓          | ✓       | ↓     | ↓     |       |
| [PMLA](lltk/lltk/corpus/PMLA)                               |                                                                                                            |            |         |       |       | ☂     |
| [RavenGarside](lltk/lltk/corpus/RavenGarside)               | Raven & Garside's Bibliography of English Novels, 1770-1830                                                | ✓          |         |       |       |       |
| [SOTU](lltk/lltk/corpus/SOTU)                               | State of the Union Addresses                                                                               |            | ✓       | ↓     |       |       |
| [Sellars](lltk/lltk/corpus/Sellars)                         | 19th Century Texts compiled by Jordan Sellars                                                              | ✓          | ✓       | ✓     |       |       |
| [SemanticCohort](lltk/lltk/corpus/SemanticCohort)           | Corpus for "Semantic Cohort Method" (Literary Lab Pamphlet 4, 2012)                                        | ✓          |         |       |       |       |
| [Spectator](lltk/lltk/corpus/Spectator)                     | The Spectator (1711-1714)                                                                                  | ✓          | ✓       | ✓     |       |       |
| [TedJDH](lltk/lltk/corpus/TedJDH)                           | From Ted Underwood & Jordan Sellars, "Emergence of Literary Diction", Journal of Digital Humanities (2012) | ✓          | ✓       | ✓     |       |       |
| [TxtLab](lltk/lltk/corpus/TxtLab)                           | A multilingual dataset of 450 novels                                                                       | ✓          | ✓       | ✓     |       | ↓     |

