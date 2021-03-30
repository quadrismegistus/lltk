# Literary Language Toolkit (LLTK)
Corpora, models, and tools for the study of complex language.

## Quickstart

See [this notebook](https://nbviewer.jupyter.org/gist/quadrismegistus/0fb3821cd00d7879bf6ce3bc9ab954fe) for a more interactive quickstart ([run the code here on Binder](https://mybinder.org/v2/gist/quadrismegistus/0fb3821cd00d7879bf6ce3bc9ab954fe/master?filepath=lltk-readme.ipynb)).

### Install

Open a terminal, Jupyter, or Colab notebook and type:

```
pip install -qU lltk-dh

# or for very latest version:
#pip install -qU git+https://github.com/quadrismegistus/lltk
```

Show [available corpora](#available-corpora):
```
lltk show
```

Or, within python, show in markdown:

```python
import lltk
lltk.show()
```

### Play with corpora

See [below](#available-corpora) for available corpora.

```python
# Load/install a corpus
import lltk
corpus = lltk.load('ECCO_TCP')           # load the corpus by name or ID

# Metadata
meta = corpus.meta                       # metadata as data frame
smpl = meta.query('1770<year<1830')      # easy query access         

# Data
mfw = corpus.mfw()                       # get the 10K most frequent words as a list
dtm = corpus.dtm()                       # get a document-term matrix as a pandas dataframe
dtm = corpus.dtm(tfidf=True)             # get DTM as tf-idf
mdw = corpus.mdw('gender')               # get most distinctive words for a metadata group
```

### Play with texts

```python
# accessing text objs
texts = corpus.texts()                   # get a list of corpus's text objects
texts_smpl = corpus.texts(smpl)          # text objects from df/list of ids 
texts_rad = corpus.au.Radcliffe          # hit "tab" after typing e.g. "Rad" to autocomplete 
text = corpus.t                          # get a random text object from corpus

# metadata access
text_meta = text.meta                    # get text metadata as dictionary
author = text.author                     # get common metadata as attributes    
title = text.title
year = text.year
dec = text.decade                        # few inferred attributes too
dec_str = text.decade_str                # '1890-1900' rather than 1890

# data access
txt = text.txt                           # get plain text as string
xml = text.xml                           # get xml as string

# simple nlp
words  = text.words                      # get list of words (excl punct)
sents = text.sents                       # get list of sentences
counts = text.counts                     # get word counts as dictionary (from JSON if saved)

# other nlp
tnltk = text.nltk                        # get nltk Text object
tblob = text.blob                        # get TextBlob object
tstanza = text.stanza                    # get list of stanza objects (one per para)
tspacy = text.spacy                      # get list of spacy objects (one per para)
```


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
| ECCO                | [Eighteenth Century Collections Online](https://www.gale.com/intl/primary-sources/eighteenth-century-collections-online)                                                  | Commercial                                                                  | ☂️                                                                                     | ☂️                                                                                  | ☂️                                                                             | ☂️                                                                      | ☂️                                                                      |
| ECCO_TCP            | [ECCO (Text Creation Partnership)](https://textcreationpartnership.org/tcp-texts/ecco-tcp-eighteenth-century-collections-online/)                                         | Free                                                                        | [🌞](https://www.dropbox.com/s/xh991n4sohulczb/ecco_tcp_metadata.zip?dl=1)            | [🌞](https://www.dropbox.com/s/sdf5pdyifnrulyk/ecco_tcp_freqs.zip?dl=1)            | [🌞](https://www.dropbox.com/s/8sa4f6yqpz6ku3d/ecco_tcp_txt.zip?dl=1)         | [🌞](https://www.dropbox.com/s/vtv2iw7ujtivqss/ecco_tcp_xml.zip?dl=1)  | [🌞](https://www.dropbox.com/s/aubdaixvc59d8o9/ecco_tcp_raw.zip?dl=1)  |
| EEBO_TCP            | [Early English Books Online (curated by the Text Creation Partnership)](https://textcreationpartnership.org/tcp-texts/eebo-tcp-early-english-books-online/)               | Free                                                                        | [🌞](https://www.dropbox.com/s/th2i7jvuxksb0ma/eebo_tcp_metadata.zip?dl=1)            | [🌞](https://www.dropbox.com/s/n2oocs233wh5edo/eebo_tcp_freqs.zip?dl=1)            | [🌞](https://www.dropbox.com/s/otgqbs0vdli3gvb/eebo_tcp_txt.zip?dl=1)         | [🌞](https://www.dropbox.com/s/1wui9qjhkzy8fnm/eebo_tcp_xml.zip?dl=1)  |                                                                        |
| ESTC                | [English Short Title Catalogue](http://estc.ucr.edu/)                                                                                                                     | Academic                                                                    | ☂️                                                                                     |                                                                                    |                                                                               |                                                                        |                                                                        |
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






## Documentation

Incomplete for now. See [this sample notebook](https://nbviewer.jupyter.org/gist/quadrismegistus/0fb3821cd00d7879bf6ce3bc9ab954fe) for some examples.

### New corpus

Import a corpus into LLTK:

```
lltk import                           # use the "import" command \
  -path_txt mycorpus/txts             # a folder of txt files  (use -path_xml for xml) \
  -path_metadata mycorpus/meta.xls    # a metadata csv/tsv/xls about those txt files \
  -col_fn filename                    # .txt/.xml filename col in metadata (use -col_id if no ext)
```

Or create a new one:
```
lltk create
```


### Most frequent words

```python
corpus.mfw_df(
    n=None,                            # Number of top words overall
    by_ntext=False,                    # Count number of documents not number of words
    by_fpm=False,                      # Count by within-text relative sums
    min_count=None,                    # Minimum count of word

    yearbin=None,                      # Average relative counts across `yearbin` periods
    col_group='period',                # Which column to periodize on
    n_by_period=None,                  # Number of top words per period
    keep_periods=True,                 # Keep periods in output dataframe
    n_agg='median',                    # How to aggregate across periods
    min_periods=None,                  # minimum number of periods a word must appear in

    excl_stopwords=False,              # Exclude stopwords (at `PATH_TO_ENGLISH_STOPWORDS`)
    excl_top=0,                        # Exclude words ranked 1:`not_top`
    valtype='fpm',                     # valtype to compute top words by
    **attrs
)
```

### Document term matrix

```python
corpus.dtm(
    words=[],                          # words to use in DTM
    n=25000,                           # if not `words`, how many mfw?
    texts=None,                        # set texts to use explicitly, otherwise use all
    tf=False,                          # return term frequencies, not counts
    tfidf=False,                       # return tfidf, not counts
    meta=False,                        # include metadata (e.g. ["gender","nation"])
    **mfw_attrs,                       # all other attributes passed to self.mfw()
)
```

### Most distinctive words

```python
corpus.mdw(                                 
    groupby,                           # metadata categorical variable to group by
    words=[],                          # explicitly set words to use
    texts=None,                        # explicitly set texts to use
    tfidf=True,                        # use tfidf as mdw calculation
    keep_null_cols=False,              # remove texts which do not have `groupby` set
    remove_zeros=True,                 # remove rows summing to zero
    agg='median',                      # aggregate by `agg`
)
```

