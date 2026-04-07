"""
Genre classification and translation detection for ESTC records.

classify_genres() assigns fine-grained genre labels from MARC 655 (form)
and 650 (subject) fields, with title-keyword fallback.

detect_translation() uses MARC structural signals (relator codes, uniform
title language) plus title/notes keyword matching.
"""

import re

# ═══════════════════════════════════════════════════════════════════
# Genre classification
# ═══════════════════════════════════════════════════════════════════

GENRE_RULES = {

    # ── Fiction sub-genres (more specific first, then catch-all) ────
    'Novel': {
        'form_exact': {
            'novels', 'novellas', 'gothic novels', 'gothic fiction',
            'historical fiction', 'biographical fiction',
            'mystery and detective fiction', 'comic histories',
        },
        'form_keywords': [],
        'topic_exact': set(),
        'topic_keywords': [],
        'title_keywords': [],  # 'novel' removed: too many false positives ("A Novel Method...")
    },

    'Romance': {
        'form_exact': {'romances'},
        'form_keywords': [],
        'topic_exact': set(),
        'topic_keywords': [],
        'title_keywords': [],  # 'romance' removed
    },

    'Tale': {
        'form_exact': {
            'fairy tales', 'folk tales', 'nursery stories',
        },
        'form_keywords': [],
        'topic_exact': set(),
        'topic_keywords': ["children's stories"],
        'title_keywords': [],  # 'tale'/'tales' removed
    },

    'Fable': {
        'form_exact': {'fables', 'fabliaux'},
        'form_keywords': [],
        'topic_exact': {'fables'},
        'topic_keywords': ['fables,'],
        'title_keywords': [],  # 'fable'/'fables' removed
    },

    'Picaresque': {
        'form_exact': {'picaresque fiction'},
        'form_keywords': ['picaresque'],
        'topic_exact': set(),
        'topic_keywords': [],
        'title_keywords': [],
    },

    'Epistolary fiction': {
        'form_exact': {'epistolary novels', 'epistolary fiction'},
        'form_keywords': [],
        'topic_exact': set(),
        'topic_keywords': ['epistolary fiction'],
        'title_keywords': [],
    },

    'Imaginary voyage': {
        'form_exact': {
            'imaginary voyages', 'imaginary conversations',
            'robinsonades',
        },
        'form_keywords': [],
        'topic_exact': set(),
        'topic_keywords': ['voyages, imaginary'],
        'title_keywords': [],
    },

    'Fiction': {
        'form_exact': {
            'fiction', 'adventure stories',
            'utopian literature', 'utopian literature.',
            'fantasy literature', 'harlequinades',
            'short stories', 'sea stories', 'bible stories',
            'comic books',
        },
        'form_keywords': [],
        'topic_exact': {
            'english fiction', 'french fiction', 'utopias',
            'fiction',
        },
        'topic_keywords': [],
        'title_keywords': [],
    },

    'Poetry': {
        'form_exact': {
            'poems', 'broadside poems', 'odes', 'elegies', 'verse',
            'lyric poems', 'occasional poems', 'pastoral poems',
            'poetical miscellanies', 'epics', 'acrostics',
            'epitaphs', 'epigrams', 'eclogues', 'sonnets',
            'english poetry', 'american poetry', 'neo-latin poems',
            'penny poems', 'begging poems', 'alphabet rhymes',
            'carol books', 'epistolary poetry', 'poetry of places',
        },
        'form_keywords': ['poem', 'poetry', 'elegiac'],
        'topic_exact': {
            'english poetry', 'latin poetry', 'french poetry',
            'greek poetry', 'italian poetry', 'poetry',
        },
        'topic_keywords': [
            'poetry,', 'elegiac poetry', 'verse,',
            'narrative poetry', 'love poetry', 'political poetry',
            'christian poetry', 'pastoral poetry', 'religious poetry',
            'occasional verse', 'epic poetry',
        ],
        'title_keywords': [
            'poem', 'poems', 'ode', 'odes', 'elegy', 'elegies',
            'verse', 'verses', 'sonnet', 'sonnets', 'eclogue',
            'epitaph', 'epigram', 'epigrams', 'poetry', 'poetical',
        ],
    },

    'Drama': {
        'form_exact': {
            'plays', 'plays.', 'comedies', 'tragedies',
            'tradegies', 'farces', 'tragicomedies', 'drama', 'dramas',
            'operas', 'operettas', 'libretti', 'masques',
            'morality plays', 'heroic dramas', 'ballad operas',
            'english drama (comedy', 'acting editions',
            'promptbooks', 'scenarios',
        },
        'form_keywords': ['play', 'drama', 'comedi', 'tragedi', 'librett', 'opera'],
        'topic_exact': {
            'english drama', 'opera', 'operas', 'drama',
            'french drama', 'italian drama', 'theater', 'theatres',
        },
        'topic_keywords': [
            'english drama', 'ballad opera', 'english farce',
        ],
        'title_keywords': [
            'comedy', 'tragedy', 'farce', 'opera',
            'acted', 'dramatick', 'dramatic', 'theatrical',
            'masque', 'interlude',
        ],
    },

    'Sermon': {
        'form_exact': {
            'sermons', 'funeral sermons', 'ordination sermons',
            'fast-day sermons', 'fast day sermons', 'thanksgiving sermons',
            'election sermons', 'execution sermons', 'occasional sermons',
            'artillery election sermons', 'dedication sermons',
            'installation sermons', 'farewell sermons', 'visitation sermons',
            "children's sermons", 'new year sermons', 'christmas sermons',
            'fourth of july sermons', 'anniversary sermons',
            'apocalyptic sermons', 'wedding sermons', 'century sermons',
            'half century sermons', 'thanksgiving day sermons',
        },
        'form_keywords': ['sermon'],
        'topic_exact': set(),
        'topic_keywords': ['sermon'],
        'title_keywords': ['sermon', 'sermons', 'preached', "preach'd"],
    },

    'Essay': {
        'form_exact': {'essays', 'essays.', 'essaysy', 'english essays'},
        'form_keywords': ['essay'],
        'topic_exact': {'english essays'},
        'topic_keywords': [],
        'title_keywords': ['essay', 'essays'],
    },

    'Letter': {
        'form_exact': {'letters'},
        'form_keywords': [],
        'topic_exact': {'english letters'},
        'topic_keywords': ['letter writing'],
        'title_keywords': ['letter', 'letters', 'epistle', 'epistles'],
    },

    'Almanac': {
        'form_exact': {'almanacs', 'alamancs', 'ephemerides'},
        'form_keywords': ['almanac'],
        'topic_exact': {'almanacs', 'ephemerides'},
        'topic_keywords': ['almanac'],
        'title_keywords': ['almanack', 'almanac', 'ephemeris', 'prognostication'],
    },

    'Ballad/Song': {
        'form_exact': {
            'ballads', 'songs', 'songsters', 'national songs',
            'song sheets', 'broadside ballads', 'carol books',
        },
        'form_keywords': ['ballad', 'songster'],
        'topic_exact': set(),
        'topic_keywords': ['ballads,', 'songs,', "children's songs"],
        'title_keywords': ['ballad', 'ballads', 'song', 'songs', 'garland'],
    },

    'Satire': {
        'form_exact': {
            'satires', 'satire', 'burlesques', 'lampoons',
            'lampoons test', 'parodies', 'facetiae', 'jestbooks',
            'caricatures',
        },
        'form_keywords': ['satir', 'lampoon', 'burlesque'],
        'topic_exact': set(),
        'topic_keywords': [
            'satire,', 'political satire', 'verse satire',
            'wit and humor', 'english wit',
        ],
        'title_keywords': ['satire', 'satyr', 'burlesque'],
    },

    'Catechism': {
        'form_exact': {'catechisms'},
        'form_keywords': ['catechism'],
        'topic_exact': {'catechisms'},
        'topic_keywords': ['catechism'],
        'title_keywords': ['catechism', 'catechisme', 'catechismus'],
    },

    'Devotional': {
        'form_exact': {
            'devotional literature', 'devoltional literature',
            'devotional works', 'prayer books', 'prayer books.',
            'prayers', 'hymnals', 'psalters', 'liturgical books',
            'books of hours', 'book of hours', 'breviaries', 'brevaries',
            'service books', 'missals', 'devotional calendars',
            'gospel books', 'evangeliaries',
            'primers (devotional books',
        },
        'form_keywords': ['devotional', 'prayer book', 'hymnal', 'psalter'],
        'topic_exact': {
            'devotional literature', 'devotional exercises',
            'meditations', 'prayers', 'prayer', 'psalmody',
            'psalters', 'worship', 'public worship',
        },
        'topic_keywords': [
            'devotional literature', 'devotional exercise',
            'hymns,', 'primers (prayer',
        ],
        'title_keywords': [
            'prayer', 'prayers', 'devotion', 'devotions',
            'meditation', 'meditations', 'liturgy', 'psalter',
        ],
    },

    'Hymn': {
        'form_exact': {'hymns', 'hymnals'},
        'form_keywords': ['hymn'],
        'topic_exact': set(),
        'topic_keywords': ['hymns,'],
        'title_keywords': ['hymn', 'hymns', 'psalm', 'psalms'],
    },

    'Periodical': {
        'form_exact': {
            'newspapers', 'periodicals', 'periocials', 'periiodicals',
            'newsbooks', 'newsletters',
        },
        'form_keywords': ['periodical', 'newspaper', 'newsbook', 'newsletter'],
        'topic_exact': set(),
        'topic_keywords': [],
        'title_keywords': [
            'gazette', 'mercury', 'courant', 'intelligencer',
            'advertiser', 'magazine',
        ],
    },

    'Catalogue': {
        'form_exact': {
            'catalogs', 'catalogues', "booksellers' catalogs",
            "booksellers' catalogues", 'library catalogs',
            'auction catalogs', 'auction catalogues',
            'sales catalogs', 'sales catalogues', 'college catalogs',
            'trade catalogs', 'exhibition catalogs', "publishers' catalogs",
            'book auction catalogs', 'museum catalogs', 'university catalogs',
        },
        'form_keywords': ['catalog'],
        'topic_exact': set(),
        'topic_keywords': ['catalogs,', 'book auction', 'estate sales'],
        'title_keywords': ['catalogue', 'catalog'],
    },

    'Biography': {
        'form_exact': {
            'biographies', 'biogarphies', 'biography', 'memoirs',
            'autobiographies', 'diaries',
        },
        'form_keywords': ['biograph'],
        'topic_exact': {'biography', 'christian biography'},
        'topic_keywords': ['biograph'],
        'title_keywords': ['life', 'lives', 'memoir', 'memoirs'],
    },

    'History': {
        'form_exact': {
            'chronicles', 'historical works', 'military histories',
            'local histories', 'family histories', 'military unit histories',
            'annals',
        },
        'form_keywords': ['histor'],
        'topic_exact': {'history', 'world history', 'church history'},
        'topic_keywords': ['history,', 'constitutional history'],
        'title_keywords': ['chronicle', 'annals'],  # not 'history' — too many novels use it
    },

    'Travel': {
        'form_exact': {
            'travel literature', 'exploration literature',
            'discovery narratives', 'maritime journals', 'roadbooks',
        },
        'form_keywords': ['travel'],
        'topic_exact': {'voyages and travels'},
        'topic_keywords': ['voyages and travel', 'voyages around', 'shipwreck'],
        'title_keywords': ['voyage', 'voyages', 'travels', 'tour'],
    },

    'Legal': {
        'form_exact': {
            'laws', 'bills', 'biils', 'legal works', 'session laws',
            'legal formularies', 'legal instruments', 'legal petitions',
            'legal proceedings', 'charters', 'constitutions', 'treaties',
            'trial proceedings', 'trials', 'indentures', 'deeds',
            'bonds (legal records', 'oaths',
        },
        'form_keywords': ['legal'],
        'topic_exact': {'law'},
        'topic_keywords': [
            'law report', 'criminal justice', 'courts',
            'ecclesiastical law', 'maritime law', 'criminal law',
            'civil procedure', 'pleading', 'canon law',
            'equity pleading', 'conveyancing',
        ],
        'title_keywords': ['act', 'statute', 'ordinance', 'trial', 'tryal', 'indictment'],
    },

    'Proclamation': {
        'form_exact': {
            'proclamations', 'thanksgiving day proclamations',
            'fast day proclamations', 'orders in council',
            'royal ordinances', 'regulations',
            'administrative regulations',
            'military regulations', 'military orders',
        },
        'form_keywords': ['proclamation'],
        'topic_exact': {'proclamations'},
        'topic_keywords': [],
        'title_keywords': ['proclamation'],
    },

    'Petition/Address': {
        'form_exact': {
            'petitions', 'petition', 'petitoins',
            'addresses', 'addresses.', 'funeral addresses',
            'fourth of july addresses', 'legislative addresses',
            "carriers' addresses", 'new year addresses',
            'thanksgiving day addresses', 'congressional addresses',
            'parliamentary addresses', 'occasional addresses',
            'academic addresses', 'parliamentary petitions',
            'royal petitions', 'fast day addresses',
        },
        'form_keywords': ['petition'],
        'topic_exact': set(),
        'topic_keywords': [],
        'title_keywords': ['petition', 'address', 'remonstrance'],
    },

    'Speech': {
        'form_exact': {
            'speeches', 'gallows speeches', 'gallows speeches y 1723',
        },
        'form_keywords': [],
        'topic_exact': set(),
        'topic_keywords': ['speeches, addresses'],
        'title_keywords': ['speech', 'oration'],
    },

    'Dialogue': {
        'form_exact': {'dialogues', 'questions and answers',
                       'question and answers', 'ouestion and answers'},
        'form_keywords': ['dialogue'],
        'topic_exact': set(),
        'topic_keywords': ['dialogues,', 'questions and answers'],
        'title_keywords': ['dialogue', 'dialogues'],
    },

    'Treatise': {
        'form_exact': {'treatises', 'discursive works'},
        'form_keywords': ['treatise'],
        'topic_exact': set(),
        'topic_keywords': [],
        'title_keywords': ['treatise', 'discourse', 'dissertation'],
    },

    'Grammar/Textbook': {
        'form_exact': {
            'grammars', 'grammar', 'textbooks', 'text books',
            'spellers', 'readers', 'alphabet books',
            'primers (instructional books', 'writing books',
            'instruction books', 'drawing books', 'copy books (penmanship',
            'exercise books (penmanship', 'penmanship manuals',
            'penmanship specimen books', 'phrase books', 'hornbooks',
            'instructional works',
        },
        'form_keywords': ['grammar', 'textbook', 'speller'],
        'topic_exact': {
            'latin language', 'english language', 'french language',
            'greek language', 'hebrew language', 'italian language',
            'rhetoric', 'elocution', 'shorthand', 'penmanship',
            'bookkeeping',
        },
        'topic_keywords': ['language and language'],
        'title_keywords': ['grammar', 'arithmetic', 'spelling'],
    },

    'Dictionary': {
        'form_exact': {
            'dictionaries', 'etymological dictionaries',
            'glossaries', 'thesauri', 'concordances',
            'encyclopedias', 'bibliographies', 'indexes',
            'gazetteers', 'gazetters',
        },
        'form_keywords': ['dictionar', 'encycloped'],
        'topic_exact': {'encyclopedias and dictionaries'},
        'topic_keywords': [],
        'title_keywords': ['dictionary', 'lexicon', 'glossary', 'thesaurus'],
    },

    'Narrative': {
        'form_exact': {
            'narratives', 'captivity narratives',
            'conversion narratives', "survivors' narratives",
            'miracle narratives',
        },
        'form_keywords': ['narrative'],
        'topic_exact': set(),
        'topic_keywords': ['indian captivities'],
        'title_keywords': ['narrative', 'account', 'relation'],
    },

    'Cookery': {
        'form_exact': {'cookbooks', 'cookery books'},
        'form_keywords': ['cookbook', 'cookery'],
        'topic_exact': {'cookery'},
        'topic_keywords': ['cookery,'],
        'title_keywords': ['cookery'],
    },

    'Medical': {
        'form_exact': {'medical formularies', 'pharmacopoeias', 'herbals'},
        'form_keywords': ['pharmacopoeia'],
        'topic_exact': {'medicine', 'surgery', 'obstetrics', 'pharmacy'},
        'topic_keywords': [
            'medicine,', 'materia medica', 'dispensatories',
            'veterinary', 'pharmacopoeia',
        ],
        'title_keywords': ['physick', 'pharmacopoeia', 'dispensatory', 'surgery', 'anatomy'],
    },

    'Map': {
        'form_exact': {
            'maps', 'atlases', 'atlases (geographic',
            'atlases (scientific', 'celestial atlases', 'pilot guides',
        },
        'form_keywords': ['atlas'],
        'topic_exact': set(),
        'topic_keywords': ['pilot guide'],
        'title_keywords': ['atlas'],
    },

    'Lecture': {
        'form_exact': {'lectures', 'lecture notes'},
        'form_keywords': ['lecture'],
        'topic_exact': {'oratory'},
        'topic_keywords': [],
        'title_keywords': ['lecture', 'lectures'],
    },

    'Advertisement': {
        'form_exact': {
            'advertisements', 'advertisements.', 'advertisments',
            'adverisements', 'advertisement',
            "booksellers' advertisements", "booksellers advertisements",
            "booksellers' asvertisements", "bookseller's advertisements",
            "publishers' advertisements", "publishers' advertisement",
            "publisher's advertisements", "printers' advertisements",
            'prospectuses', 'prospectus', 'book prospectuses',
            'company prospectuses',
        },
        'form_keywords': ['advertisement', 'prospectus'],
        'topic_exact': set(),
        'topic_keywords': [],
        'title_keywords': [],
    },

    'Juvenile': {
        'form_exact': {'juvenile literature', 'juvenilia'},
        'form_keywords': ['juvenile'],
        'topic_exact': set(),
        'topic_keywords': [],
        'title_keywords': [],
    },

    'Genealogy': {
        'form_exact': {'genealogies', 'genalogies', 'family trees', 'armorials'},
        'form_keywords': ['genealog'],
        'topic_exact': {'heraldry'},
        'topic_keywords': ['genealog'],
        'title_keywords': ['genealogy', 'pedigree'],
    },

    'Music': {
        'form_exact': {'musical works', 'scores', 'part books'},
        'form_keywords': ['musical'],
        'topic_exact': {'music', 'church music', 'concert programs'},
        'topic_keywords': ['oratorio'],
        'title_keywords': ['cantata', 'oratorio', 'sonata', 'concerto'],
    },

    'Allegory': {
        'form_exact': {
            'allegories', 'emblem books', 'parables',
            'doomsday literature',
        },
        'form_keywords': ['allegor'],
        'topic_exact': set(),
        'topic_keywords': ['allegory', 'emblem'],
        'title_keywords': ['allegory', 'parable'],
    },
}


# ── Classification functions ──────────────────────────────────────

def _match_terms(terms, field_type):
    """Match a list of terms against genre rules for the given field type."""
    genres = set()
    for genre_name, rules in GENRE_RULES.items():
        exact_set = rules.get(f'{field_type}_exact', set())
        keywords = rules.get(f'{field_type}_keywords', [])
        for term in terms:
            t = term.strip().lower().rstrip('.')
            if not t:
                continue
            if t in exact_set:
                genres.add(genre_name)
                break
            for kw in keywords:
                if kw in t:
                    genres.add(genre_name)
                    break
    return genres


def _match_title(title_text):
    genres = set()
    words = set(re.findall(r"[a-z']+", title_text.lower()))
    for genre_name, rules in GENRE_RULES.items():
        for kw in rules.get('title_keywords', []):
            if kw in words:
                genres.add(genre_name)
                break
    return genres


def classify_genres(form_terms=None, subject_terms=None, title=None, title_sub=None):
    """
    Classify an ESTC record into genre(s).

    Parameters
    ----------
    form_terms : list[str] or str or None
        655$a genre/form terms. Accepts a list of strings or a
        pipe-joined string (backward compat).
    subject_terms : list[str] or str or None
        650$a topical subject terms. Same format as form_terms.
    title : str or None
        245$a title.
    title_sub : str or None
        245$b subtitle.

    Returns
    -------
    dict with 'genres' (set of str) and 'source' ('form'|'topic'|'title'|None).
    """
    genres = set()
    source = None

    # Normalize inputs: accept pipe-joined strings or lists
    if isinstance(form_terms, str):
        form_terms = [s.strip() for s in form_terms.split('|') if s.strip()]
    elif form_terms is None:
        form_terms = []
    if isinstance(subject_terms, str):
        subject_terms = [s.strip() for s in subject_terms.split('|') if s.strip()]
    elif subject_terms is None:
        subject_terms = []

    if form_terms:
        form_genres = _match_terms(form_terms, 'form')
        if form_genres:
            genres.update(form_genres)
            source = 'form'

    if subject_terms:
        topic_genres = _match_terms(subject_terms, 'topic')
        if topic_genres:
            genres.update(topic_genres)
            if source is None:
                source = 'topic'

    if not genres:
        title_text = ''
        if title and not (isinstance(title, float) and title != title):
            title_text = str(title)
        if title_sub and not (isinstance(title_sub, float) and title_sub != title_sub):
            title_text = title_text + ' ' + str(title_sub)
        if title_text.strip():
            title_genres = _match_title(title_text)
            if title_genres:
                genres.update(title_genres)
                source = 'title'

    return {'genres': genres, 'source': source}


def get_all_genre_names():
    return sorted(GENRE_RULES.keys())


# ═══════════════════════════════════════════════════════════════════
# Fiction detection
# ═══════════════════════════════════════════════════════════════════

FICTION_GENRES = {
    'Fiction', 'Novel', 'Romance', 'Tale', 'Fable',
    'Picaresque', 'Epistolary fiction', 'Imaginary voyage',
}

def is_fiction(genres):
    """Check if a set of genres contains any fiction sub-genre."""
    if not genres:
        return False
    return bool(genres & FICTION_GENRES)


# ═══════════════════════════════════════════════════════════════════
# Genre harmonization
# ═══════════════════════════════════════════════════════════════════

_GENRE_TO_HARMONIZED = {
    # Fiction family
    'Fiction': 'Fiction', 'Novel': 'Fiction', 'Romance': 'Fiction',
    'Tale': 'Fiction', 'Fable': 'Fiction', 'Picaresque': 'Fiction',
    'Epistolary fiction': 'Fiction', 'Imaginary voyage': 'Fiction',
    # Direct mappings
    'Poetry': 'Poetry', 'Drama': 'Drama', 'Sermon': 'Sermon',
    'Essay': 'Essay', 'Letter': 'Letters', 'Almanac': 'Almanac',
    'Biography': 'Biography', 'Legal': 'Legal', 'Speech': 'Speech',
    'Periodical': 'Periodical', 'Treatise': 'Treatise',
    # ESTC-specific → broad
    'Ballad/Song': 'Poetry', 'Hymn': 'Poetry',
    'Satire': None,  # cross-cutting mode
    'Catechism': 'Nonfiction', 'Devotional': 'Nonfiction',
    'Catalogue': 'Reference', 'Dictionary': 'Reference',
    'Grammar/Textbook': 'Reference', 'Map': 'Reference',
    'History': 'History', 'Travel': 'Nonfiction',
    'Narrative': 'Nonfiction', 'Cookery': 'Nonfiction',
    'Medical': 'Nonfiction', 'Lecture': 'Nonfiction',
    'Advertisement': 'Nonfiction', 'Juvenile': None,
    'Genealogy': 'Reference', 'Music': 'Nonfiction',
    'Allegory': None, 'Dialogue': 'Nonfiction',
    'Proclamation': 'Legal',
    'Petition/Address': 'Nonfiction',
}

_HARMONIZED_PRIORITY = [
    'Sermon', 'Legal', 'Periodical', 'Almanac', 'Speech',
    'Essay', 'Letters', 'Treatise', 'Biography', 'History',
    'Reference', 'Nonfiction',
    'Drama', 'Poetry', 'Fiction',
]

def _genres_to_harmonized(genres):
    """Map a set of fine-grained genres to a single harmonized GENRE_VOCAB label."""
    harmonized = set()
    for g in genres:
        h = _GENRE_TO_HARMONIZED.get(g)
        if h:
            harmonized.add(h)
    if not harmonized:
        return None
    for h in _HARMONIZED_PRIORITY:
        if h in harmonized:
            return h
    return harmonized.pop()


# ═══════════════════════════════════════════════════════════════════
# Translation detection
# ═══════════════════════════════════════════════════════════════════

_TRANSLATION_PHRASES = [
    'translated', 'translation', 'translator',
    'rendered into', 'rendred into', "render'd into", "rendr'd into",
    'done into english', 'turn\'d into english', 'put into english',
    'englished', 'made english',
]

_TRANSLATION_LANGUAGES = {
    'french', 'italian', 'german', 'spanish', 'dutch', 'portuguese',
    'latin', 'greek', 'hebrew', 'arabic', 'persian', 'turkish',
    'swedish', 'danish', 'polish', 'russian', 'chinese', 'japanese',
    'gaelic', 'irish', 'welsh', 'scottish gaelic',
}

_TRANSLATOR_RELATORS = {'tr', 'trl', 'translator'}


def detect_translation(rec):
    """
    Detect if a parsed ESTC bib record is a translation.

    Uses three tiers of evidence:
      1. MARC structural signals (strongest):
         - 700$e relator = translator
         - 240$l uniform title language subfield
      2. Title/subtitle/notes keyword matching
      3. Subject topic foreign language indicators

    Parameters
    ----------
    rec : dict
        Parsed bib record from parse_bib_record().

    Returns
    -------
    bool
    """
    # ── Tier 1: MARC structural signals ───────────────────────────
    # Added persons with translator relator
    for p in (rec.get('added_persons') or []):
        relator = (p.get('relator') or '').strip().rstrip('.').lower()
        if relator in _TRANSLATOR_RELATORS:
            return True
        relator_code = (p.get('relator_code') or '').strip().lower()
        if relator_code in ('trl',):
            return True

    # Uniform title with language subfield = translation into that language
    ut = rec.get('uniform_title')
    if ut and isinstance(ut, dict) and ut.get('l'):
        return True

    # ── Tier 2: title/subtitle/notes keyword matching ─────────────
    text_fields = [
        rec.get('title'),
        rec.get('subtitle'),
    ]
    # Flatten notes list
    for note in (rec.get('notes') or []):
        text_fields.append(note)
    # Form terms
    for g in (rec.get('genres') or []):
        text_fields.append(g.get('term'))

    for val in text_fields:
        if not val:
            continue
        s = str(val).lower()
        for phrase in _TRANSLATION_PHRASES:
            if phrase in s:
                return True

    # ── Tier 3: subject foreign language indicators ───────────────
    for subj in (rec.get('subjects_topical') or []):
        term = (subj.get('term') or '').lower()
        for lang in _TRANSLATION_LANGUAGES:
            if lang in term:
                return True

    return False


def detect_translation_flat(title=None, title_sub=None, notes=None, form=None,
                            subject_topic=None):
    """Legacy translation detection from flat metadata columns (pipe-joined strings)."""
    for val in (title, title_sub, notes, form):
        if not val or (isinstance(val, float) and val != val):
            continue
        s = str(val).lower()
        for phrase in _TRANSLATION_PHRASES:
            if phrase in s:
                return True
    if subject_topic and not (isinstance(subject_topic, float) and subject_topic != subject_topic):
        s = str(subject_topic).lower()
        for lang in _TRANSLATION_LANGUAGES:
            if lang in s:
                return True
    return False
