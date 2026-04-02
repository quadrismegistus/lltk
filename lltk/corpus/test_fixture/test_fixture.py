from lltk.imports import *

FIXTURE_PATH = os.path.dirname(os.path.abspath(__file__))

class TextTestFixture(BaseText):
    SECTION_DIR_NAME = 'chapters'

class TestFixture(BaseCorpus):
    TEXT_CLASS = TextTestFixture
    LINKS = {'test_fixture_linked': ('id', 'ref_id')}

    def load_metadata(self, *x, **y):
        meta = super().load_metadata(*x, **y)
        if 'genre' in meta.columns:
            meta['genre_raw'] = meta['genre']
            meta['genre'] = meta['genre'].str.title()  # fiction → Fiction
        return meta

    def __init__(self, **kwargs):
        kwargs['_path'] = FIXTURE_PATH
        super().__init__(**kwargs)
        # override paths that were absolutized against PATH_CORPUS by manifest loader
        self._path_metadata = os.path.join(FIXTURE_PATH, 'metadata.csv')
        self._path_txt = os.path.join(FIXTURE_PATH, 'txt')
        self._path_xml = os.path.join(FIXTURE_PATH, 'xml')
        self._path_freqs = os.path.join(FIXTURE_PATH, 'freqs')
