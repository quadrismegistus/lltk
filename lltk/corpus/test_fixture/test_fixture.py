from lltk.imports import *

FIXTURE_PATH = os.path.dirname(os.path.abspath(__file__))

class TextTestFixture(BaseText):
    pass

class TestFixture(BaseCorpus):
    TEXT_CLASS = TextTestFixture

    def __init__(self, **kwargs):
        kwargs['_path'] = FIXTURE_PATH
        super().__init__(**kwargs)
        # override paths that were absolutized against PATH_CORPUS by manifest loader
        self._path_metadata = os.path.join(FIXTURE_PATH, 'metadata.csv')
        self._path_txt = os.path.join(FIXTURE_PATH, 'txt')
        self._path_freqs = os.path.join(FIXTURE_PATH, 'freqs')
