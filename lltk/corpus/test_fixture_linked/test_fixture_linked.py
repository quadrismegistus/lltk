from lltk.imports import *

FIXTURE_PATH = os.path.dirname(os.path.abspath(__file__))

class TextTestFixtureLinked(BaseText):
    pass

class TestFixtureLinked(BaseCorpus):
    TEXT_CLASS = TextTestFixtureLinked
    LINKS = {'test_fixture': ('ref_id', 'id')}

    def __init__(self, **kwargs):
        kwargs['_path'] = FIXTURE_PATH
        super().__init__(**kwargs)
        self._path_metadata = os.path.join(FIXTURE_PATH, 'metadata.csv')
        self._path_txt = os.path.join(FIXTURE_PATH, 'txt')

    def load_metadata(self):
        meta = super().load_metadata()
        if not len(meta):
            return meta
        return self.merge_linked_metadata(meta)
