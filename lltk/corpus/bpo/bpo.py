from lltk.imports import *



class TextBPO(BaseText): pass


class BPO(BaseCorpus):
    TEXT_CLASS=TextBPO
    
    BPO_GENRE_MAP = {
        'Fiction': 'Fiction',
        'Poem': 'Poetry',
        'Correspondence': 'Letters',
        'Review': 'Criticism',
        'News': 'Periodical',
    }

    def load_metadata(self,**kwargs):
        meta=super().load_metadata(**kwargs)
        meta['genre_raw'] = meta['ObjectType']
        meta['genre'] = meta['genre_raw'].map(self.BPO_GENRE_MAP)
        return meta