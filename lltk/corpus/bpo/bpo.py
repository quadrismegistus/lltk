from lltk.imports import *



class TextBPO(Text): pass


class BPO(Corpus):
    TEXT_CLASS=TextBPO
    
    def load_metadata(self,**kwargs):
        meta=super().load_metadata(**kwargs)
        meta['genre']=meta['ObjectType']
        return meta