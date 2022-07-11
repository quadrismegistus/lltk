import sys; sys.path.insert(0,'../../..')
from lltk.imports import *

class TextSemanticCohort(BaseText): pass

class SemanticCohort(BaseCorpus):
    NAME='SemanticCohort'
    ID='semantic_cohort'
    TEXT_CLASS=TextSemanticCohort

    def compile(self,**attrs):
        """
        This is a custom installation function. By default, it will simply try to download itself,
        unless a custom function is written here which either installs or provides installation instructions.
        """
        # Get metadata
        return self.install(parts=['metadata'])

    def load_metadata(self,*x,**y):
        """
        Magic attribute loading metadata, and doing any last minute customizing
        """
        meta=super().load_metadata()
        # ?
        return meta

