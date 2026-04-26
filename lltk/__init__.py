from .imports import *
from .db import db


def task_path(_id: str, task_name: str) -> str:
    """Resolve corpus-local task directory without instantiating Text/Corpus.

    >>> lltk.task_path('_chadwyck/ncf0204.01', 'social_network')
    '~/lltk_data/corpora/chadwyck/tasks/social_network/ncf0204.01'
    """
    if _id.startswith('_'):
        _id = _id[1:]
    corpus_id, text_id = _id.split('/', 1)
    return os.path.join(PATH_CORPUS, corpus_id, 'tasks', task_name, text_id)