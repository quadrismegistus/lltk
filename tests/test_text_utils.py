"""Tests for pure utility functions in lltk.text.utils."""
import pytest
from collections import Counter


# ---------------------------------------------------------------------------
# get_idx
# ---------------------------------------------------------------------------

class TestGetIdx:
    def test_string_passthrough(self):
        from lltk.text.utils import get_idx
        assert get_idx('hello') == 'hello'

    def test_preserves_spaces(self):
        from lltk.text.utils import get_idx
        assert get_idx('hello world') == 'hello world'

    def test_preserves_plus(self):
        from lltk.text.utils import get_idx
        assert get_idx('a+b') == 'a+b'

    def test_preserves_dollar(self):
        from lltk.text.utils import get_idx
        assert get_idx('a$b') == 'a$b'

    def test_preserves_slash(self):
        from lltk.text.utils import get_idx
        assert get_idx('path/to/id') == 'path/to/id'

    def test_preserves_dash(self):
        from lltk.text.utils import get_idx
        assert get_idx('some-id') == 'some-id'

    def test_preserves_dot(self):
        from lltk.text.utils import get_idx
        assert get_idx('file.txt') == 'file.txt'

    def test_strips_disallowed_punctuation(self):
        from lltk.text.utils import get_idx
        # Characters like @ # % ^ & * are not in the allow set
        result = get_idx('hello@world')
        assert '@' not in result

    def test_int_input(self):
        from lltk.text.utils import get_idx
        result = get_idx(42)
        assert result == 'T00042'

    def test_float_input(self):
        from lltk.text.utils import get_idx
        result = get_idx(7.0)
        assert result == 'T00007'

    def test_none_generates_random(self):
        from lltk.text.utils import get_idx
        result = get_idx(None)
        assert result.startswith('T')
        assert len(result) == 6  # 'T' + 5 digits

    def test_empty_string_generates_random(self):
        from lltk.text.utils import get_idx
        result = get_idx('')
        assert result.startswith('T')


# ---------------------------------------------------------------------------
# id_is_addr / is_addr_str
# ---------------------------------------------------------------------------

class TestIdIsAddr:
    def test_valid_addr(self):
        from lltk.text.utils import id_is_addr
        assert id_is_addr('_estc/T012345') is True

    def test_no_underscore_prefix(self):
        from lltk.text.utils import id_is_addr
        assert id_is_addr('estc/T012345') is False

    def test_no_slash(self):
        from lltk.text.utils import id_is_addr
        assert id_is_addr('_estc') is False

    def test_empty_string(self):
        from lltk.text.utils import id_is_addr
        assert not id_is_addr('')

    def test_none(self):
        from lltk.text.utils import id_is_addr
        assert not id_is_addr(None)

    def test_non_string(self):
        from lltk.text.utils import id_is_addr
        assert not id_is_addr(123)

    def test_just_underscore_slash(self):
        from lltk.text.utils import id_is_addr
        assert id_is_addr('_/') is True

    def test_is_addr_str_alias(self):
        from lltk.text.utils import is_addr_str
        assert is_addr_str('_corpus/id') is True
        assert is_addr_str('plain_id') is False


# ---------------------------------------------------------------------------
# to_corpus_and_id
# ---------------------------------------------------------------------------

class TestToCorpusAndId:
    def test_valid_addr(self):
        from lltk.text.utils import to_corpus_and_id
        assert to_corpus_and_id('_estc/T012345') == ('estc', 'T012345')

    def test_plain_id(self):
        from lltk.text.utils import to_corpus_and_id
        assert to_corpus_and_id('T012345') == ('', 'T012345')

    def test_addr_with_nested_slash(self):
        from lltk.text.utils import to_corpus_and_id
        # split(IDSEP, 1) means only first slash splits corpus from id
        assert to_corpus_and_id('_hathi/mdp/39015009144422') == ('hathi', 'mdp/39015009144422')

    def test_empty_string(self):
        from lltk.text.utils import to_corpus_and_id
        assert to_corpus_and_id('') == ('', '')


# ---------------------------------------------------------------------------
# get_imsg
# ---------------------------------------------------------------------------

class TestGetImsg:
    def test_all_args(self):
        from lltk.text.utils import get_imsg
        result = get_imsg('myid', 'mycorpus', 'mysource')
        assert 'id = myid' in result
        assert 'corpus = mycorpus' in result
        assert 'source = mysource' in result

    def test_id_only(self):
        from lltk.text.utils import get_imsg
        result = get_imsg('myid')
        assert result == 'id = myid'

    def test_no_args(self):
        from lltk.text.utils import get_imsg
        assert get_imsg() == ''

    def test_none_args(self):
        from lltk.text.utils import get_imsg
        assert get_imsg(None, None, None) == ''

    def test_with_kwargs(self):
        from lltk.text.utils import get_imsg
        result = get_imsg('myid', foo='bar')
        assert 'id = myid' in result
        assert 'kwargs' in result


# ---------------------------------------------------------------------------
# is_text_obj / is_corpus_obj / is_addr_str
# ---------------------------------------------------------------------------

class TestTypeChecks:
    def test_is_text_obj_with_string(self):
        from lltk.text.utils import is_text_obj
        assert is_text_obj('hello') is False

    def test_is_text_obj_with_int(self):
        from lltk.text.utils import is_text_obj
        assert is_text_obj(42) is False

    def test_is_text_obj_with_none(self):
        from lltk.text.utils import is_text_obj
        assert is_text_obj(None) is False

    def test_is_corpus_obj_with_string(self):
        from lltk.text.utils import is_corpus_obj
        assert is_corpus_obj('hello') is False

    def test_is_corpus_obj_with_int(self):
        from lltk.text.utils import is_corpus_obj
        assert is_corpus_obj(42) is False

    def test_is_addr_str_valid(self):
        from lltk.text.utils import is_addr_str
        assert is_addr_str('_corpus/id') is True

    def test_is_addr_str_invalid(self):
        from lltk.text.utils import is_addr_str
        assert is_addr_str('not_an_addr') is False


# ---------------------------------------------------------------------------
# merge_dict
# ---------------------------------------------------------------------------

class TestMergeDict:
    def test_basic_merge(self):
        from lltk.text.utils import merge_dict
        result = merge_dict({'a': 1}, {'b': 2})
        assert result == {'a': 1, 'b': 2}

    def test_later_overrides_earlier(self):
        from lltk.text.utils import merge_dict
        result = merge_dict({'a': 1}, {'a': 2})
        assert result == {'a': 2}

    def test_three_dicts(self):
        from lltk.text.utils import merge_dict
        result = merge_dict({'a': 1}, {'b': 2}, {'c': 3})
        assert result == {'a': 1, 'b': 2, 'c': 3}

    def test_empty_dicts(self):
        from lltk.text.utils import merge_dict
        result = merge_dict({}, {})
        assert result == {}

    def test_single_dict(self):
        from lltk.text.utils import merge_dict
        result = merge_dict({'x': 10})
        assert result == {'x': 10}

    def test_no_args(self):
        from lltk.text.utils import merge_dict
        result = merge_dict()
        assert result == {}


# ---------------------------------------------------------------------------
# merge_dict_smpl
# ---------------------------------------------------------------------------

class TestMergeDictSmpl:
    def test_basic_merge(self):
        from lltk.text.utils import merge_dict_smpl
        result = merge_dict_smpl({'a': 1}, {'b': 2})
        assert result == {'a': 1, 'b': 2}

    def test_skips_none_values(self):
        from lltk.text.utils import merge_dict_smpl
        result = merge_dict_smpl({'a': None, 'b': 2})
        assert result == {'b': 2}

    def test_skips_empty_string_values(self):
        from lltk.text.utils import merge_dict_smpl
        result = merge_dict_smpl({'a': '', 'b': 'ok'})
        assert result == {'b': 'ok'}

    def test_later_overrides(self):
        from lltk.text.utils import merge_dict_smpl
        result = merge_dict_smpl({'a': 1}, {'a': 2})
        assert result == {'a': 2}

    def test_keys_become_strings(self):
        from lltk.text.utils import merge_dict_smpl
        result = merge_dict_smpl({1: 'one'})
        assert '1' in result


# ---------------------------------------------------------------------------
# merge_dict_list
# ---------------------------------------------------------------------------

class TestMergeDictList:
    def test_basic_merge(self):
        from lltk.text.utils import merge_dict_list
        result = merge_dict_list({'a': 'x'}, {'b': 'y'})
        assert result == {'a': 'x', 'b': 'y'}

    def test_duplicate_key_becomes_list(self):
        from lltk.text.utils import merge_dict_list
        result = merge_dict_list({'a': 'x'}, {'a': 'y'})
        assert result == {'a': ['x', 'y']}

    def test_list_value_joined_to_string(self):
        from lltk.text.utils import merge_dict_list
        result = merge_dict_list({'a': ['x', 'y']})
        assert result == {'a': 'x; y'}

    def test_skips_non_dict(self):
        from lltk.text.utils import merge_dict_list
        result = merge_dict_list({'a': 1}, 'not_a_dict')
        assert result == {'a': '1'}


# ---------------------------------------------------------------------------
# merge_dict_set
# ---------------------------------------------------------------------------

class TestMergeDictSet:
    def test_basic_merge(self):
        from lltk.text.utils import merge_dict_set
        result = merge_dict_set({'a': 'x'}, {'b': 'y'})
        # single-value sets unwrap to the single value
        assert result['a'] == 'x'
        assert result['b'] == 'y'

    def test_duplicate_key_merges_sets(self):
        from lltk.text.utils import merge_dict_set
        result = merge_dict_set({'a': ['x', 'y']}, {'a': ['y', 'z']})
        assert set(result['a']) == {'x', 'y', 'z'}

    def test_single_value_unwrapped(self):
        from lltk.text.utils import merge_dict_set
        result = merge_dict_set({'a': ['same']}, {'a': ['same']})
        assert result['a'] == 'same'

    def test_skips_non_dict(self):
        from lltk.text.utils import merge_dict_set
        result = merge_dict_set({'a': 'x'}, 'not_a_dict')
        assert result['a'] == 'x'


# ---------------------------------------------------------------------------
# clean_text
# ---------------------------------------------------------------------------

class TestCleanText:
    def test_html_entity_eacute(self):
        from lltk.text.utils import clean_text
        assert 'é' in clean_text('caf&eacute;')

    def test_html_entity_amp(self):
        from lltk.text.utils import clean_text
        assert '&' in clean_text('&amp;')

    def test_mdash_replacement(self):
        from lltk.text.utils import clean_text
        assert '--' in clean_text('&mdash;')

    def test_longs_replacement(self):
        from lltk.text.utils import clean_text
        result = clean_text('&longs;peak')
        assert result == 'speak'

    def test_plain_text_unchanged(self):
        from lltk.text.utils import clean_text
        result = clean_text('hello world')
        assert result == 'hello world'

    def test_unicode_mdash_replaced(self):
        from lltk.text.utils import clean_text
        result = clean_text('word—word')
        assert '--' in result

    def test_entity_without_semicolon(self):
        from lltk.text.utils import clean_text
        # &eacute without semicolon should still be caught
        result = clean_text('caf&eacute rest')
        assert 'é' in result


# ---------------------------------------------------------------------------
# tokenize / tokenize_fast / tokenize_agnostic
# ---------------------------------------------------------------------------

class TestTokenize:
    def test_tokenize_basic(self):
        from lltk.text.utils import tokenize
        result = tokenize('Hello world')
        assert 'Hello' in result
        assert 'world' in result

    def test_tokenize_strips_punctuation(self):
        from lltk.text.utils import tokenize
        result = tokenize('Hello, world!')
        assert 'Hello' in result
        assert 'world' in result
        # commas and exclamation marks should not appear as tokens
        assert ',' not in result
        assert '!' not in result

    def test_tokenize_lower(self):
        from lltk.text.utils import tokenize
        result = tokenize('Hello World', lower=True)
        assert 'hello' in result
        assert 'world' in result

    def test_tokenize_fast_basic(self):
        from lltk.text.utils import tokenize_fast
        result = tokenize_fast("It's a fine day")
        assert "It's" in result
        assert 'fine' in result
        assert 'day' in result

    def test_tokenize_fast_hyphenated(self):
        from lltk.text.utils import tokenize_fast
        result = tokenize_fast('well-known fact')
        assert 'well-known' in result
        assert 'fact' in result

    def test_tokenize_fast_empty(self):
        from lltk.text.utils import tokenize_fast
        result = tokenize_fast('')
        assert result == []

    def test_tokenize_agnostic_basic(self):
        from lltk.text.utils import tokenize_agnostic
        result = tokenize_agnostic('Hello world')
        # Should contain the word tokens
        words = [t for t in result if t.strip()]
        assert 'Hello' in words
        assert 'world' in words


# ---------------------------------------------------------------------------
# xml2txt_default
# ---------------------------------------------------------------------------

class TestXml2txtDefault:
    def test_paragraph_extraction(self):
        from lltk.text.utils import xml2txt_default
        xml = '<text><p>Hello world.</p><p>Second paragraph.</p></text>'
        result = xml2txt_default(xml)
        assert 'Hello world.' in result
        assert 'Second paragraph.' in result

    def test_line_extraction(self):
        from lltk.text.utils import xml2txt_default
        xml = '<text><l>Line one</l><l>Line two</l></text>'
        result = xml2txt_default(xml)
        assert 'Line one' in result
        assert 'Line two' in result

    def test_bad_tags_removed(self):
        from lltk.text.utils import xml2txt_default
        xml = '<text><p>Keep this.</p><note>Remove this.</note></text>'
        result = xml2txt_default(xml)
        assert 'Keep this.' in result
        assert 'Remove this.' not in result

    def test_empty_xml(self):
        from lltk.text.utils import xml2txt_default
        result = xml2txt_default('<text></text>')
        assert result == ''


# ---------------------------------------------------------------------------
# unhtml
# ---------------------------------------------------------------------------

class TestUnhtml:
    def test_strips_tags(self):
        from lltk.text.utils import unhtml
        assert unhtml('<b>bold</b>') == 'bold'

    def test_nested_tags(self):
        from lltk.text.utils import unhtml
        assert unhtml('<div><p>text</p></div>') == 'text'

    def test_no_tags(self):
        from lltk.text.utils import unhtml
        assert unhtml('plain text') == 'plain text'

    def test_empty_string(self):
        from lltk.text.utils import unhtml
        assert unhtml('') == ''

    def test_self_closing_tag(self):
        from lltk.text.utils import unhtml
        assert unhtml('before<br/>after') == 'beforeafter'


# ---------------------------------------------------------------------------
# grab_tag_text
# ---------------------------------------------------------------------------

class TestGrabTagText:
    def test_single_tag(self):
        import bs4
        from lltk.text.utils import grab_tag_text
        html = '<div><title>My Title</title><body>content</body></div>'
        dom = bs4.BeautifulSoup(html, 'html.parser')
        result = grab_tag_text(dom, 'title')
        assert result == 'My Title'

    def test_multiple_tags(self):
        import bs4
        from lltk.text.utils import grab_tag_text
        html = '<div><p>First</p><p>Second</p></div>'
        dom = bs4.BeautifulSoup(html, 'html.parser')
        result = grab_tag_text(dom, 'p')
        assert 'First' in result
        assert 'Second' in result
        assert ' || ' in result

    def test_list_of_tag_names(self):
        import bs4
        from lltk.text.utils import grab_tag_text
        html = '<div><h1>Header</h1><p>Para</p></div>'
        dom = bs4.BeautifulSoup(html, 'html.parser')
        result = grab_tag_text(dom, ['h1', 'p'])
        assert 'Header' in result
        assert 'Para' in result

    def test_missing_tag(self):
        import bs4
        from lltk.text.utils import grab_tag_text
        html = '<div><p>Only para</p></div>'
        dom = bs4.BeautifulSoup(html, 'html.parser')
        result = grab_tag_text(dom, 'span')
        assert result == ''


# ---------------------------------------------------------------------------
# remove_bad_tags
# ---------------------------------------------------------------------------

class TestRemoveBadTags:
    def test_removes_specified_tags(self):
        import bs4
        from lltk.text.utils import remove_bad_tags
        html = '<div><p>Keep</p><note>Remove</note></div>'
        dom = bs4.BeautifulSoup(html, 'html.parser')
        result = remove_bad_tags(dom, ['note'])
        assert 'Keep' in str(result)
        assert 'Remove' not in str(result)

    def test_no_tags_to_remove(self):
        import bs4
        from lltk.text.utils import remove_bad_tags
        html = '<div><p>Keep</p></div>'
        dom = bs4.BeautifulSoup(html, 'html.parser')
        result = remove_bad_tags(dom, ['note'])
        assert 'Keep' in str(result)

    def test_multiple_bad_tags(self):
        import bs4
        from lltk.text.utils import remove_bad_tags
        html = '<div><p>Keep</p><note>Gone1</note><head>Gone2</head></div>'
        dom = bs4.BeautifulSoup(html, 'html.parser')
        result = remove_bad_tags(dom, ['note', 'head'])
        assert 'Keep' in str(result)
        assert 'Gone1' not in str(result)
        assert 'Gone2' not in str(result)


# ---------------------------------------------------------------------------
# filter_freqs
# ---------------------------------------------------------------------------

class TestFilterFreqs:
    def test_lowercases_by_default(self):
        from lltk.text.utils import filter_freqs
        freqs = {'Hello': 3, 'WORLD': 2}
        result = filter_freqs(freqs)
        assert 'hello' in result
        assert 'world' in result
        assert 'Hello' not in result

    def test_no_lowercase(self):
        from lltk.text.utils import filter_freqs
        freqs = {'Hello': 3, 'WORLD': 2}
        result = filter_freqs(freqs, lower=False)
        assert 'Hello' in result
        assert 'WORLD' in result

    def test_case_merge(self):
        from lltk.text.utils import filter_freqs
        freqs = {'Hello': 3, 'hello': 2}
        result = filter_freqs(freqs)
        assert result['hello'] == 5

    def test_empty_freqs(self):
        from lltk.text.utils import filter_freqs
        result = filter_freqs({})
        assert dict(result) == {}

    def test_returns_counter(self):
        from lltk.text.utils import filter_freqs
        from collections import Counter
        result = filter_freqs({'a': 1})
        assert isinstance(result, Counter)


# ---------------------------------------------------------------------------
# get_mini_meta
# ---------------------------------------------------------------------------

class TestGetMiniMeta:
    def test_basic_extraction(self):
        from lltk.text.utils import get_mini_meta
        d = {'author': 'Austen', 'title': 'Emma', 'year': 1815}
        result = get_mini_meta(d)
        assert result.get('author') == 'Austen'
        assert result.get('title') == 'Emma'
        assert result.get('year') == 1815

    def test_prefix_matching(self):
        from lltk.text.utils import get_mini_meta
        # 'date' maps to 'year' key in MINIMETAD
        d = {'date': '1815'}
        result = get_mini_meta(d)
        assert result.get('year') == '1815'

    def test_empty_dict(self):
        from lltk.text.utils import get_mini_meta
        result = get_mini_meta({})
        assert result == {}

    def test_custom_mapd(self):
        from lltk.text.utils import get_mini_meta
        d = {'foo_bar': 'baz'}
        mapd = {'out_key': ['foo']}
        result = get_mini_meta(d, mapd=mapd)
        assert result.get('out_key') == 'baz'

    def test_falsy_values_skipped(self):
        from lltk.text.utils import get_mini_meta
        d = {'author': '', 'title': 'Emma'}
        result = get_mini_meta(d)
        assert 'author' not in result
        assert result.get('title') == 'Emma'
