"""Tests for pure utility functions in lltk.tools.tools."""

import json
import math
import os
import sys

import numpy as np
import pandas as pd
import pytest

from lltk.tools.tools import (
    Bunch,
    Capturing,
    OrderedSetDict,
    SetList,
    backup_fn,
    camel_case_split,
    ensure_dir_exists,
    ensure_snake,
    fillna,
    human_format,
    is_dictish,
    is_hashable,
    is_iterable,
    iter_filename,
    modernize_spelling_in_txt,
    ngram,
    noPunc,
    pmap,
    ppath,
    read,
    read_df,
    read_json,
    remove_duplicates,
    rmfn,
    rpath,
    safebool,
    save_df,
    snake2camel,
    to_camel_case,
    zeropunc,
)


# ---------------------------------------------------------------------------
# ngram
# ---------------------------------------------------------------------------
class TestNgram:
    def test_trigrams(self):
        assert ngram([1, 2, 3, 4], n=3) == [(1, 2, 3), (2, 3, 4)]

    def test_bigrams(self):
        assert ngram([1, 2, 3], n=2) == [(1, 2), (2, 3)]

    def test_input_shorter_than_n(self):
        assert ngram([1, 2], n=3) == []

    def test_exact_length(self):
        assert ngram([1, 2, 3], n=3) == [(1, 2, 3)]

    def test_empty_input(self):
        assert ngram([], n=3) == []

    def test_unigrams(self):
        assert ngram(["a", "b", "c"], n=1) == [("a",), ("b",), ("c",)]

    def test_strings(self):
        assert ngram(list("abcd"), n=2) == [("a", "b"), ("b", "c"), ("c", "d")]


# ---------------------------------------------------------------------------
# modernize_spelling_in_txt
# ---------------------------------------------------------------------------
class TestModernizeSpelling:
    def test_with_custom_dict(self):
        d = {"loue": "love", "haue": "have"}
        assert modernize_spelling_in_txt("I loue and haue", spelling_d=d) == "I love and have"

    def test_preserves_punctuation(self):
        d = {"olde": "old"}
        result = modernize_spelling_in_txt('"olde" world', spelling_d=d)
        assert '"old" world' == result

    def test_multiline(self):
        d = {"ye": "the"}
        result = modernize_spelling_in_txt("ye first\nye second", spelling_d=d)
        assert result == "the first\nthe second"

    def test_no_matching_words_noop(self):
        d = {"xyz_never_match": "replaced"}
        txt = "some text here"
        assert modernize_spelling_in_txt(txt, spelling_d=d) == txt


# ---------------------------------------------------------------------------
# iter_filename
# ---------------------------------------------------------------------------
class TestIterFilename:
    def test_nonexistent_returns_same(self, tmp_path):
        fn = str(tmp_path / "new.txt")
        assert iter_filename(fn) == fn

    def test_existing_file_gets_number(self, tmp_path):
        fn = str(tmp_path / "file.txt")
        open(fn, "w").close()
        result = iter_filename(fn)
        assert result == str(tmp_path / "file2.txt")

    def test_force_returns_original_name(self, tmp_path):
        fn = str(tmp_path / "file.txt")
        # file does not exist but force=True
        result = iter_filename(fn, force=True)
        assert result == fn

    def test_with_prefix(self, tmp_path):
        fn = str(tmp_path / "file.txt")
        open(fn, "w").close()
        result = iter_filename(fn, prefix="bak_")
        # prefix applied: bak_file.txt (no collision, so no number)
        assert result == str(tmp_path / "bak_file.txt")

    def test_increments_past_collisions(self, tmp_path):
        base = str(tmp_path / "file.txt")
        open(base, "w").close()
        open(str(tmp_path / "file2.txt"), "w").close()
        result = iter_filename(base)
        assert result == str(tmp_path / "file3.txt")


# ---------------------------------------------------------------------------
# safebool
# ---------------------------------------------------------------------------
class TestSafebool:
    def test_nan_is_false(self):
        assert safebool(np.nan) is False

    def test_empty_string_is_false(self):
        assert safebool("") is False

    def test_nonempty_string_is_true(self):
        assert safebool("hello") is True

    def test_zero_is_false(self):
        assert safebool(0) is False

    def test_positive_int_is_true(self):
        assert safebool(42) is True

    def test_empty_list_is_false(self):
        assert safebool([]) is False

    def test_nonempty_list_is_true(self):
        assert safebool([1]) is True

    def test_none_is_false(self):
        assert safebool(None) is False

    def test_dict_filters_bad_keys_and_values(self):
        result = safebool({np.nan: 1, "ok": np.nan, "good": "val"})
        assert result == {"good": "val"}


# ---------------------------------------------------------------------------
# is_hashable / is_dictish / is_iterable
# ---------------------------------------------------------------------------
class TestTypeChecks:
    def test_hashable_string(self):
        assert is_hashable("hello") is True

    def test_hashable_int(self):
        assert is_hashable(42) is True

    def test_not_hashable_list(self):
        assert is_hashable([1, 2]) is False

    def test_not_hashable_dict(self):
        assert is_hashable({"a": 1}) is False

    def test_dictish_dict(self):
        assert is_dictish({"a": 1}) is True

    def test_not_dictish_list(self):
        assert is_dictish([1, 2]) is False

    def test_iterable_list(self):
        assert is_iterable([1, 2]) is True

    def test_iterable_string(self):
        assert is_iterable("abc") is True

    def test_not_iterable_int(self):
        assert is_iterable(42) is False

    def test_none_not_iterable(self):
        assert is_iterable(None) is False


# ---------------------------------------------------------------------------
# SetList
# ---------------------------------------------------------------------------
class TestSetList:
    def test_deduplicates(self):
        sl = SetList([1, 2, 2, 3, 1])
        assert list(sl) == [1, 2, 3]

    def test_append_duplicate_ignored(self):
        sl = SetList([1, 2])
        sl.append(2)
        assert list(sl) == [1, 2]

    def test_append_new(self):
        sl = SetList([1])
        sl.append(2)
        assert list(sl) == [1, 2]

    def test_extend(self):
        sl = SetList([1])
        sl.extend([2, 3, 1])
        assert list(sl) == [1, 2, 3]

    def test_iadd(self):
        sl = SetList([1])
        sl += [2, 3]
        assert list(sl) == [1, 2, 3]

    def test_remove(self):
        sl = SetList([1, 2, 3])
        sl.remove(2)
        assert list(sl) == [1, 3]

    def test_remove_nonexistent_silent(self):
        sl = SetList([1, 2])
        sl.remove(99)  # should not raise
        assert list(sl) == [1, 2]

    def test_append_list_flattens(self):
        sl = SetList()
        sl.append([1, 2, 3])
        assert list(sl) == [1, 2, 3]

    def test_repr(self):
        sl = SetList([1, 2])
        assert repr(sl) == "[1, 2]"

    def test_len(self):
        sl = SetList([1, 2, 2, 3])
        assert len(sl) == 3


# ---------------------------------------------------------------------------
# OrderedSetDict
# ---------------------------------------------------------------------------
class TestOrderedSetDict:
    def test_basic_set_get(self):
        osd = OrderedSetDict()
        osd["key"] = "a"
        osd["key"] = "b"
        osd["key"] = "a"  # duplicate
        assert osd["key"] == ["a", "b"]

    def test_len(self):
        osd = OrderedSetDict()
        osd["a"] = 1
        osd["b"] = 2
        assert len(osd) == 2

    def test_delitem(self):
        osd = OrderedSetDict()
        osd["a"] = 1
        del osd["a"]
        assert len(osd) == 0

    def test_iter(self):
        osd = OrderedSetDict()
        osd["x"] = 1
        osd["y"] = 2
        assert list(osd) == ["x", "y"]

    def test_to_dict_single_unwrap(self):
        osd = OrderedSetDict()
        osd["a"] = 1
        osd["b"] = [2, 3]
        d = osd.to_dict()
        assert d["a"] == 1
        assert d["b"] == [2, 3]

    def test_set_with_list(self):
        osd = OrderedSetDict()
        osd["k"] = [1, 2, 3, 2]
        assert osd["k"] == [1, 2, 3]


# ---------------------------------------------------------------------------
# Bunch
# ---------------------------------------------------------------------------
class TestBunch:
    def test_attr_access(self):
        b = Bunch(x=1, y=2)
        assert b.x == 1
        assert b.y == 2

    def test_missing_attr_returns_empty_string(self):
        b = Bunch()
        assert b.nonexistent == ""

    def test_setattr(self):
        b = Bunch()
        b.foo = "bar"
        assert b.foo == "bar"

    def test_iter(self):
        b = Bunch(a=1, b=2)
        assert set(b) == {1, 2}


# ---------------------------------------------------------------------------
# Capturing (stdout capture context manager)
# ---------------------------------------------------------------------------
class TestCapturing:
    def test_captures_print(self):
        with Capturing() as output:
            print("hello")
            print("world")
        assert output == ["hello", "world"]

    def test_restores_stdout(self):
        original = sys.stdout
        with Capturing():
            pass
        assert sys.stdout is original

    def test_empty_capture(self):
        with Capturing() as output:
            pass
        assert output == []


# ---------------------------------------------------------------------------
# fillna
# ---------------------------------------------------------------------------
class TestFillna:
    def test_replaces_nan(self):
        assert fillna(np.nan, "default") == "default"

    def test_preserves_value(self):
        assert fillna(42, "default") == 42

    def test_preserves_string(self):
        assert fillna("hello", "default") == "hello"

    def test_default_replacement_is_empty_string(self):
        assert fillna(np.nan) == ""

    def test_none_not_replaced(self):
        # np.isnan(None) raises TypeError, so fillna returns None
        assert fillna(None, "default") is None


# ---------------------------------------------------------------------------
# snake2camel / to_camel_case / ensure_snake
# ---------------------------------------------------------------------------
class TestCaseConversion:
    def test_snake2camel(self):
        assert snake2camel("hello_world") == "HelloWorld"

    def test_snake2camel_single(self):
        assert snake2camel("word") == "Word"

    def test_to_camel_case(self):
        assert to_camel_case("hello world") == "HelloWorld"

    def test_to_camel_case_single(self):
        assert to_camel_case("hello") == "Hello"

    def test_ensure_snake_basic(self):
        assert ensure_snake("Hello World") == "hello_world"

    def test_ensure_snake_strips_punc(self):
        assert ensure_snake("Hello! World?") == "hello_world"

    def test_ensure_snake_no_double_underscore(self):
        # META_KEY_SEP is '__', ensure_snake collapses it
        assert "__" not in ensure_snake("a__b")

    def test_ensure_snake_preserves_underscores(self):
        assert ensure_snake("some_name") == "some_name"


# ---------------------------------------------------------------------------
# zeropunc / noPunc
# ---------------------------------------------------------------------------
class TestPuncStripping:
    def test_zeropunc_removes_all(self):
        assert zeropunc("hello!") == "hello"

    def test_zeropunc_allows_specified(self):
        assert zeropunc("hello-world!", allow={"-"}) == "hello-world"

    def test_zeropunc_empty_string(self):
        assert zeropunc("") == ""

    def test_zeropunc_only_punc(self):
        assert zeropunc("!@#$") == ""

    def test_noPunc_strips_leading_trailing(self):
        assert noPunc("...hello...") == "hello"

    def test_noPunc_preserves_internal(self):
        assert noPunc("it's") == "it's"

    def test_noPunc_empty(self):
        assert noPunc("") == ""


# ---------------------------------------------------------------------------
# camel_case_split
# ---------------------------------------------------------------------------
class TestCamelCaseSplit:
    def test_basic(self):
        assert camel_case_split("camelCase") == ["camel", "Case"]

    def test_multiple_words(self):
        assert camel_case_split("CamelCaseSplit") == ["Camel", "Case", "Split"]

    def test_single_word_lower(self):
        assert camel_case_split("word") == ["word"]

    def test_single_word_upper(self):
        assert camel_case_split("Word") == ["Word"]

    def test_all_upper_stays_together(self):
        assert camel_case_split("URL") == ["URL"]


# ---------------------------------------------------------------------------
# human_format
# ---------------------------------------------------------------------------
class TestHumanFormat:
    def test_small_number(self):
        assert human_format(42) == "42"

    def test_thousands(self):
        assert human_format(1500) == "2K"

    def test_exact_thousand(self):
        assert human_format(1000) == "1K"

    def test_millions(self):
        assert human_format(1_500_000) == "2M"

    def test_exact_million(self):
        assert human_format(1_000_000) == "1M"

    def test_billions(self):
        assert human_format(1_000_000_000) == "1B"

    def test_zero(self):
        assert human_format(0) == "0"

    def test_999(self):
        assert human_format(999) == "999"


# ---------------------------------------------------------------------------
# remove_duplicates
# ---------------------------------------------------------------------------
class TestRemoveDuplicates:
    def test_basic(self):
        assert remove_duplicates([1, 2, 2, 3, 1]) == [1, 2, 3]

    def test_preserves_order(self):
        assert remove_duplicates([3, 1, 2, 1, 3]) == [3, 1, 2]

    def test_empty(self):
        assert remove_duplicates([]) == []

    def test_no_duplicates(self):
        assert remove_duplicates([1, 2, 3]) == [1, 2, 3]

    def test_strings(self):
        assert remove_duplicates(["a", "b", "a"]) == ["a", "b"]

    def test_remove_empty(self):
        assert remove_duplicates(["a", "", "b", ""], remove_empty=True) == ["a", "b"]


# ---------------------------------------------------------------------------
# pmap (sequential mode only, num_proc=1)
# ---------------------------------------------------------------------------
class TestPmap:
    def test_basic_sequential(self):
        result = pmap(lambda x: x * 2, [1, 2, 3], num_proc=1, progress=False)
        assert result == [2, 4, 6]

    def test_empty_input(self):
        result = pmap(lambda x: x, [], num_proc=1, progress=False)
        assert result == []

    def test_with_extra_args(self):
        def add(x, y):
            return x + y
        result = pmap(add, [1, 2, 3], args=(10,), num_proc=1, progress=False)
        assert result == [11, 12, 13]

    def test_with_kwargs(self):
        def multiply(x, factor=1):
            return x * factor
        result = pmap(multiply, [1, 2, 3], kwargs={"factor": 5}, num_proc=1, progress=False)
        assert result == [5, 10, 15]


# ---------------------------------------------------------------------------
# read_json (uses tmp_path)
# ---------------------------------------------------------------------------
class TestReadJson:
    def test_reads_json_file(self, tmp_path):
        fn = str(tmp_path / "data.json")
        data = {"key": "value", "num": 42}
        with open(fn, "w") as f:
            json.dump(data, f)
        result = read_json(fn)
        assert result == data

    def test_nonexistent_returns_empty_dict(self, tmp_path):
        fn = str(tmp_path / "missing.json")
        assert read_json(fn) == {}

    def test_reads_list(self, tmp_path):
        fn = str(tmp_path / "list.json")
        data = [1, 2, 3]
        with open(fn, "w") as f:
            json.dump(data, f)
        assert read_json(fn) == data


# ---------------------------------------------------------------------------
# read (file reading)
# ---------------------------------------------------------------------------
class TestRead:
    def test_reads_text_file(self, tmp_path):
        fn = str(tmp_path / "sample.txt")
        with open(fn, "w") as f:
            f.write("hello world")
        assert read(fn) == "hello world"

    def test_nonexistent_returns_empty(self, tmp_path):
        fn = str(tmp_path / "nope.txt")
        result = read(fn)
        assert result == ""

    def test_reads_gz_file(self, tmp_path):
        import gzip
        fn = str(tmp_path / "sample.txt.gz")
        with gzip.open(fn, "wt", encoding="utf-8") as f:
            f.write("compressed text")
        assert read(fn) == "compressed text"


# ---------------------------------------------------------------------------
# ensure_dir_exists
# ---------------------------------------------------------------------------
class TestEnsureDirExists:
    def test_creates_parent_of_filepath(self, tmp_path):
        # ensure_dir_exists always infers fn=True (splitext returns tuple != str),
        # so it creates the parent directory of the given path.
        fn = str(tmp_path / "subdir" / "file.txt")
        ensure_dir_exists(fn)
        assert os.path.isdir(str(tmp_path / "subdir"))

    def test_explicit_fn_true_creates_parent(self, tmp_path):
        fn = str(tmp_path / "deep" / "nested" / "file.txt")
        ensure_dir_exists(fn, fn=True)
        assert os.path.isdir(str(tmp_path / "deep" / "nested"))

    def test_explicit_fn_false_creates_path_itself(self, tmp_path):
        target = str(tmp_path / "new" / "dir")
        ensure_dir_exists(target, fn=False)
        assert os.path.isdir(target)

    def test_existing_dir_ok(self, tmp_path):
        target = str(tmp_path / "existing")
        os.makedirs(target)
        ensure_dir_exists(target, fn=False)  # should not raise
        assert os.path.isdir(target)

    def test_empty_path_returns_empty(self):
        result = ensure_dir_exists("")
        assert result == ""


# ---------------------------------------------------------------------------
# rmfn (file removal)
# ---------------------------------------------------------------------------
class TestRmfn:
    def test_removes_existing_file(self, tmp_path):
        fn = str(tmp_path / "deleteme.txt")
        with open(fn, "w") as f:
            f.write("bye")
        rmfn(fn)
        assert not os.path.exists(fn)

    def test_nonexistent_file_no_error(self, tmp_path):
        fn = str(tmp_path / "nope.txt")
        rmfn(fn)  # should not raise


# ---------------------------------------------------------------------------
# backup_fn
# ---------------------------------------------------------------------------
class TestBackupFn:
    def test_copies_file(self, tmp_path):
        fn = str(tmp_path / "original.txt")
        with open(fn, "w") as f:
            f.write("content")
        backup_fn(fn)
        bak = str(tmp_path / "original.bak.txt")
        assert os.path.exists(bak)
        assert os.path.exists(fn)  # original still exists (copy mode)
        with open(bak) as f:
            assert f.read() == "content"

    def test_nonexistent_no_error(self, tmp_path):
        fn = str(tmp_path / "missing.txt")
        backup_fn(fn)  # should not raise

    def test_move_mode(self, tmp_path):
        fn = str(tmp_path / "moveme.txt")
        with open(fn, "w") as f:
            f.write("data")
        backup_fn(fn, copy=False, move=True)
        bak = str(tmp_path / "moveme.bak.txt")
        assert os.path.exists(bak)
        assert not os.path.exists(fn)  # original moved


# ---------------------------------------------------------------------------
# read_df / save_df (CSV roundtrip)
# ---------------------------------------------------------------------------
class TestDataFrameIO:
    def test_csv_roundtrip(self, tmp_path):
        fn = str(tmp_path / "data.csv")
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        save_df(df, fn)
        result = read_df(fn)
        assert list(result.columns) == ["a", "b"]
        assert len(result) == 3
        assert list(result["a"]) == [1, 2, 3]

    def test_tsv_roundtrip(self, tmp_path):
        fn = str(tmp_path / "data.tsv")
        df = pd.DataFrame({"col1": [10, 20]})
        save_df(df, fn)
        result = read_df(fn)
        assert len(result) == 2

    def test_read_df_nonexistent_returns_none(self, tmp_path):
        fn = str(tmp_path / "nope.csv")
        assert read_df(fn) is None

    def test_save_creates_parent_dirs(self, tmp_path):
        fn = str(tmp_path / "sub" / "dir" / "data.csv")
        df = pd.DataFrame({"a": [1]})
        save_df(df, fn)
        assert os.path.exists(fn)


# ---------------------------------------------------------------------------
# ppath / rpath  (home dir masking/unmasking)
# ---------------------------------------------------------------------------
class TestPpathRpath:
    def test_ppath_masks_home(self):
        home = os.path.expanduser("~")
        assert ppath(home + "/some/path") == "~/some/path"

    def test_rpath_expands_tilde(self):
        home = os.path.expanduser("~")
        assert rpath("~/some/path") == home + "/some/path"

    def test_roundtrip(self):
        home = os.path.expanduser("~")
        original = home + "/test/path"
        assert rpath(ppath(original)) == original

    def test_ppath_no_home_prefix(self):
        assert ppath("/tmp/other") == "/tmp/other"

    def test_rpath_no_tilde(self):
        assert rpath("/absolute/path") == "/absolute/path"
