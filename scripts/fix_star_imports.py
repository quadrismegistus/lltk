#!/usr/bin/env python3
"""Analyze and fix star imports from lltk.imports.

Usage:
    python scripts/fix_star_imports.py analyze lltk/corpus/chadwyck/chadwyck.py
    python scripts/fix_star_imports.py analyze --all
    python scripts/fix_star_imports.py apply lltk/corpus/chadwyck/chadwyck.py
    python scripts/fix_star_imports.py apply --batch corpus_a
"""
import ast
import sys
import os
import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
LLTK = REPO / "lltk"

# Complete set of names exported by lltk.imports (generated via dir(lltk.imports))
LLTK_IMPORTS_NAMES = {
    'ANNO_EXTS', 'AuthorBunch', 'BAD_CHAR_IDS', 'BAD_COLS', 'BAD_TAGS', 'BOOKNLP_DEFAULT_LANGUAGE',
    'BOOKNLP_DEFAULT_MODEL', 'BOOKNLP_DEFAULT_PIPELINE', 'BOOKNLP_NARRATOR_ID', 'BOOKNLP_RENAME_COLS',
    'BROKENSTATE', 'BaseCorpus', 'BaseModel', 'BaseObject', 'BaseText', 'Bunch', 'C', 'CHECKMARK',
    'COL_ADDR', 'COL_CORPUS', 'COL_ID', 'CORPUSOBJD', 'CORPUS_CACHE', 'CORPUS_FUNCS',
    'CORPUS_SOURCE_RANKS', 'CROSSMARK', 'Callable', 'Capturing', 'Corpus', 'Counter',
    'DATABOX', 'DEFAULT_COMPAREBY', 'DEFAULT_DTM_N', 'DEFAULT_MFW_N', 'DEFAULT_MFW_YEARBIN',
    'DEFAULT_NUM_PROC', 'DEFAULT_PATH_TO_MANIFEST', 'DEST_LLTK_CORPORA', 'DIR_SECTION_NAME',
    'DIR_TEXTS_NAME', 'DOWNLOAD_PART_DEFAULTS', 'Dict', 'EMPTY_GROUP', 'ENGLISH', 'HIDDEN_NOW',
    'HOME', 'HTTPError', 'IDSEP', 'IDSEP_START', 'INIT_DB_WITH_CORPORA', 'Iterable', 'Iterator',
    'KEYSERVER_URL', 'LLTK_ROOT', 'LOGGER', 'LOG_TO_FILE', 'LOG_TO_SCREEN', 'LOG_VERBOSE_JUPYTER',
    'LOG_VERBOSE_TERMINAL', 'List', 'Log', 'Logger', 'MANIFEST', 'MANIFEST_DEFAULTS',
    'MANIFEST_REQUIRED_DATA', 'MATCHRELNAME', 'MDETOK', 'META_KEYS_USED_IN_AUTO_IDX', 'META_KEY_SEP',
    'MINIMETAD', 'MODERNIZE_SPELLING', 'Mapping', 'MaybeListDict', 'MutableMapping', 'NULL_QID',
    'Namespace', 'NullText', 'Optional', 'OrderedDict', 'OrderedSetDict', 'PATH_BASE_CONF',
    'PATH_CORENLP', 'PATH_CORPUS', 'PATH_CORPUS_ZIP', 'PATH_DEFAULT_CONF', 'PATH_DEFAULT_DATA',
    'PATH_DEFAULT_LLTK_HOME', 'PATH_HERE', 'PATH_LLTK_CODE_HOME', 'PATH_LLTK_CONFIG_DIR',
    'PATH_LLTK_CONFIG_USR', 'PATH_LLTK_DATA', 'PATH_LLTK_DB', 'PATH_LLTK_DB_ENGINE',
    'PATH_LLTK_DB_FN', 'PATH_LLTK_DB_MATCHES', 'PATH_LLTK_HOME', 'PATH_LLTK_HOME_DATA',
    'PATH_LLTK_KEYS', 'PATH_LLTK_LOG_FN', 'PATH_LLTK_MATCHES', 'PATH_LLTK_REPO', 'PATH_LLTK_ZODB',
    'PATH_MANIFEST', 'PATH_MANIFESTS', 'PATH_MANIFEST_GLOBAL', 'PATH_MANIFEST_USER',
    'PATH_MANIFEST_USER_LAB', 'PATH_MANIFEST_USER_SHARE', 'PATH_TO_BOOKNLP_BINARY',
    'PATH_TO_CORPUS_CODE', 'PATH_TO_DATA_CODE', 'PATH_TO_ENGLISH_OCR_CORRECTION_RULES',
    'PATH_TO_ENGLISH_SPELLING_MODERNIZER', 'PATH_TO_ENGLISH_STOPWORDS', 'PATH_TO_ENGLISH_WORD2POS',
    'PATH_TO_ENGLISH_WORDLIST', 'PREPROC_CMDS', 'ParagraphSectionCorpus', 'PassageSectionCorpus',
    'Path', 'ProcessPoolExecutor', 'REMOTE_DEFAULT', 'REMOTE_REMOTE_DEFAULT', 'REMOTE_SOURCES',
    'ROOT', 'SOURCES', 'SectionCorpus', 'SetList', 'StringIO', 'T', 'TEXT_CACHE',
    'TEXT_META_DEFAULT', 'TEXT_PATH_KEYS', 'TMP_CORPUS_ID', 'Text', 'TextList', 'TextSection',
    'ThreadPoolExecutor', 'Tuple', 'Union', 'UserList', 'V2S', 'YEARKEYS', 'ZIP_PART_DEFAULTS',
    'ZipFile', 'analyze_as_dist', 'b64decode', 'b64encode', 'backup_fn', 'baseobj', 'bigrams',
    'camel_case_split', 'cast', 'chardata_metakeys_initial', 'check_corpora', 'check_make_dir',
    'check_make_dirs', 'check_move_file', 'check_move_link_file', 'clean_all_meta', 'clean_meta',
    'clean_text', 'cloud_list', 'compressed', 'config', 'configparser', 'copyfileobj', 'corpora',
    'corpus', 'corpus_ids', 'corpus_names', 'corr_with_cluster', 'crunch', 'csv', 'datatype',
    'datetime', 'dbget', 'defaultdict', 'deserialize', 'df_requiring_id', 'dist',
    'divide_texts_historically', 'do_gen_mfw', 'do_gen_mfw_grp', 'do_mannwhitney',
    'do_metadata_text', 'do_parse_spacy', 'do_parse_stanza', 'do_preprocess_txt', 'do_text',
    'do_to_yearbin', 'download', 'download_wget', 'ensure_dir_exists', 'ensure_snake',
    'escape_linebreaks', 'expanduser', 'extract', 'fillna', 'filter_freqs', 'fix_meta', 'freqs',
    'gen_manifest', 'get_addr', 'get_addr_from_d', 'get_addr_str', 'get_all_sources_recursive',
    'get_anno_fn_if_exists', 'get_backup_fn', 'get_config_file_location', 'get_dtm_freqs',
    'get_field2words', 'get_fields', 'get_fields0', 'get_id_str', 'get_ideal_cpu_count', 'get_idx',
    'get_idx_from_int', 'get_idx_from_meta', 'get_imsg', 'get_inducted_corpus_ids', 'get_mini_meta',
    'get_num_lines', 'get_ocr_corrections', 'get_passkey', 'get_path_abs', 'get_pkey',
    'get_prop_ish', 'get_python_path', 'get_spacy_nlp', 'get_spelling_modernizer', 'get_stanza_nlp',
    'get_text', 'get_texts', 'get_tqdm', 'get_url_or_path', 'get_user_email', 'get_user_info',
    'get_word2fields', 'get_word2pos', 'get_wordlist', 'getfreqs', 'gethtml', 'gleanPunc2',
    'grab_tag_text', 'gzip', 'header', 'hide_log', 'human_format', 'id_is_addr', 'in_jupyter',
    'index', 'induct_corpus', 'inspect', 'install', 'is_addr', 'is_addr_str', 'is_broken_obj',
    'is_corpus_obj', 'is_dictish', 'is_hashable', 'is_hashable_rly', 'is_iterable', 'is_logged_on',
    'is_text_obj', 'is_textish', 'is_valid_mfw_word', 'is_valid_text_obj', 'iter_filename',
    'iter_metadata_from_df_or_fn', 'iter_move', 'iter_texts', 'json', 'just_meta_no_id',
    'just_metadata', 'kmeans', 'ld2dd', 'ld2dl', 'ld2dld', 'linreg', 'llmap', 'load',
    'load_corpus', 'load_corpus_manifest', 'load_corpus_manifest_defaults',
    'load_corpus_manifest_unique', 'load_english', 'load_manifest', 'load_metadata',
    'load_metadata_from_df_or_fn', 'load_with_anno', 'load_with_anno_or_orig', 'log', 'log_hidden',
    'log_off', 'log_on', 'log_shown', 'logger', 'logging', 'logs', 'mask_home_dir', 'math',
    'measure_fields', 'merge_dict', 'merge_dict_list', 'merge_dict_oset', 'merge_dict_set',
    'merge_dict_smpl', 'merge_read_dfs', 'merge_read_dfs_anno', 'merge_read_dfs_dict',
    'merge_read_dfs_iter', 'meta_iter', 'meta_iter_corpora', 'meta_numlines', 'model',
    'modernize_spelling_in_txt', 'mp', 'mp_cpu_count', 'newpolyfit', 'ngram', 'nlp', 'nlpd',
    'noPunc', 'no_id', 'now', 'np', 'nx', 'orjson', 'os', 'partial', 'passages', 'pd',
    'phrase2variants', 'pickle', 'picklify', 'pmap', 'pmap_apply_cols', 'pmap_iter', 'ppath',
    'pprint', 'preprocess', 'printm', 'proc_minhash', 'product', 'quote_plus', 'random', 're',
    'read', 'read_df', 'read_df_anno', 'read_df_annos', 'read_json', 'read_ld', 'readgen',
    'readgen_csv', 'register_cell_magic', 'register_corpus_func', 'regressions', 'remove_bad_tags',
    'remove_duplicates', 'requests', 'rescale_col', 'reset_index', 'rmfn', 'rpath', 'safebool',
    'safeget', 'safejson', 'save_df', 'save_freqs_json', 'save_tokenize_text', 'serialize',
    'share', 'share_corpora', 'show', 'show_log', 'show_stats', 'showcorp', 'showcorp_readme',
    'shutil', 'skipgram_do_text', 'skipgram_do_text2', 'skipgram_save_text', 'slice', 'small_meta',
    'snake2camel', 'spellingd', 'stamp_d', 'standard2variant', 'start_new_corpus',
    'start_new_corpus_interactive', 'stats', 'status_corpora', 'status_corpora_markdown',
    'status_corpora_readme', 'stopwords', 'symlink', 'sys', 'tarfile', 'tempfile', 'text',
    'textlist', 'time', 'to_authorkey', 'to_bs64', 'to_camel_case', 'to_corpus_and_id',
    'to_corpus_objs', 'to_counts', 'to_lastname', 'to_mdw', 'to_mdw_mannwhitney',
    'to_numeric_dict', 'to_textids', 'to_tf', 'to_tfidf', 'to_titlekey', 'to_yearbin',
    'tokenize', 'tokenize_agnostic', 'tokenize_agnostic0', 'tokenize_fast', 'tokenize_nltk',
    'tools', 'tsne', 'tsv2ld', 'unhtml', 'unpicklify', 'unstamp_d', 'unzip', 'update_df',
    'upload', 'urllib', 'utils', 'variant2standard', 'warnings', 'which', 'worddb', 'write',
    'write2', 'write_ld', 'write_manifest', 'writegen', 'writegengen', 'xml2txt_default',
    'xml2txt_prose', 'xopen', 'yank', 'yield_addrs', 'yield_corpora_meta', 'yield_ids',
    'zeropunc', 'zfy', 'zipcorpus',
}

STDLIB_MODULES = {
    "os",
    "sys",
    "json",
    "random",
    "gzip",
    "time",
    "inspect",
    "pickle",
    "re",
    "configparser",
    "urllib",
    "tempfile",
    "shutil",
    "tarfile",
    "logging",
    "math",
    "warnings",
    "csv",
    "codecs",
    "statistics",
    "multiprocessing",
}

THIRD_PARTY = {
    "np": "import numpy as np",
    "pd": "import pandas as pd",
    "nx": "import networkx as nx",
    "requests": "import requests",
    "orjson": "import orjson",
}

FROM_IMPORTS = {
    "xopen": ("from xopen import xopen", {"xopen"}),
    "b64decode": (
        "from base64 import b64decode, b64encode",
        {"b64decode", "b64encode"},
    ),
    "b64encode": (
        "from base64 import b64decode, b64encode",
        {"b64decode", "b64encode"},
    ),
    "pprint": ("from pprint import pprint", {"pprint"}),
    "partial": ("from functools import partial", {"partial"}),
    "datetime": ("from datetime import datetime", {"datetime"}),
    "expanduser": ("from os.path import expanduser", {"expanduser"}),
    "Namespace": ("from argparse import Namespace", {"Namespace"}),
    "HTTPError": ("from urllib.error import HTTPError", {"HTTPError"}),
    "ZipFile": ("from zipfile import ZipFile", {"ZipFile"}),
    "Path": ("from pathlib import Path", {"Path"}),
    "quote_plus": ("from urllib.parse import quote_plus", {"quote_plus"}),
    "StringIO": ("from io import StringIO", {"StringIO"}),
}

COLLECTIONS = {"defaultdict", "Counter", "OrderedDict", "UserList"}
COLLECTIONS_ABC = {"MutableMapping"}
TYPING = {
    "Callable",
    "Dict",
    "Iterable",
    "Iterator",
    "List",
    "Mapping",
    "Optional",
    "Union",
    "cast",
    "Tuple",
}

ALL_EXTERNAL = (
    STDLIB_MODULES
    | set(THIRD_PARTY)
    | set(FROM_IMPORTS)
    | COLLECTIONS
    | COLLECTIONS_ABC
    | TYPING
)

# Python builtins that should never appear in import lists
BUILTINS = set(dir(__builtins__)) if isinstance(__builtins__, dict) else set(dir(__builtins__))


def get_used_names(filepath):
    with open(filepath) as f:
        source = f.read()
    tree = ast.parse(source)
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
            names.add(node.id)
        elif isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
            names.add(node.value.id)
    return names


def get_defined_names(filepath):
    with open(filepath) as f:
        source = f.read()
    tree = ast.parse(source)
    defined = set()
    for node in ast.iter_child_nodes(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            defined.add(node.name)
        elif isinstance(node, ast.ClassDef):
            defined.add(node.name)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    defined.add(target.id)
                elif isinstance(target, ast.Tuple):
                    for elt in target.elts:
                        if isinstance(elt, ast.Name):
                            defined.add(elt.id)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                defined.add(alias.asname or alias.name)
        elif isinstance(node, ast.ImportFrom):
            if any(alias.name == "*" for alias in node.names):
                continue
            for alias in node.names:
                defined.add(alias.asname or alias.name)
        elif isinstance(node, ast.AugAssign):
            if isinstance(node.target, ast.Name):
                defined.add(node.target.id)
        elif isinstance(node, ast.For):
            if isinstance(node.target, ast.Name):
                defined.add(node.target.id)
    return defined


def get_star_import_lines(filepath):
    results = []
    with open(filepath) as f:
        for i, line in enumerate(f, 1):
            stripped = line.strip()
            if stripped.startswith("#"):
                continue
            m = re.match(r"from\s+([\w.]+)\s+import\s+\*", stripped)
            if m:
                results.append((i, m.group(1)))
    return results


def classify_name(name):
    if name in STDLIB_MODULES:
        return "stdlib_module"
    if name in THIRD_PARTY:
        return "third_party"
    if name in FROM_IMPORTS:
        return "from_import"
    if name in COLLECTIONS:
        return "collections"
    if name in COLLECTIONS_ABC:
        return "collections_abc"
    if name in TYPING:
        return "typing"
    if name in BUILTINS:
        return "builtin"
    return "lltk_specific"


def generate_imports(needed_names):
    lines = []

    stdlib = sorted(n for n in needed_names if n in STDLIB_MODULES)
    for mod in stdlib:
        lines.append(f"import {mod}")

    tp = sorted(n for n in needed_names if n in THIRD_PARTY)
    for name in tp:
        lines.append(THIRD_PARTY[name])

    seen_from = set()
    from_names = sorted(n for n in needed_names if n in FROM_IMPORTS)
    for name in from_names:
        stmt, group = FROM_IMPORTS[name]
        if stmt not in seen_from:
            actual = sorted(group & needed_names)
            src = stmt.split("import")[0].strip()
            lines.append(f"{src} import {', '.join(actual)}")
            seen_from.add(stmt)

    coll = sorted(n for n in needed_names if n in COLLECTIONS)
    if coll:
        lines.append(f"from collections import {', '.join(coll)}")

    abc = sorted(n for n in needed_names if n in COLLECTIONS_ABC)
    if abc:
        lines.append(f"from collections.abc import {', '.join(abc)}")

    typ = sorted(n for n in needed_names if n in TYPING)
    if typ:
        lines.append(f"from typing import {', '.join(typ)}")

    lltk = sorted(n for n in needed_names if classify_name(n) == "lltk_specific")
    if lltk:
        oneliner = f"from lltk.imports import {', '.join(lltk)}"
        if len(oneliner) <= 88:
            lines.append(oneliner)
        else:
            items_block = ",\n    ".join(lltk)
            lines.append(f"from lltk.imports import (\n    {items_block},\n)")

    return lines


def analyze_file(filepath):
    used = get_used_names(filepath)
    defined = get_defined_names(filepath)
    star_lines = get_star_import_lines(filepath)

    from_star = (used - defined - BUILTINS) & LLTK_IMPORTS_NAMES
    import_lines = generate_imports(from_star)

    return {
        "file": str(filepath),
        "star_imports": [(ln, mod) for ln, mod in star_lines],
        "from_star": sorted(from_star),
        "stdlib": sorted(n for n in from_star if n in STDLIB_MODULES),
        "third_party": sorted(n for n in from_star if n in THIRD_PARTY),
        "collections": sorted(n for n in from_star if n in COLLECTIONS),
        "typing": sorted(n for n in from_star if n in TYPING),
        "lltk_specific": sorted(
            n for n in from_star if classify_name(n) == "lltk_specific"
        ),
        "import_lines": import_lines,
    }


def apply_file(filepath, dry_run=False):
    analysis = analyze_file(filepath)
    star_lines_lltk = [
        (ln, mod)
        for ln, mod in analysis["star_imports"]
        if "lltk.imports" in mod
    ]
    if not star_lines_lltk:
        print(f"  SKIP {filepath}: no lltk.imports star import")
        return False

    with open(filepath) as f:
        lines = f.readlines()

    star_line_nums = {ln for ln, _ in star_lines_lltk}
    new_lines = []
    replaced = False
    for i, line in enumerate(lines, 1):
        if i in star_line_nums and not replaced:
            for imp_line in analysis["import_lines"]:
                new_lines.append(imp_line + "\n")
            replaced = True
        elif i in star_line_nums:
            continue
        else:
            new_lines.append(line)

    if dry_run:
        print(f"\n--- {filepath} ---")
        for imp in analysis["import_lines"]:
            print(f"  {imp}")
        return True

    with open(filepath, "w") as f:
        f.writelines(new_lines)
    print(f"  FIXED {filepath}")
    return True


BATCHES = {
    "phase1": [
        "lltk/tools/logs.py",
        "lltk/tools/tools.py",
        "lltk/tools/baseobj.py",
        "lltk/tools/freqs.py",
        "lltk/tools/stats.py",
    ],
    "phase2": [
        "lltk/text/utils.py",
        "lltk/text/text.py",
        "lltk/text/textlist.py",
        "lltk/corpus/utils.py",
        "lltk/corpus/corpus.py",
        "lltk/corpus/tcp/tcp.py",
    ],
    "corpus_a": [
        "lltk/corpus/chadwyck/chadwyck.py",
        "lltk/corpus/chadwyck_drama/chadwyck_drama.py",
        "lltk/corpus/earlyprint/earlyprint.py",
        "lltk/corpus/eebo_tcp/eebo_tcp.py",
        "lltk/corpus/ecco_tcp/ecco_tcp.py",
        "lltk/corpus/evans_tcp/evans_tcp.py",
        "lltk/corpus/ecco/ecco.py",
        "lltk/corpus/end/end.py",
        "lltk/corpus/estc/estc.py",
        "lltk/corpus/fiction_biblio/fiction_biblio.py",
    ],
    "corpus_b": [
        "lltk/corpus/hathi/hathi.py",
        "lltk/corpus/hathi_englit/hathi_englit.py",
        "lltk/corpus/internet_archive/internet_archive.py",
        "lltk/corpus/blbooks/blbooks.py",
        "lltk/corpus/bpo/bpo.py",
        "lltk/corpus/oldbailey/oldbailey.py",
        "lltk/corpus/gale_amfic/gale_amfic.py",
        "lltk/corpus/litlab/litlab.py",
        "lltk/corpus/chicago/chicago.py",
        "lltk/corpus/markmark/markmark.py",
    ],
    "corpus_c": [
        "lltk/corpus/artfl/artfl.py",
        "lltk/corpus/gallica_literary_fictions/gallica_literary_fictions.py",
        "lltk/corpus/french_pd_books/french_pd_books.py",
        "lltk/corpus/paige/paige.py",
        "lltk/corpus/dta/dta.py",
        "lltk/corpus/german_fiction/german_fiction.py",
        "lltk/corpus/german_pd/german_pd.py",
        "lltk/corpus/de_corp/de_corp.py",
        "lltk/corpus/impact_es/impact_es.py",
        "lltk/corpus/spanish_pd_books/spanish_pd_books.py",
        "lltk/corpus/coha/coha.py",
    ],
    "corpus_d": [
        "lltk/corpus/clmet/clmet.py",
        "lltk/corpus/dialogues/dialogues.py",
        "lltk/corpus/epistolary/epistolary.py",
        "lltk/corpus/gildedage/gildedage.py",
        "lltk/corpus/long_arc_prestige/long_arc_prestige.py",
        "lltk/corpus/ravengarside/ravengarside.py",
        "lltk/corpus/semantic_cohort/semantic_cohort.py",
        "lltk/corpus/sotu/sotu.py",
        "lltk/corpus/spectator/spectator.py",
        "lltk/corpus/tedjdh/tedjdh.py",
        "lltk/corpus/txtlab/txtlab.py",
        "lltk/corpus/canon_fiction/canon_fiction.py",
        "lltk/corpus/test_fixture/test_fixture.py",
        "lltk/corpus/test_fixture_linked/test_fixture_linked.py",
        "lltk/corpus/default/new_corpus.py",
    ],
    "model": [
        "lltk/model/model.py",
        "lltk/model/booknlp.py",
        "lltk/model/characters.py",
        "lltk/model/charnet.py",
        "lltk/model/classifier.py",
        "lltk/model/networks.py",
        "lltk/model/ner.py",
        "lltk/model/preprocess.py",
        "lltk/model/word2vec.py",
    ],
}


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Analyze and fix star imports")
    parser.add_argument("action", choices=["analyze", "apply"])
    parser.add_argument("target", nargs="?", help="file path")
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--batch", type=str)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    os.chdir(REPO)

    if args.all:
        files = []
        for batch in BATCHES.values():
            files.extend(batch)
    elif args.batch:
        files = BATCHES.get(args.batch, [])
        if not files:
            print(f"Unknown batch: {args.batch}. Available: {list(BATCHES.keys())}")
            sys.exit(1)
    elif args.target:
        files = [args.target]
    else:
        parser.print_help()
        sys.exit(1)

    for f in files:
        if args.action == "analyze":
            result = analyze_file(f)
            print(f"\n=== {f} ===")
            print(f"  Star imports: {result['star_imports']}")
            print(f"  stdlib: {result['stdlib']}")
            print(f"  third_party: {result['third_party']}")
            print(f"  lltk_specific: {result['lltk_specific']}")
            print(f"  Replacement:")
            for line in result["import_lines"]:
                print(f"    {line}")
        else:
            apply_file(f, dry_run=args.dry_run)
