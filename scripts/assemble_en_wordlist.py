#!/usr/bin/env python3
"""Assemble a comprehensive English wordlist from multiple sources.

Sources:
  1. MorphAdorner spelling variants (358K old->modern pairs — both sides)
  2. OED headwords (264K, extracted as first whitespace-delimited token)
  3. aspell.net wordlist (734K with caps)
  4. Hand-transcribed corpora via CH text_words table:
     eebo_tcp, ecco_tcp, earlyprint, chadwyck, chadwyck_poetry, chadwyck_drama

Output: data/wordlist_en.txt (one word per line, sorted, lowercased)

Usage:
    python scripts/assemble_en_wordlist.py
    python scripts/assemble_en_wordlist.py --no-ch   # skip ClickHouse query
"""

import argparse
import gzip
import json
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_DIR = os.path.dirname(SCRIPT_DIR)
OUTPUT_PATH = os.path.join(REPO_DIR, 'data', 'wordlist_en.txt')

MORPHADORNER_PATH = os.path.join(REPO_DIR, 'data', 'default',
                                  'spelling_variants_from_morphadorner.txt.gz')
OED_PATH = os.path.join(REPO_DIR, 'data', 'raw', 'oed_words.json')
ASPELL_PATH = os.path.join(REPO_DIR, 'data', 'default',
                            'wordlist.aspell.net.with_caps.txt.gz')

TRANSCRIBED_CORPORA = [
    'eebo_tcp', 'ecco_tcp', 'earlyprint',
    'chadwyck', 'chadwyck_poetry', 'chadwyck_drama',
]

MIN_WORD_LEN = 2
MIN_DOC_FREQ = 2  # word must appear in >=2 texts to be included from CH


def load_morphadorner(path):
    words = set()
    if not os.path.exists(path):
        print(f'  [skip] {path} not found')
        return words
    with gzip.open(path, 'rt') as f:
        for ln in f:
            ln = ln.strip()
            if not ln:
                continue
            parts = ln.split('\t')
            if len(parts) == 2:
                words.add(parts[0].strip().lower())
                words.add(parts[1].strip().lower())
    print(f'  MorphAdorner: {len(words):,} words')
    return words


def load_oed(path):
    words = set()
    if not os.path.exists(path):
        print(f'  [skip] {path} not found')
        return words
    with open(path) as f:
        data = json.load(f)
    for entry in data:
        token = entry.strip().split()[0].strip() if entry.strip() else ''
        if token and len(token) >= MIN_WORD_LEN:
            words.add(token.lower())
    print(f'  OED: {len(words):,} words')
    return words


def load_aspell(path):
    words = set()
    if not os.path.exists(path):
        print(f'  [skip] {path} not found')
        return words
    with gzip.open(path, 'rt') as f:
        for ln in f:
            w = ln.strip()
            w = w.strip()
            if w and len(w) >= MIN_WORD_LEN:
                words.add(w.lower())
    print(f'  aspell: {len(words):,} words')
    return words


def load_ch_corpora(corpora, min_doc_freq=MIN_DOC_FREQ):
    words = set()
    try:
        import lltk
        adapter = lltk.db.adapter
    except Exception as e:
        print(f'  [skip] ClickHouse not available: {e}')
        return words

    corpora_sql = ', '.join(f"'{c}'" for c in corpora)
    print(f'  Querying text_words for {len(corpora)} corpora (min_doc_freq={min_doc_freq})...')
    df = adapter.query_df(f"""
        SELECT word, count(DISTINCT _id) AS doc_freq
        FROM lltk.text_words
        WHERE corpus IN ({corpora_sql})
        GROUP BY word
        HAVING doc_freq >= {min_doc_freq}
          AND length(word) >= {MIN_WORD_LEN}
    """)
    for w in df['word']:
        words.add(w.strip().lower())
    print(f'  CH transcribed corpora: {len(words):,} words')
    return words


def main():
    parser = argparse.ArgumentParser(description='Assemble English wordlist')
    parser.add_argument('--no-ch', action='store_true',
                        help='Skip ClickHouse query for transcribed corpora')
    parser.add_argument('-o', '--output', default=OUTPUT_PATH)
    args = parser.parse_args()

    words = set()

    print('Loading sources:')
    words |= load_morphadorner(MORPHADORNER_PATH)
    words |= load_oed(OED_PATH)
    words |= load_aspell(ASPELL_PATH)

    if not args.no_ch:
        words |= load_ch_corpora(TRANSCRIBED_CORPORA)

    # filter: only alphabetic (allows hyphens and apostrophes)
    filtered = {w for w in words if len(w) >= MIN_WORD_LEN and any(c.isalpha() for c in w)}

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, 'w') as f:
        for w in sorted(filtered):
            f.write(w + '\n')

    print(f'\nWritten {len(filtered):,} words to {args.output}')


if __name__ == '__main__':
    main()
