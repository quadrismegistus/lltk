"""
Per-text language detection via stopword intersection, ClickHouse-backed.

Streams freqs from lltk.text_freqs in Arrow batches, scores each text
against per-language stopword lists, writes results to lltk.text_langs.
Query-time joins expose lang_detected / lang_coverage / lang_confidence
alongside texts.
"""

import time
import pandas as pd
import pyarrow as pa

from lltk.tools.db_adapter import get_adapter


def detect_langs_clickhouse(ch_adapter, min_tokens=50,
                             coverage_threshold=0.05,
                             confidence_threshold=2.0,
                             batch_size=5000, progress=True):
    """Score every row in lltk.text_freqs, write lang_detected to lltk.text_langs.

    Returns a dict with distribution stats (lang_counts, n_texts,
    n_unknown, n_below_min_tokens).
    """
    from lltk.tools.lang_detect import function_words_table
    from lltk.tools.tools import get_tqdm

    # Ensure destination table exists
    ch_adapter.execute("""
        CREATE TABLE IF NOT EXISTS lltk.text_langs (
            _id              String,
            lang_detected    LowCardinality(String),
            lang_coverage    Float32,
            lang_confidence  Float32,
            detected_at      DateTime DEFAULT now()
        )
        ENGINE = ReplacingMergeTree(detected_at)
        ORDER BY _id
    """)

    n_freqs = ch_adapter.query("SELECT count() FROM lltk.text_freqs")[0][0]
    if n_freqs == 0:
        print('lltk.text_freqs is empty — run `lltk db-freqs` first')
        return {}

    # Invert function-word list: word → tuple of langs
    word_to_langs = {}
    for w, lg in function_words_table():
        word_to_langs.setdefault(w, []).append(lg)
    word_to_langs = {w: tuple(lgs) for w, lgs in word_to_langs.items()}
    langs = sorted({lg for lgs in word_to_langs.values() for lg in lgs})
    print(f'Detecting across {len(langs)} languages ({len(word_to_langs)} distinct stopwords)')

    # Streaming scorer. Needs a dedicated write client because the streaming
    # read holds the primary client's session open — concurrent inserts on
    # that same session raise SESSION_IS_LOCKED.
    from lltk.tools.db_adapter import get_adapter
    write_adapter = get_adapter()  # defaults to LLTK_CLICKHOUSE_URL

    results = []
    pbar = get_tqdm(total=n_freqs, desc='detect_langs',
                    unit='text', unit_scale=True) if progress else None

    with ch_adapter.client.query_arrow_stream(
        "SELECT _id, freqs FROM lltk.text_freqs"
    ) as stream:
        for arrow_batch in stream:
            ids = arrow_batch.column('_id').to_pylist()
            freqs_col = arrow_batch.column('freqs').to_pylist()
            for _id, entries in zip(ids, freqs_col):
                if not entries:
                    results.append((_id, 'null', 0.0, 0.0))
                    continue
                items = entries.items() if isinstance(entries, dict) else entries
                total_tokens = 0
                lang_hits = {}
                for w, n in items:
                    total_tokens += n
                    lgs = word_to_langs.get(w)
                    if lgs:
                        for lg in lgs:
                            lang_hits[lg] = lang_hits.get(lg, 0) + n
                if total_tokens < min_tokens:
                    results.append((_id, 'null', 0.0, 0.0))
                    continue
                if not lang_hits:
                    results.append((_id, 'unknown', 0.0, 0.0))
                    continue
                top_lang = None
                top_hits = 0
                second_hits = 0
                for lg, h in lang_hits.items():
                    if h > top_hits:
                        second_hits = top_hits
                        top_hits = h
                        top_lang = lg
                    elif h > second_hits:
                        second_hits = h
                coverage = top_hits / total_tokens
                confidence = top_hits / second_hits if second_hits > 0 else 999.0
                if coverage < coverage_threshold or confidence < confidence_threshold:
                    results.append((_id, 'unknown', coverage, confidence))
                else:
                    results.append((_id, top_lang, coverage, confidence))
            if pbar:
                pbar.update(len(ids))
            # Flush results to CH periodically to keep memory bounded
            if len(results) >= batch_size * 20:
                _flush_langs(write_adapter, results)
                results.clear()
    if pbar:
        pbar.close()

    if results:
        _flush_langs(write_adapter, results)

    # Stats
    dist = ch_adapter.query("""
        SELECT lang_detected, count()
        FROM lltk.text_langs FINAL
        GROUP BY lang_detected
        ORDER BY count() DESC
    """)
    stats = {r[0]: r[1] for r in dist}
    print('\nDistribution:')
    for lg, n in stats.items():
        print(f'  {lg}: {n:,}')
    return stats


def _flush_langs(ch_adapter, rows):
    """Insert a batch of (_id, lang_detected, lang_coverage, lang_confidence)
    into lltk.text_langs via Arrow.
    """
    if not rows:
        return
    ids = [r[0] for r in rows]
    langs = [r[1] for r in rows]
    covs = [float(r[2]) for r in rows]
    confs = [min(float(r[3]), 999.0) for r in rows]
    table = pa.Table.from_arrays(
        [pa.array(ids, type=pa.string()),
         pa.array(langs, type=pa.string()),
         pa.array(covs, type=pa.float32()),
         pa.array(confs, type=pa.float32())],
        names=['_id', 'lang_detected', 'lang_coverage', 'lang_confidence'],
    )
    ch_adapter.client.insert_arrow('text_langs', table)
