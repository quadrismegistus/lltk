"""Per-text OCR accuracy scoring via text_freqs + wordlist JOIN.

Loads a reference wordlist into a tmp Memory table, then for each text
in text_freqs, computes the fraction of tokens whose word type appears
in the wordlist. Writes results to lltk.text_ocr (ReplacingMergeTree).
"""

import os
import time

from logmap import logmap

from lltk.db.adapter import ch_quote

WORDLIST_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    'data', 'wordlist_en.txt',
)


def _load_wordlist(path=None):
    path = path or WORDLIST_PATH
    if not os.path.exists(path):
        raise FileNotFoundError(
            f'Wordlist not found at {path}. '
            f'Run: python scripts/assemble_en_wordlist.py'
        )
    words = []
    with open(path) as f:
        for ln in f:
            w = ln.strip()
            if w:
                words.append(w)
    return words


def score_ocr_accuracy(ch_adapter, *, corpora=None, skip_existing=True,
                       wordlist_path=None, progress=True, batch_size=50_000):
    """Score OCR accuracy for all texts in text_freqs.

    For each text, explodes its freqs Map, checks each word type against
    the wordlist, and computes token-weighted coverage.

    Writes to lltk.text_ocr (ReplacingMergeTree on _id).
    """
    with logmap('Scoring OCR accuracy...') as log:
        from lltk.db.schema import CLICKHOUSE_SCHEMA
        ch_adapter.execute(CLICKHOUSE_SCHEMA['text_ocr'].format(db='lltk'))

        with logmap('Loading wordlist...') as wl_log:
            words = _load_wordlist(wordlist_path)
            wl_log.debug(f'{len(words):,} words from wordlist')

            ch_adapter.execute(
                "CREATE TABLE IF NOT EXISTS tmp.wordlist "
                "(word String) ENGINE=Memory"
            )
            ch_adapter.execute("TRUNCATE TABLE tmp.wordlist")

            t0 = time.time()
            for i in range(0, len(words), 100_000):
                chunk = [[w] for w in words[i:i + 100_000]]
                ch_adapter.client.insert('tmp.wordlist', chunk, column_names=['word'])
            wl_log.debug(f'Loaded into tmp.wordlist in {time.time() - t0:.1f}s')

        with logmap('Scoring texts...') as score_log:
            wheres = []
            if corpora:
                corpora_sql = ', '.join(f"'{ch_quote(c)}'" for c in corpora)
                wheres.append(f"corpus IN ({corpora_sql})")
            if skip_existing:
                wheres.append("_id NOT IN (SELECT _id FROM lltk.text_ocr FINAL)")
            where_sql = f"WHERE {' AND '.join(wheres)}" if wheres else ''

            n_texts = ch_adapter.query(
                f"SELECT count() FROM lltk.text_freqs {where_sql}"
            )[0][0]
            score_log.debug(f'{n_texts:,} texts to score')

            if n_texts == 0:
                score_log.debug('Nothing to score')
                return

            t0 = time.time()
            ch_adapter.execute(f"""
                INSERT INTO lltk.text_ocr (_id, corpus, n_tokens, n_known_tokens, ocr_accuracy)
                SELECT
                    f._id,
                    f.corpus,
                    arraySum(mapValues(f.freqs)) AS n_tokens,
                    arraySum(
                        arrayMap(
                            (k, v) -> if(k IN (SELECT word FROM tmp.wordlist), v, 0),
                            mapKeys(f.freqs),
                            mapValues(f.freqs)
                        )
                    ) AS n_known_tokens,
                    if(n_tokens > 0, n_known_tokens / n_tokens, 0) AS ocr_accuracy
                FROM lltk.text_freqs f
                {where_sql}
            """)
            elapsed = time.time() - t0
            score_log.debug(f'Scored {n_texts:,} texts in {elapsed:.1f}s')

        with logmap('Summary by corpus...') as sum_log:
            df = ch_adapter.query_df("""
                SELECT
                    corpus,
                    count() AS n,
                    round(avg(ocr_accuracy), 3) AS mean_acc,
                    round(quantile(0.1)(ocr_accuracy), 3) AS p10,
                    round(quantile(0.5)(ocr_accuracy), 3) AS median,
                    round(quantile(0.9)(ocr_accuracy), 3) AS p90
                FROM lltk.text_ocr FINAL
                GROUP BY corpus
                ORDER BY mean_acc
            """)
            sum_log.debug('\n' + df.to_string(index=False))
