"""MinHash/LSH near-duplicate detection via word-set overlap on text_freqs.

Finds texts with similar vocabularies (high Jaccard similarity) that
metadata-based matching misses — different editions, abridgements,
anthologies containing the same text, etc.

Writes results to lltk.matches as match_type='minhash'.

Signature computation is checkpointed per-corpus to
~/lltk_data/corpora/{corpus}/data/minhash_{num_perm}.pkl so interrupted
runs resume from the last completed corpus.
"""

import os
import pickle
import time
import pyarrow as pa

from logmap import logmap

from lltk.db.adapter import ch_quote


def _cache_path(corpus_id, num_perm):
    return os.path.expanduser(
        f'~/lltk_data/corpora/{corpus_id}/data/minhash_{num_perm}.pkl'
    )


def minhash_match_ch(ch_adapter, *, threshold=0.5, num_perm=128, corpus=None):
    from datasketch import MinHash, MinHashLSH

    with logmap('MinHash matching...') as log:

        with logmap('Loading signatures (cached or computed)...') as load_log:
            t0 = time.time()
            if corpus:
                corpora = [corpus]
            else:
                corpora = [r[0] for r in ch_adapter.query(
                    "SELECT DISTINCT corpus FROM lltk.text_freqs ORDER BY corpus"
                )]
            load_log.debug(f'{len(corpora)} corpora to process')

            signatures = {}
            n_cached = 0
            for ci, c in enumerate(corpora):
                cache = _cache_path(c, num_perm)
                if os.path.exists(cache):
                    with open(cache, 'rb') as f:
                        cached_sigs = pickle.load(f)
                    signatures.update(cached_sigs)
                    n_cached += 1
                    load_log.debug(
                        f'[{ci+1}/{len(corpora)}] {c}: {len(cached_sigs):,} cached '
                        f'({len(signatures):,} total)'
                    )
                    continue

                corpus_sigs = {}
                batch_size = 10_000
                offset = 0
                while True:
                    rows = ch_adapter.client.query(
                        f"SELECT _id, mapKeys(freqs) AS words "
                        f"FROM lltk.text_freqs WHERE corpus = '{c}' "
                        f"ORDER BY _id LIMIT {batch_size} OFFSET {offset}",
                        settings={'max_memory_usage': 0},
                    ).result_rows
                    if not rows:
                        break
                    for _id, words in rows:
                        if not words:
                            continue
                        m = MinHash(num_perm=num_perm)
                        for w in words:
                            m.update(w.encode('utf8'))
                        corpus_sigs[_id] = m
                    offset += len(rows)
                    if len(rows) < batch_size:
                        break

                os.makedirs(os.path.dirname(cache), exist_ok=True)
                with open(cache, 'wb') as f:
                    pickle.dump(corpus_sigs, f, protocol=pickle.HIGHEST_PROTOCOL)

                signatures.update(corpus_sigs)
                load_log.debug(
                    f'[{ci+1}/{len(corpora)}] {c}: {len(corpus_sigs):,} sigs (saved) '
                    f'({len(signatures):,} total)'
                )

            if not signatures:
                load_log.debug('No freqs found. Run `lltk db-freqs` first.')
                return
            load_log.debug(
                f'{len(signatures):,} signatures in {(time.time()-t0)/60:.1f}m '
                f'({n_cached} from cache)'
            )

        with logmap(f'Running LSH (threshold={threshold})...') as lsh_log:
            t0 = time.time()
            lsh = MinHashLSH(threshold=threshold, num_perm=num_perm)
            for _id, sig in signatures.items():
                try:
                    lsh.insert(_id, sig)
                except ValueError:
                    pass

            pairs = set()
            for _id, sig in signatures.items():
                for c in lsh.query(sig):
                    if c != _id:
                        pairs.add(tuple(sorted([_id, c])))
            lsh_log.debug(f'{len(pairs):,} candidate pairs in {time.time()-t0:.1f}s')

        with logmap('Computing exact similarities...') as exact_log:
            t0 = time.time()
            matches = []
            for a, b in pairs:
                sim = signatures[a].jaccard(signatures[b])
                if sim >= threshold:
                    matches.append((a, b, float(sim)))
            matches.sort(key=lambda x: -x[2])
            exact_log.debug(f'{len(matches):,} matches (Jaccard >= {threshold}) in {time.time()-t0:.1f}s')

        if not matches:
            log.debug('No matches found.')
            return

        with logmap('Writing to lltk.matches...') as write_log:
            import pandas as pd
            df = pd.DataFrame(matches, columns=['_id_a', '_id_b', 'similarity'])
            df['match_type'] = 'minhash'

            swap = df['_id_a'] > df['_id_b']
            df.loc[swap, ['_id_a', '_id_b']] = df.loc[swap, ['_id_b', '_id_a']].values
            df = df.drop_duplicates(subset=['_id_a', '_id_b'])

            try:
                ch_adapter.execute(
                    "ALTER TABLE lltk.matches DELETE WHERE match_type='minhash' "
                    "SETTINGS mutations_sync=1"
                )
            except Exception as e:
                write_log.debug(f'old-row DELETE failed: {e}')

            tbl = pa.Table.from_pandas(
                df[['_id_a', '_id_b', 'similarity', 'match_type']],
                preserve_index=False,
            )
            ch_adapter.client.insert_arrow('matches', tbl)
            write_log.debug(f'Inserted {len(df):,} minhash matches')

        with logmap('Top 10 matches...') as top_log:
            for a, b, sim in matches[:10]:
                try:
                    a_esc = ch_quote(a)
                    b_esc = ch_quote(b)
                    ra = ch_adapter.query(
                        f"SELECT title, author, year FROM lltk.texts FINAL WHERE _id='{a_esc}' LIMIT 1"
                    )
                    rb = ch_adapter.query(
                        f"SELECT title, author, year FROM lltk.texts FINAL WHERE _id='{b_esc}' LIMIT 1"
                    )
                    ra = ra[0] if ra else None
                    rb = rb[0] if rb else None
                    ta = f'[{ra[2]}] {(ra[1] or "")[:20]}: {(ra[0] or "")[:40]}' if ra else a
                    tb = f'[{rb[2]}] {(rb[1] or "")[:20]}: {(rb[0] or "")[:40]}' if rb else b
                except Exception:
                    ta, tb = a, b
                top_log.debug(f'{sim:.3f}  {ta}')
                top_log.debug(f'       {tb}')

        return len(df)
