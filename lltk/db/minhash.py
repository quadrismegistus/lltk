"""MinHash/LSH near-duplicate detection via word-set overlap on text_freqs.

Finds texts with similar vocabularies (high Jaccard similarity) that
metadata-based matching misses — different editions, abridgements,
anthologies containing the same text, etc.

Writes results to lltk.matches as match_type='minhash'.
"""

import time
import pyarrow as pa

from logmap import logmap


def minhash_match_ch(ch_adapter, *, threshold=0.5, num_perm=128, corpus=None):
    from datasketch import MinHash, MinHashLSH

    with logmap('MinHash matching...') as log:
        where = f"WHERE corpus = '{corpus}'" if corpus else ''

        with logmap('Loading word sets from text_freqs...') as load_log:
            t0 = time.time()
            rows = ch_adapter.query(
                f"SELECT _id, mapKeys(freqs) AS words FROM lltk.text_freqs {where}"
            )
            if not rows:
                load_log.debug('No freqs found. Run `lltk db-freqs` first.')
                return
            load_log.debug(f'{len(rows):,} texts in {time.time()-t0:.1f}s')

        with logmap(f'Computing MinHash signatures ({num_perm} perms)...') as sig_log:
            t0 = time.time()
            signatures = {}
            for i, (_id, words) in enumerate(rows):
                if not words:
                    continue
                m = MinHash(num_perm=num_perm)
                for w in words:
                    m.update(w.encode('utf8'))
                signatures[_id] = m
                if (i + 1) % 50000 == 0:
                    elapsed = time.time() - t0
                    rate = (i + 1) / elapsed
                    remaining = (len(rows) - i - 1) / rate / 60
                    sig_log.debug(f'{i+1:,} sigs ({rate:.0f}/s, ~{remaining:.0f}m left)')
            sig_log.debug(f'{len(signatures):,} signatures in {(time.time()-t0)/60:.1f}m')

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
                    a_esc = a.replace("'", "''")
                    b_esc = b.replace("'", "''")
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
