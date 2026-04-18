"""
MinHash matching on freqs — finds near-duplicate texts by word-set overlap.
Writes results into ClickHouse lltk.matches as match_type='minhash'.

Usage:
    python scripts/minhash_match.py [--threshold 0.5] [--num-perm 128]
"""
import os, sys, time, argparse
from datasketch import MinHash, MinHashLSH
from lltk.tools.db_adapter import get_adapter

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--threshold', type=float, default=0.5)
    parser.add_argument('--num-perm', type=int, default=128)
    parser.add_argument('--corpus', type=str, default=None, help='Limit to one corpus')
    args = parser.parse_args()

    print('Loading word sets from ClickHouse text_freqs...', flush=True)
    t0 = time.time()
    ch_url = os.environ.get(
        'LLTK_CLICKHOUSE_URL',
        'clickhouse://lltk:lltk@localhost:8123/lltk',
    )
    ch = get_adapter(ch_url)
    where = f"WHERE corpus = '{args.corpus}'" if args.corpus else ''
    rows = ch.query(f"""
        SELECT _id, mapKeys(freqs) AS words FROM lltk.text_freqs {where}
    """)
    ch.close()
    if not rows:
        print('No freqs found. Run `lltk db-freqs` first.', flush=True)
        sys.exit(1)
    print(f'Loaded {len(rows):,} texts in {time.time()-t0:.1f}s', flush=True)

    # Compute MinHash signatures
    print(f'Computing MinHash signatures ({args.num_perm} perms)...', flush=True)
    t0 = time.time()
    signatures = {}
    for i, (_id, words) in enumerate(rows):
        if not words:
            continue
        m = MinHash(num_perm=args.num_perm)
        for w in words:
            m.update(w.encode('utf8'))
        signatures[_id] = m
        if (i + 1) % 10000 == 0:
            elapsed = time.time() - t0
            rate = (i + 1) / elapsed
            remaining = (len(rows) - i - 1) / rate / 60
            print(f'  {i+1:,} sigs ({rate:.0f}/s, ~{remaining:.0f}m left)', flush=True)

    print(f'  {len(signatures):,} signatures in {(time.time()-t0)/60:.1f}m', flush=True)

    # LSH to find candidate pairs
    print(f'Running LSH (threshold={args.threshold})...', flush=True)
    t0 = time.time()
    lsh = MinHashLSH(threshold=args.threshold, num_perm=args.num_perm)
    for _id, sig in signatures.items():
        try:
            lsh.insert(_id, sig)
        except ValueError:
            pass

    pairs = set()
    for _id, sig in signatures.items():
        candidates = lsh.query(sig)
        for c in candidates:
            if c != _id:
                pair = tuple(sorted([_id, c]))
                pairs.add(pair)

    print(f'  {len(pairs):,} candidate pairs in {time.time()-t0:.1f}s', flush=True)

    # Compute exact Jaccard for candidates
    print('Computing exact similarities...', flush=True)
    t0 = time.time()
    matches = []
    for a, b in pairs:
        sim = signatures[a].jaccard(signatures[b])
        if sim >= args.threshold:
            matches.append((a, b, float(sim)))

    matches.sort(key=lambda x: -x[2])
    print(f'  {len(matches):,} matches (Jaccard >= {args.threshold}) in {time.time()-t0:.1f}s', flush=True)

    if not matches:
        print('No matches found.')
        return

    # Save matches to CSV first (in case DB is locked)
    import pandas as pd
    import pandas as pd
    import pyarrow as pa
    csv_path = os.path.expanduser('~/lltk_data/data/minhash_matches.csv')
    df = pd.DataFrame(matches, columns=['_id_a', '_id_b', 'similarity'])
    df['match_type'] = 'minhash'
    df.to_csv(csv_path, index=False)
    print(f'Saved {len(matches):,} matches to {csv_path}', flush=True)

    # Write to ClickHouse lltk.matches
    print('Writing to ClickHouse lltk.matches...', flush=True)
    ch = get_adapter(ch_url)

    # Remove old minhash matches first
    try:
        ch.execute("ALTER TABLE lltk.matches DELETE WHERE match_type='minhash' "
                   "SETTINGS mutations_sync=1")
    except Exception as e:
        print(f'  old-row DELETE failed: {e}')

    # Ensure (_id_a < _id_b) ordering to match the table's ORDER BY
    df = df.copy()
    a_lt = df['_id_a'] < df['_id_b']
    df.loc[~a_lt, ['_id_a', '_id_b']] = df.loc[~a_lt, ['_id_b', '_id_a']].values
    df = df.drop_duplicates(subset=['_id_a', '_id_b'])
    tbl = pa.Table.from_pandas(df[['_id_a', '_id_b', 'similarity', 'match_type']],
                               preserve_index=False)
    ch.client.insert_arrow('matches', tbl)
    print(f'  Inserted {len(df):,} minhash matches', flush=True)

    # Show sample with titles
    print(f'\nTop 20 matches:')
    for a, b, sim in matches[:20]:
        try:
            a_esc = a.replace("'", "''")
            b_esc = b.replace("'", "''")
            ra = ch.query(f"SELECT title, author, year FROM lltk.texts FINAL WHERE _id='{a_esc}' LIMIT 1")
            rb = ch.query(f"SELECT title, author, year FROM lltk.texts FINAL WHERE _id='{b_esc}' LIMIT 1")
            ra = ra[0] if ra else None
            rb = rb[0] if rb else None
            ta = f'[{ra[2]}] {(ra[1] or "")[:20]}: {(ra[0] or "")[:40]}' if ra else a
            tb = f'[{rb[2]}] {(rb[1] or "")[:20]}: {(rb[0] or "")[:40]}' if rb else b
        except Exception:
            ta, tb = a, b
        print(f'  {sim:.3f}  {ta}')
        print(f'         {tb}')
        print()

    ch.close()
    print('Done.')

if __name__ == '__main__':
    main()
