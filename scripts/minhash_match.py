"""
MinHash matching on freqs DB — finds near-duplicate texts by word frequency overlap.
Writes results into metadb_matches.duckdb as match_type='minhash'.

Usage:
    python scripts/minhash_match.py [--threshold 0.5] [--num-perm 128]
"""
import duckdb, os, sys, time, argparse
from datasketch import MinHash, MinHashLSH

FREQS_PATH = os.path.expanduser('~/lltk_data/data/metadb_freqs.duckdb')
MATCH_PATH = os.path.expanduser('~/lltk_data/data/metadb_matches.duckdb')

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--threshold', type=float, default=0.5)
    parser.add_argument('--num-perm', type=int, default=128)
    parser.add_argument('--corpus', type=str, default=None, help='Limit to one corpus')
    args = parser.parse_args()

    # Load word sets from freqs DB
    print('Loading word sets from freqs DB...', flush=True)
    t0 = time.time()
    freqs_conn = duckdb.connect(FREQS_PATH, read_only=True)

    corpus_filter = f"WHERE corpus = '{args.corpus}'" if args.corpus else ""
    rows = freqs_conn.execute(f"""
        SELECT _id, map_keys(freqs) as words FROM text_freqs {corpus_filter}
    """).fetchall()
    freqs_conn.close()
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
    csv_path = os.path.join(os.path.dirname(MATCH_PATH), 'minhash_matches.csv')
    df = pd.DataFrame(matches, columns=['_id_a', '_id_b', 'similarity'])
    df['match_type'] = 'minhash'
    df.to_csv(csv_path, index=False)
    print(f'Saved {len(matches):,} matches to {csv_path}', flush=True)

    # Write to match DB
    print('Writing to match DB...', flush=True)
    try:
        match_conn = duckdb.connect(MATCH_PATH)
    except Exception as e:
        print(f'Cannot open match DB: {e}')
        print(f'Matches saved to CSV. Run: python scripts/minhash_insert.py')
        return

    # Remove old minhash matches
    try:
        n_old = match_conn.execute("SELECT COUNT(*) FROM matches WHERE match_type='minhash'").fetchone()[0]
        if n_old > 0:
            match_conn.execute("DELETE FROM matches WHERE match_type='minhash'")
            print(f'  Removed {n_old:,} old minhash matches', flush=True)
    except Exception:
        pass

    # Insert new matches (skip pairs that already exist from other tiers)
    import pandas as pd
    df = pd.DataFrame(matches, columns=['_id_a', '_id_b', 'similarity'])
    df['match_type'] = 'minhash'
    match_conn.execute("""
        INSERT OR IGNORE INTO matches (_id_a, _id_b, similarity, match_type)
        SELECT _id_a, _id_b, similarity, match_type FROM df
    """)
    print(f'  Inserted {len(matches):,} minhash matches', flush=True)

    # Show sample
    meta_conn = duckdb.connect(os.path.expanduser('~/lltk_data/data/metadb.duckdb'), read_only=True)
    print(f'\nTop 20 matches:')
    for a, b, sim in matches[:20]:
        try:
            ra = meta_conn.execute(f"SELECT title, author, year FROM texts WHERE _id='{a}'").fetchone()
            rb = meta_conn.execute(f"SELECT title, author, year FROM texts WHERE _id='{b}'").fetchone()
            ta = f'[{ra[2]}] {(ra[1] or "")[:20]}: {(ra[0] or "")[:40]}' if ra else a
            tb = f'[{rb[2]}] {(rb[1] or "")[:20]}: {(rb[0] or "")[:40]}' if rb else b
        except:
            ta, tb = a, b
        print(f'  {sim:.3f}  {ta}')
        print(f'         {tb}')
        print()

    meta_conn.close()
    match_conn.close()
    print('Done.')

if __name__ == '__main__':
    main()
