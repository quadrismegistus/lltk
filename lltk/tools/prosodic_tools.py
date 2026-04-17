"""
Prosodic integration: batch parse a corpus into per-text parquet/json files,
then optionally aggregate into a single corpus-level parquet for analysis.

Layout under {corpus.path_prosodic}/{text.id}/:
    syll.parquet     flat syllable-level DataFrame
    parsed.parquet   per-line best parse + violations
    meta.json        config/metadata for resumability

The per-text format and loading is owned by the `prosodic` package
(TextModel.save / TextModel.load). This module only orchestrates.
"""
from __future__ import annotations

import os


def _load_prosodic(min_version=(3, 1)):
    """Import prosodic. Requires >=3.1 for parse_corpus / TextModel."""
    try:
        import prosodic
    except ImportError as e:
        raise ImportError(
            "prosodic is an optional dep; install with `pip install prosodic>=3.1`"
        ) from e
    if not hasattr(prosodic, 'parse_corpus') or not hasattr(prosodic, 'TextModel'):
        raise ImportError(
            f"prosodic is installed but missing parse_corpus/TextModel "
            f"(need >= {'.'.join(map(str, min_version))}); upgrade with `pip install -U prosodic`"
        )
    return prosodic


def parse_corpus(corpus_id, n_workers=1, device='auto', resume=True,
                 syntax=False, limit=None):
    """Batch-parse a corpus's texts with prosodic. Writes per-text dirs under
    {corpus.path_prosodic}/{text.id}/.

    Resume is on by default: texts whose meta.json already exists are skipped.
    Empty texts (no .txt content) are skipped silently.
    """
    prosodic = _load_prosodic()
    import lltk

    C = lltk.load(corpus_id)
    out_dir = C.path_prosodic
    os.makedirs(out_dir, exist_ok=True)

    # Lazy generator over texts with non-empty txt
    def _items():
        for i, t in enumerate(C.texts()):
            if limit is not None and i >= limit:
                return
            try:
                txt = t.txt
            except Exception:
                txt = ''
            if not txt:
                continue
            yield (t.id, txt)

    try:
        total = limit if limit is not None else len(C.meta)
    except Exception:
        total = None

    text_kwargs = {'syntax': True} if syntax else None

    stats = prosodic.parse_corpus(
        _items(), out_dir,
        n_workers=n_workers,
        device=device,
        resume=resume,
        text_kwargs=text_kwargs,
        total=total,
        progress=True,
        on_error='log',
    )
    print(
        f"done: {stats.get('n_done', 0)}  "
        f"skipped: {stats.get('n_skipped', 0)}  "
        f"failed: {stats.get('n_failed', 0)}"
    )
    return stats


def aggregate_corpus(corpus_id, out_path=None):
    """Concatenate every per-text parsed.parquet into a single corpus-level
    parquet at {corpus.path}/prosodic.parquet (or `out_path` if provided).

    Streams incrementally via pyarrow.parquet.ParquetWriter so Hathi-scale
    corpora don't OOM.
    """
    import lltk
    import pyarrow as pa
    import pyarrow.parquet as pq

    C = lltk.load(corpus_id)
    prosodic_dir = C.path_prosodic
    if not os.path.isdir(prosodic_dir):
        print(f"No prosodic directory at {prosodic_dir} — run `lltk prosodic-parse {corpus_id}` first")
        return

    if out_path is None:
        out_path = os.path.join(C.path, 'prosodic.parquet')
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    writer = None
    n_texts = 0
    n_rows = 0
    text_ids = sorted(
        tid for tid in os.listdir(prosodic_dir)
        if os.path.isdir(os.path.join(prosodic_dir, tid))
    )
    try:
        from lltk.tools.tools import get_tqdm
        iterator = get_tqdm(text_ids, desc=f'Aggregating {corpus_id}')
    except Exception:
        iterator = text_ids

    for tid in iterator:
        pq_path = os.path.join(prosodic_dir, tid, 'parsed.parquet')
        if not os.path.exists(pq_path):
            continue
        try:
            tbl = pq.read_table(pq_path)
        except Exception:
            continue
        # Prepend text_id column
        tbl = tbl.append_column('text_id', pa.array([tid] * tbl.num_rows, type=pa.string()))
        if writer is None:
            writer = pq.ParquetWriter(out_path, tbl.schema)
        else:
            # Ensure schema compatibility: re-align columns if necessary
            if tbl.schema != writer.schema:
                try:
                    tbl = tbl.select(writer.schema.names)
                except Exception:
                    # Skip texts whose schema has diverged (rare but possible
                    # across prosodic version bumps)
                    continue
        writer.write_table(tbl)
        n_texts += 1
        n_rows += tbl.num_rows
    if writer is not None:
        writer.close()
        print(f"aggregated {n_texts} texts, {n_rows:,} rows → {out_path}")
    else:
        print(f"no parsed.parquet files found under {prosodic_dir}")
