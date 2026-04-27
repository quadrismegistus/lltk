"""
Prosodic integration: batch parse a corpus into per-text parquet/json files,
then optionally aggregate into a single corpus-level parquet for analysis.

Layout under {corpus.path_prosodic}/{text.id}/:
    syll.parquet     flat syllable-level DataFrame
    parsed.parquet   parsed lines + violations (rows depend on save_parses)
    text.txt[.gz]    source text
    meta.json        config/metadata for resumability

The per-text format and loading is owned by the `prosodic` package
(TextModel.save / TextModel.load). This module only orchestrates.
"""
from __future__ import annotations

import os

from logmap import logmap


def _load_prosodic(min_version=(3, 2, 1)):
    """Import prosodic. Requires >=3.2.1 for parse_corpus(save_kwargs=...)."""
    try:
        import prosodic
    except ImportError as e:
        raise ImportError(
            "prosodic is an optional dep; install with `pip install 'prosodic>=3.2.1'`"
        ) from e
    if not hasattr(prosodic, 'parse_corpus') or not hasattr(prosodic, 'TextModel'):
        raise ImportError(
            f"prosodic is installed but missing parse_corpus/TextModel "
            f"(need >= {'.'.join(map(str, min_version))}); upgrade with `pip install -U prosodic`"
        )
    return prosodic


_VALID_SAVE_PARSES = ('best', 'unbounded', 'all')


def parse_corpus(corpus_id, n_workers=1, device='auto', resume=True,
                 syntax=False, limit=None,
                 # Output-shape knobs
                 save_parses='unbounded', compression='gzip',
                 # Common meter knobs (forwarded to text.parse() via meter_kwargs)
                 max_s=None, max_w=None, combine_by=None, constraints=None,
                 # Escape hatches: arbitrary kwargs forwarded down
                 meter_kwargs=None, text_kwargs=None):
    """Batch-parse a corpus's texts with prosodic. Writes per-text dirs under
    {corpus.path_prosodic}/{text.id}/.

    Args:
      save_parses: 'best' (1 parse/line), 'unbounded' (all Pareto-optimal —
        prosodic's default), or 'all' (every parse including dominated).
        On Shakespeare's sonnets: best ≈ 22K rows, unbounded ≈ 53K, all ≈ 4.3M.
      compression: parquet compression. 'gzip' (default) or None for raw.
      max_s, max_w: max strong / weak positions per foot (Meter knobs).
      combine_by: 'line' (default) or 'sent' — parse-and-combine granularity.
      constraints: dict overriding any of {w_peak, w_stress, s_unstress,
        unres_across, unres_within, foot_size}; defaults all 1.0.
      meter_kwargs / text_kwargs: escape hatches forwarded to text.parse(...)
        and prosodic.Text(...) respectively. CLI-style flags above merge into
        these dicts.

    Resume is on by default: texts whose meta.json already exists are skipped.
    Empty texts (no .txt content) are skipped silently.
    """
    if save_parses not in _VALID_SAVE_PARSES:
        raise ValueError(
            f"save_parses must be one of {_VALID_SAVE_PARSES}; got {save_parses!r}"
        )

    prosodic = _load_prosodic()
    import lltk

    # Merge convenience flags into meter_kwargs / text_kwargs
    mk = dict(meter_kwargs or {})
    if max_s is not None:
        mk['max_s'] = max_s
    if max_w is not None:
        mk['max_w'] = max_w
    if combine_by is not None:
        mk['combine_by'] = combine_by
    if constraints is not None:
        # Merge with prosodic's defaults so a partial dict overrides only those keys
        defaults = {'w_peak': 1.0, 'w_stress': 1.0, 's_unstress': 1.0,
                    'unres_across': 1.0, 'unres_within': 1.0, 'foot_size': 1.0}
        defaults.update(constraints)
        mk['constraints'] = defaults
    meter_kwargs = mk or None

    tk = dict(text_kwargs or {})
    if syntax:
        tk['syntax'] = True
    text_kwargs = tk or None

    C = lltk.load(corpus_id)
    out_dir = C.path_prosodic
    os.makedirs(out_dir, exist_ok=True)

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

    with logmap(f'Parsing {corpus_id} with prosodic...') as log:
        log.debug(f"save_parses={save_parses!r} compression={compression!r}")
        if meter_kwargs:
            log.debug(f"meter_kwargs={meter_kwargs}")
        if text_kwargs:
            log.debug(f"text_kwargs={text_kwargs}")

        save_kwargs = {'save_parses': save_parses, 'compression': compression}
        stats = prosodic.parse_corpus(
            _items(), out_dir,
            n_workers=n_workers,
            device=device,
            resume=resume,
            text_kwargs=text_kwargs,
            meter_kwargs=meter_kwargs,
            save_kwargs=save_kwargs,
            total=total,
            progress=True,
            on_error='log',
        )
        log.debug(
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
    with logmap(f'Aggregating prosodic data for {corpus_id}...') as log:
        if not os.path.isdir(prosodic_dir):
            log.debug(f"No prosodic directory at {prosodic_dir} — run `lltk prosodic-parse {corpus_id}` first")
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
            log.debug(f"Aggregated {n_texts} texts, {n_rows:,} rows -> {out_path}")
        else:
            log.debug(f"No parsed.parquet files found under {prosodic_dir}")
