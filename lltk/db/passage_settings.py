"""Ingest PassageSettingTask results into lltk.passage_settings.

Each input JSON represents one passage classification with settings tags,
specificity levels, narrative frequency, space/time scales.
"""

import json
import os
import glob

from logmap import logmap


def ingest_passage_settings(ch_adapter, results_dir, *, batch_size=5000):
    """Ingest passage setting JSONs into lltk.passage_settings.

    Each JSON has metadata._id + metadata.seq identifying the passage,
    plus classification fields (settings, setting_specificity, etc.).
    """
    from lltk.db.schema import CLICKHOUSE_SCHEMA
    ch_adapter.execute(CLICKHOUSE_SCHEMA['passage_settings'].format(db='lltk'))

    with logmap('Ingesting passage settings...') as log:
        files = sorted(glob.glob(os.path.join(results_dir, '**', '*.json'), recursive=True))
        log.debug(f'{len(files):,} files in {results_dir}')

        if not files:
            return 0

        rows = []
        n_errors = 0

        for path in files:
            try:
                with open(path) as f:
                    d = json.load(f)
            except (json.JSONDecodeError, OSError):
                n_errors += 1
                continue

            meta = d.get('metadata', {})
            _id = meta.get('_id') or meta.get('_canonical_id') or meta.get('source')
            seq = meta.get('seq')
            if not _id or seq is None:
                n_errors += 1
                continue

            corpus = _id.split('/')[0].lstrip('_') if '/' in _id else ''

            rows.append([
                _id,
                int(seq),
                float(meta.get('position', 0.0)),
                corpus,
                meta.get('model', 'unknown'),
                d.get('settings', []),
                d.get('settings_other', []),
                d.get('setting_specificity', ''),
                d.get('time_specificity', ''),
                d.get('narrative_frequency', ''),
                d.get('space_traversed', ''),
                d.get('time_elapsed', ''),
            ])

            if len(rows) >= batch_size:
                _insert_batch(ch_adapter, rows)
                log.debug(f'Inserted {len(rows):,} rows...')
                rows = []

        if rows:
            _insert_batch(ch_adapter, rows)

        total = len(files) - n_errors
        log.debug(f'Done: {total:,} passages ingested, {n_errors} errors')
        return total


def _insert_batch(ch_adapter, rows):
    ch_adapter.client.insert(
        'lltk.passage_settings',
        rows,
        column_names=[
            '_id', 'seq', 'position', 'corpus', 'model',
            'settings', 'settings_other',
            'setting_specificity', 'time_specificity',
            'narrative_frequency', 'space_traversed', 'time_elapsed',
        ],
    )
