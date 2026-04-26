"""
lltk.annotate — orchestrate LLM annotation tasks over lltk texts.

This module is the integration layer between lltk (which owns text
resolution, passage storage, and annotation persistence) and
largeliterarymodels (which owns pure extraction: task.run() -> dict).

Two task families:

  Task (base)        — single prompt → Pydantic model. GenreTask, FryeTask, etc.
                       lltk formats metadata into a prompt, calls task.run(prompt).

  SequentialTask     — passage list → chunked feedforward → aggregated dict.
                       SocialNetworkTask, PassageContentTask, etc.
                       lltk resolves passages, calls task.run(passages).

Usage:

    import lltk
    from lltk.annotate import run_task

    # Genre-classify all ESTC fiction texts
    run_task('genre', corpus='estc', genre='Fiction',
             model='gemini-2.5-flash')

    # Social network extraction for specific texts
    run_task('social_network', ids=['_chadwyck/Z200338805'],
             model='gemini-2.5-pro')
"""

from __future__ import annotations

import os
import time
from typing import Any, Optional

import pandas as pd

from lltk.tools.logs import log


# ── Task registry ──────────────────────────────────────────────────────
# Maps short names to (import_path, class_name, task_type).
# task_type: 'base' or 'sequential' — determines how lltk calls .run().

TASK_REGISTRY: dict[str, tuple[str, str, str]] = {
    'genre':              ('largeliterarymodels.tasks', 'GenreTask', 'base'),
    'genre_lite':         ('largeliterarymodels.tasks', 'GenreTaskLite', 'base'),
    'frye':               ('largeliterarymodels.tasks', 'FryeTask', 'base'),
    'character':          ('largeliterarymodels.tasks', 'CharacterTask', 'base'),
    'character_intro':    ('largeliterarymodels.tasks', 'CharacterIntroTask', 'base'),
    'translation':        ('largeliterarymodels.tasks', 'TranslationTask', 'base'),
    'bibliography':       ('largeliterarymodels.tasks', 'BibliographyTask', 'base'),
    'social_network':     ('largeliterarymodels.tasks', 'SocialNetworkTask', 'sequential'),
    'passage_content':    ('largeliterarymodels.tasks', 'PassageContentTask', 'sequential'),
    'passage_form':       ('largeliterarymodels.tasks', 'PassageFormTask', 'sequential'),
    'passage':            ('largeliterarymodels.tasks', 'PassageTask', 'base'),
}


def _load_task(name: str):
    """Lazily import and instantiate a task by short name."""
    if name not in TASK_REGISTRY:
        raise ValueError(
            f"Unknown task {name!r}. Available: {', '.join(sorted(TASK_REGISTRY))}"
        )
    module_path, class_name, task_type = TASK_REGISTRY[name]
    import importlib
    mod = importlib.import_module(module_path)
    cls = getattr(mod, class_name)
    return cls(), task_type


# ── Prompt formatting for base tasks ───────────────────────────────────

def _format_genre_prompt(row: dict) -> str:
    """Format a texts row into a genre classification prompt.

    Mirrors largeliterarymodels.tasks.classify_genre.format_text_for_classification
    but without importing it — lltk owns prompt construction.
    """
    title = str(row.get('title', '')).split('(')[0].strip()
    parts = [f"Title: {title}"]

    author = row.get('author_norm') or row.get('author', '')
    if author:
        if ',' in author:
            author = author.split(',')[0].strip()
        parts.append(f"Author: {author.title()}")

    year = row.get('year')
    if year and pd.notna(year):
        y = int(year)
        century_start = (y // 100) * 100
        parts.append(f"Year: {century_start}-{century_start + 100}")

    for field in ('subject_topic', 'form'):
        val = row.get(field, '')
        if val and pd.notna(val):
            parts.append(f"{field.replace('_', ' ').title()}: {val}")

    return '\n'.join(parts)


# Default prompt formatters per task.
# Tasks not listed here require passages (sequential) or use the text's
# full content as-is.
_PROMPT_FORMATTERS: dict[str, callable] = {
    'genre': _format_genre_prompt,
    'genre_lite': _format_genre_prompt,
}


# ── Annotation field mapping ──────────────────────────────────────────
# Maps task result fields → annotations field names.
# Only fields listed here get written to lltk.annotations.

_ANNOTATION_FIELDS: dict[str, dict[str, str]] = {
    'genre': {
        'genre': 'genre',
        'genre_raw': 'genre_raw',
        'is_translated': 'is_translated',
        'translated_from': 'original_lang',
        'year_estimated': 'year_estimated',
        'author_first_name': 'author_first_name',
    },
    'genre_lite': {
        'genre': 'genre',
        'genre_raw': 'genre_raw',
    },
    'frye': {
        'mode': 'frye_mode',
        'mythos': 'frye_mythos',
    },
    'translation': {
        'is_translated': 'is_translated',
        'original_lang': 'original_lang',
    },
}


# ── Core orchestrator ─────────────────────────────────────────────────

def _get_text_ids(
    ids: list[str] | None = None,
    corpus: str | None = None,
    genre: str | None = None,
    lang: str | None = None,
    year_min: int | None = None,
    year_max: int | None = None,
    where: str | None = None,
    limit: int | None = None,
) -> pd.DataFrame:
    """Query CH for text IDs matching filters. Returns texts DataFrame."""
    import lltk
    if ids is not None:
        id_list = ', '.join(f"'{i}'" for i in ids)
        df = lltk.db.adapter.query_df(
            f"SELECT * FROM lltk.texts FINAL WHERE _id IN ({id_list})"
        )
        return df

    return lltk.db.texts_df(
        where=where,
        genre=genre,
        year_min=year_min,
        year_max=year_max,
        corpora=[corpus] if corpus else None,
        dedup=True,
    )


def _already_annotated(source: str, task_name: str, ids: list[str]) -> set[str]:
    """Return set of _ids that this source has already annotated for this task's fields."""
    from lltk.tools import annotations as A
    fields = list(_ANNOTATION_FIELDS.get(task_name, {}).values())
    if not fields:
        return set()
    try:
        df = A.resolve_by_source(source, ids=ids, fields=fields[:1])
        return set(df['_id'].unique())
    except Exception:
        return set()


def _result_to_annotation_rows(
    _id: str,
    result: Any,
    task_name: str,
    confidence_override: float | None = None,
) -> list[dict]:
    """Convert a task result (Pydantic model or dict) into annotation rows."""
    field_map = _ANNOTATION_FIELDS.get(task_name, {})
    if not field_map:
        return []

    if hasattr(result, 'model_dump'):
        data = result.model_dump()
    elif isinstance(result, dict):
        data = result
    else:
        return []

    result_confidence = data.get('confidence', 1.0)
    if confidence_override is not None:
        result_confidence = confidence_override

    rows = []
    meta_fields = {}
    if 'reasoning' in data:
        meta_fields['reasoning'] = data['reasoning']

    for result_key, anno_field in field_map.items():
        value = data.get(result_key)
        if value is None or (isinstance(value, str) and not value):
            continue
        if isinstance(value, int) and value == 0 and anno_field == 'year_estimated':
            continue

        rows.append({
            '_id': _id,
            'field': anno_field,
            'value': value,
            'confidence': result_confidence,
            'meta': meta_fields if meta_fields else {},
        })

    return rows


def run_task(
    task_name: str,
    *,
    ids: list[str] | None = None,
    corpus: str | None = None,
    genre: str | None = None,
    lang: str | None = None,
    year_min: int | None = None,
    year_max: int | None = None,
    where: str | None = None,
    limit: int | None = None,
    model: str | None = None,
    source: str | None = None,
    batch_size: int = 100,
    skip_existing: bool = True,
    save_annotations: bool = True,
    save_tasks: bool = False,
    force: bool = False,
    verbose: bool = True,
    **task_kwargs,
) -> dict:
    """Run an LLM task over a set of texts.

    For base tasks (GenreTask etc): formats metadata into prompts, calls
    task.run(prompt) per text, writes results to lltk.annotations.

    For sequential tasks (SocialNetworkTask etc): resolves passages via
    get_passages(), calls task.run(passages) per text, saves output to
    task_path.

    Args:
        task_name: Short name from TASK_REGISTRY.
        ids: Specific text _ids to process.
        corpus/genre/lang/year_min/year_max/where: CH filters.
        limit: Max texts to process.
        model: LLM model override.
        source: Annotation source label (default: 'llm:{model_slug}').
        batch_size: Texts per annotation write batch.
        skip_existing: Skip texts already annotated by this source.
        save_annotations: Write results to lltk.annotations.
        save_tasks: Also save full JSON to task_path (for sequential tasks).
        force: Bypass LLM cache.
        verbose: Print progress.
        **task_kwargs: Passed to task.run().

    Returns:
        dict with keys: n_processed, n_skipped, n_errors, elapsed_seconds.
    """
    from lltk.tools import annotations as A

    task, task_type = _load_task(task_name)
    model = model or getattr(task, 'model', None) or 'gemini-2.5-flash'
    model_slug = model.split('/')[-1].lower().replace('.', '').replace(' ', '_')
    source = source or f'llm:{model_slug}'

    texts_df = _get_text_ids(
        ids=ids, corpus=corpus, genre=genre, lang=lang,
        year_min=year_min, year_max=year_max, where=where, limit=limit,
    )

    if len(texts_df) == 0 or '_id' not in texts_df.columns:
        if verbose:
            log.info(f'[annotate] {task_name}: no matching texts found')
        return {'n_processed': 0, 'n_skipped': 0, 'n_errors': 0, 'elapsed_seconds': 0}

    if limit:
        texts_df = texts_df.head(limit)

    all_ids = texts_df['_id'].tolist()
    if verbose:
        log.info(f'[annotate] {task_name}: {len(all_ids)} texts, model={model}, source={source}')

    if skip_existing and all_ids:
        done = _already_annotated(source, task_name, all_ids)
        if done:
            texts_df = texts_df[~texts_df['_id'].isin(done)]
            if verbose:
                log.info(f'[annotate] skipping {len(done)} already annotated, {len(texts_df)} remaining')

    if len(texts_df) == 0:
        if verbose:
            log.info('[annotate] nothing to do')
        return {'n_processed': 0, 'n_skipped': len(all_ids), 'n_errors': 0, 'elapsed_seconds': 0}

    formatter = _PROMPT_FORMATTERS.get(task_name)

    n_processed = 0
    n_errors = 0
    anno_batch = []
    t0 = time.time()

    for idx, row in texts_df.iterrows():
        _id = row['_id']

        try:
            if task_type == 'base' and formatter:
                prompt = formatter(row.to_dict())
                result = task.run(
                    prompt, model=model, force=force,
                    metadata={'_id': _id}, **task_kwargs,
                )
            elif task_type == 'sequential':
                import lltk
                passages_df = lltk.db.get_passages([_id])
                if len(passages_df) == 0:
                    if verbose:
                        log.warning(f'[annotate] {_id}: no passages, skipping')
                    n_errors += 1
                    continue
                passages = passages_df.sort_values('seq')['text'].tolist()
                result = task.run(
                    passages, model=model, force=force,
                    verbose=False, save=False,
                    cache_key=_id, **task_kwargs,
                )
            else:
                import lltk
                txt = lltk.db.get(_id)
                if txt and 'title' in txt:
                    prompt = _format_genre_prompt(txt)
                else:
                    prompt = str(txt)
                result = task.run(
                    prompt, model=model, force=force,
                    metadata={'_id': _id}, **task_kwargs,
                )
        except Exception as e:
            if verbose:
                log.warning(f'[annotate] {_id}: {e!s:.100s}')
            n_errors += 1
            continue

        n_processed += 1

        if save_annotations:
            anno_rows = _result_to_annotation_rows(_id, result, task_name)
            anno_batch.extend(anno_rows)
            if len(anno_batch) >= batch_size:
                A.write(source=source, rows=anno_batch)
                anno_batch = []

        if save_tasks and task_type == 'sequential':
            _save_task_result(_id, task_name, result, model_slug)

        if verbose and n_processed % 50 == 0:
            elapsed = time.time() - t0
            rate = n_processed / elapsed if elapsed > 0 else 0
            log.info(
                f'[annotate] {n_processed}/{len(texts_df)} '
                f'({rate:.1f}/s, {n_errors} errors)'
            )

    if anno_batch and save_annotations:
        A.write(source=source, rows=anno_batch)

    elapsed = time.time() - t0
    stats = {
        'n_processed': n_processed,
        'n_skipped': len(all_ids) - len(texts_df),
        'n_errors': n_errors,
        'elapsed_seconds': round(elapsed, 1),
    }

    if verbose:
        log.info(
            f'[annotate] done: {n_processed} processed, '
            f'{stats["n_skipped"]} skipped, {n_errors} errors '
            f'in {elapsed:.0f}s'
        )

    return stats


def _save_task_result(_id: str, task_name: str, result: dict, model_slug: str):
    """Save full task JSON output to corpus-local task directory."""
    import json
    import lltk
    task_dir = lltk.task_path(_id, task_name)
    os.makedirs(task_dir, exist_ok=True)
    path = os.path.join(task_dir, f'{model_slug}.json')
    with open(path, 'w') as f:
        json.dump(result, f, indent=2, ensure_ascii=False)


# ── Ingest results from external runs (Colab/HPC) ───────────���────────

# Scalar extractors: given a task result dict, return annotation rows.
# Each extractor returns a list of (field, value) pairs.

def _extract_social_network_scalars(result: dict) -> list[tuple[str, Any]]:
    """Extract scalar metrics from a social network result."""
    scalars = []
    chars = result.get('characters', [])
    if chars:
        scalars.append(('n_characters', len(chars)))
    relations = result.get('relations', [])
    if relations:
        scalars.append(('n_relations', len(relations)))
    events = result.get('events', [])
    if events:
        scalars.append(('n_events', len(events)))
    dialogue = result.get('dialogue', [])
    if dialogue:
        scalars.append(('n_dialogue', len(dialogue)))
    return scalars


_SCALAR_EXTRACTORS: dict[str, callable] = {
    'social_network': _extract_social_network_scalars,
}


def ingest_tasks(
    task_name: str,
    results_dir: str,
    *,
    source: str | None = None,
    extract_scalars: bool = False,
    dry_run: bool = False,
    verbose: bool = True,
) -> dict:
    """Ingest task result JSONs from an external run (Colab/HPC).

    Reads each JSON file, extracts _id from metadata.source, and:
      1. Places the file in the correct task_path location
      2. Optionally extracts scalar fields to lltk.annotations

    Args:
        task_name: Task name (e.g. 'social_network').
        results_dir: Directory of JSON result files (flat layout).
        source: Annotation source label for scalars.
        extract_scalars: Write derived scalars to annotations.
        dry_run: Print what would happen without writing.
        verbose: Print progress.

    Returns:
        dict with keys: n_ingested, n_skipped, n_errors.
    """
    import json
    import glob
    import shutil
    import lltk

    json_files = sorted(glob.glob(os.path.join(results_dir, '*.json')))
    if not json_files:
        if verbose:
            log.info(f'[ingest] no JSON files in {results_dir}')
        return {'n_ingested': 0, 'n_skipped': 0, 'n_errors': 0}

    if verbose:
        log.info(f'[ingest] {task_name}: {len(json_files)} files in {results_dir}')

    extractor = _SCALAR_EXTRACTORS.get(task_name) if extract_scalars else None
    anno_batch = []
    n_ingested = 0
    n_skipped = 0
    n_errors = 0

    for path in json_files:
        try:
            with open(path) as f:
                result = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            if verbose:
                log.warning(f'[ingest] {os.path.basename(path)}: {e!s:.80s}')
            n_errors += 1
            continue

        metadata = result.get('metadata', {})
        _id = metadata.get('source')
        if not _id or not _id.startswith('_'):
            if verbose:
                log.warning(f'[ingest] {os.path.basename(path)}: no valid _id in metadata.source')
            n_errors += 1
            continue

        model_name = metadata.get('model', 'unknown')
        model_slug = model_name.split('/')[-1].lower().replace('.', '').replace(' ', '_')

        task_dir = lltk.task_path(_id, task_name)
        dest = os.path.join(task_dir, f'{model_slug}.json')

        if os.path.exists(dest):
            n_skipped += 1
            continue

        if dry_run:
            if verbose:
                log.info(f'[ingest] would place {os.path.basename(path)} -> {dest}')
            n_ingested += 1
            continue

        os.makedirs(task_dir, exist_ok=True)
        shutil.copy2(path, dest)
        n_ingested += 1

        if extractor:
            scalars = extractor(result)
            src = source or f'task:{task_name}:{model_slug}'
            for field, value in scalars:
                anno_batch.append({
                    '_id': _id,
                    'field': field,
                    'value': value,
                })

    if anno_batch and not dry_run:
        from lltk.tools import annotations as A
        src = source or f'task:{task_name}'
        for row in anno_batch:
            if A.field_spec(row['field']) is None:
                A.register_field_spec(row['field'], {
                    'type': 'int', 'vocab': None, 'nullable': True,
                    'range': (0, 100_000), 'normalize': None,
                })
        A.write(source=src, rows=anno_batch, validate=True)

    stats = {'n_ingested': n_ingested, 'n_skipped': n_skipped, 'n_errors': n_errors}
    if verbose:
        log.info(
            f'[ingest] done: {n_ingested} placed, '
            f'{n_skipped} already existed, {n_errors} errors'
        )
    return stats


# ── Convenience wrappers ──────────────────────────────────────────────

def classify_genre(
    ids: list[str] | None = None,
    corpus: str | None = None,
    model: str = 'gemini-2.5-flash',
    **kwargs,
) -> dict:
    """Convenience: run genre classification."""
    return run_task('genre', ids=ids, corpus=corpus, model=model, **kwargs)


def extract_social_network(
    ids: list[str] | None = None,
    model: str = 'gemini-2.5-pro',
    **kwargs,
) -> dict:
    """Convenience: run social network extraction."""
    return run_task(
        'social_network', ids=ids, model=model,
        save_tasks=True, **kwargs,
    )
