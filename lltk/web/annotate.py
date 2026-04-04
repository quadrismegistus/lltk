"""
LLTK Corpus Annotation Web App.

Local FastAPI server for browsing and annotating CuratedCorpus metadata.
Annotations stored as JSON, merged with DuckDB at load time.

Usage:
    lltk annotate arc_fiction
    lltk annotate arc_fiction --port 9000
"""

import json
import os
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

import lltk
from lltk.tools.metadb import GENRE_VOCAB

WEB_DIR = Path(__file__).parent
TEMPLATES_DIR = WEB_DIR / 'templates'


def create_app(corpus_id: str):
    """Create a FastAPI app for annotating a specific corpus."""

    app = FastAPI(title=f'LLTK Annotate: {corpus_id}')
    app.mount('/static', StaticFiles(directory=str(WEB_DIR / 'static')), name='static')

    # Load corpus and annotations
    corpus = lltk.load(corpus_id)
    annotations_path = os.path.join(
        os.path.expanduser(lltk.imports.PATH_CORPUS),
        corpus_id, 'annotations.json'
    )

    def _load_annotations():
        if os.path.exists(annotations_path):
            with open(annotations_path) as f:
                return json.load(f)
        return {}

    def _save_annotations(data):
        os.makedirs(os.path.dirname(annotations_path), exist_ok=True)
        with open(annotations_path, 'w') as f:
            json.dump(data, f, indent=2, default=str)

    annotations = _load_annotations()

    # ── HTML pages ───────────────────────────────────────────────

    @app.get('/', response_class=HTMLResponse)
    async def table_view():
        template = (TEMPLATES_DIR / 'annotate.html').read_text()
        return template.replace('{{CORPUS_ID}}', corpus_id).replace(
            '{{GENRE_VOCAB}}', json.dumps(sorted(GENRE_VOCAB))
        )

    # ── API routes ───────────────────────────────────────────────

    @app.get('/api/texts')
    async def get_texts(
        page: int = 1,
        per_page: int = 100,
        corpus_filter: Optional[str] = None,
        genre: Optional[str] = None,
        year_min: Optional[int] = None,
        year_max: Optional[int] = None,
        is_translated: Optional[bool] = None,
        show_excluded: bool = False,
        show_annotated: Optional[bool] = None,
        search: Optional[str] = None,
        sort_by: str = 'year',
        sort_dir: str = 'asc',
    ):
        """Paginated text list with filters."""
        clauses = []
        if corpus_filter:
            clauses.append(f"t.corpus = '{corpus_filter}'")
        if genre:
            clauses.append(f"t.genre = '{genre}'")
        if year_min is not None:
            clauses.append(f't.year >= {year_min}')
        if year_max is not None:
            clauses.append(f't.year <= {year_max}')
        if is_translated is not None:
            clauses.append(f't.is_translated = {str(is_translated).lower()}')
        if search:
            safe = search.replace("'", "''")
            clauses.append(f"(t.title ILIKE '%{safe}%' OR t.author ILIKE '%{safe}%')")

        # Build source filter from corpus SOURCES
        source_filter = ''
        if hasattr(corpus, 'SOURCES') and corpus.SOURCES:
            source_clauses = []
            for cid, filters in corpus.SOURCES.items():
                parts = [f"t.corpus = '{cid}'"]
                for k, v in filters.items():
                    parts.append(f"t.{k} = '{v}'")
                source_clauses.append('(' + ' AND '.join(parts) + ')')
            source_filter = '(' + ' OR '.join(source_clauses) + ')'

        where_parts = [source_filter] if source_filter else []
        where_parts.extend(clauses)
        where = ' AND '.join(where_parts) if where_parts else '1=1'

        # Sort
        valid_sorts = {'year', 'title', 'author', 'corpus', 'genre'}
        if sort_by not in valid_sorts:
            sort_by = 'year'
        order = f't.{sort_by} {"ASC" if sort_dir == "asc" else "DESC"} NULLS LAST'

        # Dedup: keep only best representative per match group
        dedup_sql = lltk.db._dedup_sql(where, 'rank', texts_table='texts')
        dedup_join = "LEFT JOIN match_db.match_groups mg ON t._id = mg._id"

        # Count
        count_sql = f"SELECT COUNT(*) FROM texts t {dedup_join} WHERE {where} {dedup_sql}"
        total = lltk.db.conn.execute(count_sql).fetchone()[0]

        # Fetch page
        offset = (page - 1) * per_page
        sql = f"""
            SELECT t._id, t.corpus, t.id, t.title, t.author, t.year,
                   t.genre, t.genre_raw, t.is_translated, t.title_norm, t.author_norm
            FROM texts t
            {dedup_join}
            WHERE {where} {dedup_sql}
            ORDER BY {order}
            LIMIT {per_page} OFFSET {offset}
        """
        rows = lltk.db.conn.execute(sql).fetchdf()

        # Merge annotations
        texts = []
        for _, row in rows.iterrows():
            d = row.to_dict()
            _id = d['_id']
            ann = annotations.get(_id, {})
            d['exclude'] = ann.get('exclude', '')
            d['notes'] = ann.get('notes', '')
            d['is_annotated'] = bool(ann)
            # Apply annotation overrides for display
            for k, v in ann.items():
                if k in d:
                    d[k] = v
            texts.append(d)

        # Filter excluded unless requested
        if not show_excluded:
            texts = [t for t in texts if not t.get('exclude')]

        # Filter annotated-only
        if show_annotated is True:
            texts = [t for t in texts if t.get('is_annotated')]
        elif show_annotated is False:
            texts = [t for t in texts if not t.get('is_annotated')]

        return {
            'texts': texts,
            'total': total,
            'page': page,
            'per_page': per_page,
            'total_pages': (total + per_page - 1) // per_page,
        }

    @app.get('/api/text/{_id:path}')
    async def get_text(_id: str):
        """Single text detail with full metadata and txt preview."""
        row = lltk.db.get(_id)
        if not row:
            return JSONResponse({'error': 'Not found'}, status_code=404)

        # Get txt preview
        txt_preview = ''
        try:
            from lltk.corpus.corpus import Corpus
            parts = _id.lstrip('_').split('/', 1)
            if len(parts) == 2:
                t = Corpus(parts[0]).text(parts[1])
                txt = t.txt
                if txt:
                    txt_preview = txt[:50000]  # ~10K words
        except Exception:
            pass

        # Get match group
        match_group = []
        try:
            mg = lltk.db.get_group(_id)
            if len(mg):
                match_group = mg.to_dict('records')
        except Exception:
            pass

        # Merge annotation
        ann = annotations.get(_id, {})

        return {
            'metadata': row,
            'annotations': ann,
            'txt_preview': txt_preview,
            'match_group': match_group,
        }

    @app.post('/api/text/{_id:path}/annotate')
    async def annotate_text(_id: str, request: Request):
        """Save annotation for one text."""
        body = await request.json()

        # Remove empty values
        ann = {k: v for k, v in body.items() if v not in (None, '', False)}

        if ann:
            annotations[_id] = ann
        elif _id in annotations:
            del annotations[_id]

        _save_annotations(annotations)
        return {'ok': True, '_id': _id, 'annotations': annotations.get(_id, {})}

    @app.post('/api/texts/bulk-annotate')
    async def bulk_annotate(request: Request):
        """Bulk annotate multiple texts."""
        body = await request.json()
        ids = body.get('ids', [])
        updates = body.get('updates', {})
        updates = {k: v for k, v in updates.items() if v not in (None,)}

        for _id in ids:
            if _id not in annotations:
                annotations[_id] = {}
            annotations[_id].update(updates)
            # Clean empty
            annotations[_id] = {k: v for k, v in annotations[_id].items() if v not in (None, '', False)}
            if not annotations[_id]:
                del annotations[_id]

        _save_annotations(annotations)
        return {'ok': True, 'count': len(ids)}

    @app.get('/api/stats')
    async def get_stats():
        """Annotation progress stats."""
        total_annotated = len(annotations)
        total_excluded = sum(1 for a in annotations.values() if a.get('exclude'))
        genre_overrides = {}
        for a in annotations.values():
            g = a.get('genre')
            if g:
                genre_overrides[g] = genre_overrides.get(g, 0) + 1

        # Get corpus-level counts
        corpus_counts = {}
        if hasattr(corpus, 'SOURCES') and corpus.SOURCES:
            for cid in corpus.SOURCES:
                n = lltk.db.conn.execute(
                    'SELECT COUNT(*) FROM texts WHERE corpus = ?', [cid]
                ).fetchone()[0]
                corpus_counts[cid] = n

        return {
            'total_annotated': total_annotated,
            'total_excluded': total_excluded,
            'genre_overrides': genre_overrides,
            'corpus_counts': corpus_counts,
        }

    @app.get('/api/corpora')
    async def get_corpora():
        """List source corpora for this CuratedCorpus."""
        if hasattr(corpus, 'SOURCES') and corpus.SOURCES:
            return {'corpora': list(corpus.SOURCES.keys())}
        return {'corpora': []}

    @app.get('/api/genres')
    async def get_genres():
        return {'genres': sorted(GENRE_VOCAB)}

    @app.post('/api/match/link')
    async def link_texts(request: Request):
        """Manually link two texts as duplicates. Recomputes match groups."""
        body = await request.json()
        id_a = body.get('id_a', '')
        id_b = body.get('id_b', '')
        if not id_a or not id_b or id_a == id_b:
            return JSONResponse({'error': 'Need two different _id values'}, status_code=400)

        # Ensure canonical order
        if id_a > id_b:
            id_a, id_b = id_b, id_a

        # Insert into matches table
        try:
            lltk.db.conn.execute("""
                INSERT OR IGNORE INTO match_db.matches (_id_a, _id_b, similarity, match_type)
                VALUES (?, ?, 1.0, 'manual')
            """, [id_a, id_b])
        except Exception as e:
            return JSONResponse({'error': str(e)}, status_code=500)

        # Recompute match groups
        lltk.db._compute_match_groups()

        return {'ok': True, 'id_a': id_a, 'id_b': id_b}

    @app.post('/api/match/unlink')
    async def unlink_texts(request: Request):
        """Remove a manual match between two texts. Recomputes match groups."""
        body = await request.json()
        id_a = body.get('id_a', '')
        id_b = body.get('id_b', '')
        if not id_a or not id_b:
            return JSONResponse({'error': 'Need two _id values'}, status_code=400)

        # Delete both orderings
        lltk.db.conn.execute("""
            DELETE FROM match_db.matches
            WHERE (_id_a = ? AND _id_b = ?) OR (_id_a = ? AND _id_b = ?)
        """, [id_a, id_b, id_b, id_a])

        lltk.db._compute_match_groups()
        return {'ok': True}

    @app.get('/api/search-texts')
    async def search_texts(q: str = ''):
        """Quick search for linking — returns top 10 matches by title."""
        if not q or len(q) < 2:
            return {'results': []}
        safe = q.replace("'", "''")
        rows = lltk.db.conn.execute(f"""
            SELECT _id, corpus, title, author, year
            FROM texts
            WHERE title ILIKE '%{safe}%' OR author ILIKE '%{safe}%'
            ORDER BY year
            LIMIT 10
        """).fetchdf()
        return {'results': rows.to_dict('records')}

    @app.get('/api/annotation-keys')
    async def get_annotation_keys():
        """Return all custom annotation keys used across all annotations."""
        fixed = {'genre', 'genre_raw', 'is_translated', 'exclude', 'notes'}
        custom = set()
        for ann in annotations.values():
            custom.update(ann.keys())
        custom -= fixed
        return {'fixed': sorted(fixed), 'custom': sorted(custom)}

    @app.get('/api/genre-raw-values')
    async def get_genre_raw_values():
        """Return all distinct genre_raw values from DB + annotations."""
        # From DB
        try:
            rows = lltk.db.conn.execute(
                "SELECT DISTINCT genre_raw FROM texts WHERE genre_raw IS NOT NULL ORDER BY genre_raw"
            ).fetchall()
            db_values = {r[0] for r in rows if r[0]}
        except Exception:
            db_values = set()
        # From annotations
        ann_values = {a.get('genre_raw') for a in annotations.values() if a.get('genre_raw')}
        all_values = sorted(db_values | ann_values)
        return {'values': all_values}

    return app


def run_annotate(corpus_id: str, port: int = 8989, host: str = '0.0.0.0'):
    """Run the annotation server."""
    import uvicorn
    app = create_app(corpus_id)
    print(f'\n  LLTK Annotate: {corpus_id}')
    print(f'  http://{host}:{port}\n')
    uvicorn.run(app, host=host, port=port, log_level='warning')
