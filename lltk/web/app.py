"""
LLTK Corpus Explorer Web App.

FastAPI server for browsing and exploring all LLTK corpora via the DuckDB
metadata store. Read-only exploration — browse corpora, search/filter texts,
inspect match groups, see corpus overlap.

Usage:
    lltk app
    lltk app --port 9000
"""

import json
import os
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

import lltk
from lltk.tools.metadb import GENRE_VOCAB

WEB_DIR = Path(__file__).parent
TEMPLATES_DIR = WEB_DIR / 'templates'
STATIC_DIR = WEB_DIR / 'static'


def create_app():
    """Create the LLTK explorer FastAPI app."""

    app = FastAPI(title='LLTK Explorer')
    app.mount('/static', StaticFiles(directory=str(STATIC_DIR)), name='static')

    # ── HTML shell ──────────────────────────────────────────────────────

    @app.get('/', response_class=HTMLResponse)
    async def index():
        html_path = TEMPLATES_DIR / 'app.html'
        return html_path.read_text()

    # ── Global stats ────────────────────────────────────────────────────

    @app.get('/api/stats')
    async def get_stats():
        try:
            row = lltk.db.conn.execute(
                "SELECT COUNT(*) as n, COUNT(DISTINCT corpus) as n_corpora, "
                "MIN(year) as year_min, MAX(year) as year_max FROM texts"
            ).fetchone()
            # match groups
            try:
                mg = lltk.db.match_conn.execute(
                    "SELECT COUNT(DISTINCT group_id) FROM match_db.match_groups"
                ).fetchone()[0]
            except Exception:
                mg = 0
            return {
                'total_texts': row[0],
                'total_corpora': row[1],
                'year_min': row[2],
                'year_max': row[3],
                'total_match_groups': mg,
            }
        except Exception as e:
            return JSONResponse({'error': str(e)}, status_code=500)

    # ── Overview (per-corpus summaries) ─────────────────────────────────

    @app.get('/api/overview')
    async def get_overview():
        try:
            # Per-corpus stats
            corpus_stats = lltk.db.conn.execute("""
                SELECT corpus,
                       COUNT(*) as n_texts,
                       MIN(year) as year_min,
                       MAX(year) as year_max,
                       COUNT(path_freqs) as n_freqs,
                       COUNT(n_words) as n_words
                FROM texts GROUP BY corpus ORDER BY corpus
            """).fetchdf().to_dict('records')

            # Genre breakdown per corpus
            genre_rows = lltk.db.conn.execute("""
                SELECT corpus, genre, COUNT(*) as n
                FROM texts WHERE genre IS NOT NULL
                GROUP BY corpus, genre
            """).fetchdf()
            genre_by_corpus = {}
            for _, r in genre_rows.iterrows():
                genre_by_corpus.setdefault(r['corpus'], {})[r['genre']] = int(r['n'])

            for cs in corpus_stats:
                cs['genres'] = genre_by_corpus.get(cs['corpus'], {})
                cs['n_freqs'] = int(cs['n_freqs'])
                cs['n_words'] = int(cs['n_words'])

            return {'corpora': corpus_stats}
        except Exception as e:
            return JSONResponse({'error': str(e)}, status_code=500)

    # ── Heatmap (genre x decade) ────────────────────────────────────────

    @app.get('/api/heatmap')
    async def get_heatmap():
        try:
            df = lltk.db.conn.execute("""
                SELECT genre, (year / 10 * 10) as decade, COUNT(*) as n
                FROM texts
                WHERE year IS NOT NULL AND genre IS NOT NULL
                GROUP BY genre, decade
                ORDER BY decade, genre
            """).fetchdf()
            return {'cells': df.to_dict('records')}
        except Exception as e:
            return JSONResponse({'error': str(e)}, status_code=500)

    # ── Paginated text list ─────────────────────────────────────────────

    @app.get('/api/texts')
    async def get_texts(
        page: int = Query(1, ge=1),
        per_page: int = Query(100, ge=1, le=500),
        search: str = Query('', description='Search title/author'),
        corpus: str = Query('', description='Filter by corpus'),
        genre: str = Query('', description='Filter by genre'),
        year_min: Optional[int] = Query(None),
        year_max: Optional[int] = Query(None),
        sort_by: str = Query('year', description='Sort column'),
        sort_dir: str = Query('asc', description='asc or desc'),
        dedup: bool = Query(False),
        dedup_by: str = Query('rank', description='rank or oldest'),
        has_freqs: bool = Query(False, description='Only texts with freqs'),
    ):
        try:
            # Build WHERE
            clauses = []
            if search:
                escaped = search.replace("'", "''")
                clauses.append(f"(t.title ILIKE '%{escaped}%' OR t.author ILIKE '%{escaped}%')")
            if corpus:
                clauses.append(f"t.corpus = '{corpus}'")
            if genre:
                clauses.append(f"t.genre = '{genre}'")
            if year_min is not None:
                clauses.append(f"t.year >= {int(year_min)}")
            if year_max is not None:
                clauses.append(f"t.year <= {int(year_max)}")
            if has_freqs:
                clauses.append("t.path_freqs IS NOT NULL")

            where = ' AND '.join(clauses) if clauses else '1=1'

            # Validate sort
            allowed_sorts = {'title', 'author', 'year', 'genre', 'corpus', 'n_words', 'genre_raw'}
            if sort_by not in allowed_sorts:
                sort_by = 'year'
            if sort_dir not in ('asc', 'desc'):
                sort_dir = 'asc'

            # Dedup join
            if dedup:
                join = "LEFT JOIN match_db.match_groups mg ON t._id = mg._id"
                dedup_clause = lltk.db._dedup_sql(where, dedup_by=dedup_by)
            else:
                join = ""
                dedup_clause = ""

            # Count
            count_sql = f"SELECT COUNT(*) FROM texts t {join} WHERE {where} {dedup_clause}"
            total = lltk.db.conn.execute(count_sql).fetchone()[0]

            # Fetch page
            offset = (page - 1) * per_page
            select_sql = f"""
                SELECT t._id, t.corpus, t.id, t.title, t.author, t.year,
                       t.genre, t.genre_raw, t.n_words
                FROM texts t {join}
                WHERE {where} {dedup_clause}
                ORDER BY t.{sort_by} {'ASC' if sort_dir == 'asc' else 'DESC'} NULLS LAST
                LIMIT {per_page} OFFSET {offset}
            """
            rows = lltk.db.conn.execute(select_sql).fetchdf().to_dict('records')

            return {
                'texts': rows,
                'total': total,
                'page': page,
                'per_page': per_page,
                'total_pages': max(1, (total + per_page - 1) // per_page),
            }
        except Exception as e:
            return JSONResponse({'error': str(e)}, status_code=500)

    # ── Single text detail ──────────────────────────────────────────────

    @app.get('/api/text/{_id:path}')
    async def get_text(_id: str):
        row = lltk.db.get(_id)
        if not row:
            return JSONResponse({'error': 'Not found'}, status_code=404)

        # Text preview
        txt_preview = ''
        try:
            from lltk.corpus.corpus import Corpus
            parts = _id.lstrip('_').split('/', 1)
            if len(parts) == 2:
                t = Corpus(parts[0]).text(parts[1])
                txt = t.txt
                if txt:
                    txt_preview = txt[:50000]
        except Exception:
            pass

        # Match group
        match_group = []
        try:
            mg = lltk.db.get_group(_id)
            if len(mg):
                match_group = mg.to_dict('records')
        except Exception:
            pass

        return {
            'metadata': row,
            'txt_preview': txt_preview,
            'match_group': match_group,
        }

    # ── Corpora list ────────────────────────────────────────────────────

    @app.get('/api/corpora')
    async def get_corpora():
        try:
            df = lltk.db.conn.execute("""
                SELECT corpus,
                       COUNT(*) as n_texts,
                       MIN(year) as year_min,
                       MAX(year) as year_max
                FROM texts GROUP BY corpus ORDER BY corpus
            """).fetchdf()
            return {'corpora': df.to_dict('records')}
        except Exception as e:
            return JSONResponse({'error': str(e)}, status_code=500)

    # ── Single corpus detail ────────────────────────────────────────────

    @app.get('/api/corpus/{corpus_id}')
    async def get_corpus(corpus_id: str):
        try:
            # Basic stats
            stats = lltk.db.conn.execute("""
                SELECT COUNT(*) as n_texts,
                       MIN(year) as year_min, MAX(year) as year_max,
                       COUNT(path_freqs) as n_freqs
                FROM texts WHERE corpus = ?
            """, [corpus_id]).fetchone()
            if stats[0] == 0:
                return JSONResponse({'error': 'Corpus not found'}, status_code=404)

            # Genre breakdown
            genres = lltk.db.conn.execute("""
                SELECT genre, COUNT(*) as n FROM texts
                WHERE corpus = ? AND genre IS NOT NULL
                GROUP BY genre ORDER BY n DESC
            """, [corpus_id]).fetchdf().to_dict('records')

            # Year histogram (by decade)
            years = lltk.db.conn.execute("""
                SELECT (year / 10 * 10) as decade, COUNT(*) as n FROM texts
                WHERE corpus = ? AND year IS NOT NULL
                GROUP BY decade ORDER BY decade
            """, [corpus_id]).fetchdf().to_dict('records')

            # Top authors
            authors = lltk.db.conn.execute("""
                SELECT author, COUNT(*) as n FROM texts
                WHERE corpus = ? AND author IS NOT NULL
                GROUP BY author ORDER BY n DESC LIMIT 20
            """, [corpus_id]).fetchdf().to_dict('records')

            return {
                'corpus': corpus_id,
                'n_texts': stats[0],
                'year_min': stats[1],
                'year_max': stats[2],
                'n_freqs': stats[3],
                'genres': genres,
                'years': years,
                'authors': authors,
            }
        except Exception as e:
            return JSONResponse({'error': str(e)}, status_code=500)

    # ── Genres list ─────────────────────────────────────────────────────

    @app.get('/api/genres')
    async def get_genres():
        return {'genres': sorted(GENRE_VOCAB)}

    # ── Match browser ───────────────────────────────────────────────────

    @app.get('/api/matches')
    async def get_matches(
        search: str = Query('', description='Search by title'),
        page: int = Query(1, ge=1),
        per_page: int = Query(50, ge=1, le=200),
    ):
        try:
            if not search:
                return {'groups': [], 'total_groups': 0}

            df = lltk.db.find_matches(search)
            if df.empty:
                return {'groups': [], 'total_groups': 0}

            # Group by group_id
            groups = []
            for gid, gdf in df.groupby('group_id'):
                groups.append({
                    'group_id': int(gid),
                    'members': gdf.to_dict('records'),
                })

            total = len(groups)
            start = (page - 1) * per_page
            end = start + per_page

            return {
                'groups': groups[start:end],
                'total_groups': total,
                'page': page,
                'per_page': per_page,
            }
        except Exception as e:
            return JSONResponse({'error': str(e)}, status_code=500)

    # ── Match stats ─────────────────────────────────────────────────────

    @app.get('/api/match-stats')
    async def get_match_stats():
        try:
            stats = lltk.db.match_stats()
            return {
                'total_matches': stats['total_matches'],
                'total_groups': stats['total_groups'],
                'by_type': stats['by_type'].to_dict('records'),
                'group_sizes': stats['group_sizes'].to_dict('records'),
            }
        except Exception as e:
            return JSONResponse({'error': str(e)}, status_code=500)

    # ── Corpus overlap ──────────────────────────────────────────────────

    @app.get('/api/corpus-overlap')
    async def get_corpus_overlap():
        try:
            df = lltk.db.match_conn.execute("""
                SELECT t1.corpus as corpus_a, t2.corpus as corpus_b, COUNT(*) as n
                FROM match_db.matches m
                JOIN texts t1 ON m._id_a = t1._id
                JOIN texts t2 ON m._id_b = t2._id
                WHERE t1.corpus != t2.corpus
                GROUP BY t1.corpus, t2.corpus
                ORDER BY n DESC
            """).fetchdf()
            return {'overlaps': df.to_dict('records')}
        except Exception as e:
            return JSONResponse({'error': str(e)}, status_code=500)

    return app


def run_app(port: int = 8899, host: str = '0.0.0.0', reload: bool = True):
    """Run the explorer server."""
    import uvicorn
    print(f'\n  LLTK Explorer')
    print(f'  http://{host}:{port}\n')
    if reload:
        uvicorn.run(
            'lltk.web.app:app_from_env',
            host=host, port=port, log_level='warning', reload=True,
            reload_dirs=[str(Path(__file__).parent)],
        )
    else:
        app = create_app()
        uvicorn.run(app, host=host, port=port, log_level='warning')


def app_from_env():
    """Factory for uvicorn reload mode."""
    return create_app()
