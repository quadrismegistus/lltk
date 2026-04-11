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

from lltk.tools.metadb import GENRE_VOCAB

WEB_DIR = Path(__file__).parent
TEMPLATES_DIR = WEB_DIR / 'templates'
STATIC_DIR = WEB_DIR / 'static'


def create_app():
    """Create the LLTK explorer FastAPI app."""
    from lltk.tools.metadb import MetaDB

    app = FastAPI(title='LLTK Explorer')
    app.mount('/static', StaticFiles(directory=str(STATIC_DIR)), name='static')

    # Read-only DB so the app can run concurrently with db-wordindex/db-rebuild
    db = MetaDB(read_only=True)

    # ── HTML shell ──────────────────────────────────────────────────────

    @app.get('/', response_class=HTMLResponse)
    async def index():
        html_path = TEMPLATES_DIR / 'app.html'
        return html_path.read_text()

    # ── Global stats ────────────────────────────────────────────────────

    @app.get('/api/stats')
    async def get_stats():
        try:
            from datetime import date
            current_year = date.today().year
            row = db.conn.execute(
                "SELECT COUNT(*) as n, COUNT(DISTINCT corpus) as n_corpora, "
                "MIN(year) as year_min, "
                f"MAX(CASE WHEN year <= {current_year} THEN year END) as year_max "
                "FROM texts"
            ).fetchone()
            # match groups
            try:
                mg = db.match_conn.execute(
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
            # Manifest info (name, desc) keyed by corpus id
            from lltk.corpus.utils import load_manifest
            manifest = load_manifest()
            manifest_by_id = {}
            for section, vals in manifest.items():
                cid = vals.get('id', '')
                manifest_by_id[cid] = {
                    'name': vals.get('name', section),
                    'desc': vals.get('desc', ''),
                    'link': vals.get('link', ''),
                }

            # Per-corpus stats
            corpus_stats = db.conn.execute("""
                SELECT corpus,
                       COUNT(*) as n_texts,
                       MIN(year) as year_min,
                       MAX(CASE WHEN year <= 2030 THEN year END) as year_max,
                       COUNT(path_freqs) as n_freqs,
                       COUNT(n_words) as n_words
                FROM texts GROUP BY corpus ORDER BY corpus
            """).fetchdf().to_dict('records')

            # Genre breakdown per corpus
            genre_rows = db.conn.execute("""
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
                info = manifest_by_id.get(cs['corpus'], {})
                cs['name'] = info.get('name', cs['corpus'])
                cs['desc'] = info.get('desc', '')
                cs['link'] = info.get('link', '')

            return {'corpora': corpus_stats}
        except Exception as e:
            return JSONResponse({'error': str(e)}, status_code=500)

    # ── Heatmap (genre x decade) ────────────────────────────────────────

    @app.get('/api/heatmap')
    async def get_heatmap():
        try:
            df = db.conn.execute("""
                SELECT genre, CAST(year / 100 AS INTEGER) * 100 as century, COUNT(*) as n
                FROM texts
                WHERE year IS NOT NULL AND genre IS NOT NULL
                  AND year >= 1400 AND year <= 2100
                GROUP BY genre, century
                ORDER BY century, genre
            """).fetchdf()
            return {'cells': df.to_dict('records')}
        except Exception as e:
            return JSONResponse({'error': str(e)}, status_code=500)

    # ── Genre timeline (stacked area by decade) ──────────────────────────

    @app.get('/api/genre-timeline')
    async def get_genre_timeline(corpus: str = Query('', description='Filter by corpus')):
        try:
            corpus_filter = f"AND corpus = '{corpus}'" if corpus else ""
            df = db.conn.execute(f"""
                SELECT CAST(FLOOR(year / 10.0) * 10 AS INTEGER) as decade,
                       genre,
                       COUNT(*) as n
                FROM texts
                WHERE year IS NOT NULL AND year != 0 AND genre IS NOT NULL
                  AND year <= 2030
                  {corpus_filter}
                GROUP BY decade, genre
                ORDER BY decade, genre
            """).fetchdf()
            # Pivot: [{decade, Fiction, Poetry, Drama, ...}, ...]
            decades = sorted(df['decade'].unique())
            genres = sorted(df['genre'].unique())
            lookup = {}
            for _, r in df.iterrows():
                lookup[(int(r['decade']), r['genre'])] = int(r['n'])
            rows = []
            for d in decades:
                row = {'decade': int(d)}
                for g in genres:
                    row[g] = lookup.get((d, g), 0)
                rows.append(row)
            return {'data': rows, 'genres': genres}
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
                dedup_clause = db._dedup_sql(where, dedup_by=dedup_by)
            else:
                join = ""
                dedup_clause = ""

            # Count
            count_sql = f"SELECT COUNT(*) FROM texts t {join} WHERE {where} {dedup_clause}"
            total = db.conn.execute(count_sql).fetchone()[0]

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
            rows = db.conn.execute(select_sql).fetchdf().to_dict('records')

            return {
                'texts': rows,
                'total': total,
                'page': page,
                'per_page': per_page,
                'total_pages': max(1, (total + per_page - 1) // per_page),
            }
        except Exception as e:
            return JSONResponse({'error': str(e)}, status_code=500)

    @app.get('/api/texts/download')
    async def download_texts(
        search: str = Query('', description='Search title/author'),
        corpus: str = Query('', description='Filter by corpus'),
        genre: str = Query('', description='Filter by genre'),
        year_min: Optional[int] = Query(None),
        year_max: Optional[int] = Query(None),
        dedup: bool = Query(False),
        dedup_by: str = Query('rank', description='rank or oldest'),
        has_freqs: bool = Query(False),
    ):
        from fastapi.responses import StreamingResponse
        import io, json
        import pandas as pd
        MAX_ROWS = 100_000
        try:
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

            if dedup:
                join = "LEFT JOIN match_db.match_groups mg ON t._id = mg._id"
                dedup_clause = db._dedup_sql(where, dedup_by=dedup_by)
            else:
                join = ""
                dedup_clause = ""

            df = db.conn.execute(f"""
                SELECT t.* FROM texts t {join}
                WHERE {where} {dedup_clause}
                ORDER BY t.year ASC NULLS LAST, t.title
                LIMIT {MAX_ROWS}
            """).fetchdf()
            if not len(df):
                return JSONResponse({'error': 'No results'}, status_code=404)
            # Expand meta JSON
            if 'meta' in df.columns:
                meta_dicts = df['meta'].apply(lambda x: json.loads(x) if x else {})
                meta_df = pd.DataFrame(meta_dicts.tolist())
                meta_df = meta_df.drop(columns=[c for c in meta_df.columns if c in df.columns], errors='ignore')
                df = pd.concat([df.drop(columns=['meta']), meta_df], axis=1)
            buf = io.StringIO()
            df.to_csv(buf, index=False)
            buf.seek(0)
            name = corpus or 'lltk'
            return StreamingResponse(
                buf, media_type='text/csv',
                headers={'Content-Disposition': f'attachment; filename="{name}_metadata.csv"'}
            )
        except Exception as e:
            return JSONResponse({'error': str(e)}, status_code=500)

    # ── Single text detail ──────────────────────────────────────────────

    @app.get('/api/text/{_id:path}')
    async def get_text(_id: str):
        row = db.get(_id)
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
            mg = db.get_group(_id)
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
            df = db.conn.execute("""
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
            stats = db.conn.execute("""
                SELECT COUNT(*) as n_texts,
                       MIN(CASE WHEN year != 0 THEN year END) as year_min,
                       MAX(CASE WHEN year != 0 AND year <= 2030 THEN year END) as year_max,
                       COUNT(path_freqs) as n_freqs
                FROM texts WHERE corpus = ?
            """, [corpus_id]).fetchone()
            if stats[0] == 0:
                return JSONResponse({'error': 'Corpus not found'}, status_code=404)

            # Genre breakdown (include NULL as 'Unknown')
            genres = db.conn.execute("""
                SELECT COALESCE(genre, 'Unknown') as genre, COUNT(*) as n FROM texts
                WHERE corpus = ?
                GROUP BY 1 ORDER BY n DESC
            """, [corpus_id]).fetchdf().to_dict('records')

            # Year histogram (by decade)
            years = db.conn.execute("""
                SELECT CAST(FLOOR(year / 10.0) * 10 AS INTEGER) as decade, COUNT(*) as n FROM texts
                WHERE corpus = ? AND year IS NOT NULL AND year != 0 AND year <= 2030
                GROUP BY decade ORDER BY decade
            """, [corpus_id]).fetchdf().to_dict('records')

            # Top authors (normalize: strip trailing punct, title case, merge)
            import re
            from collections import Counter
            raw_authors = db.conn.execute("""
                SELECT author, COUNT(*) as n FROM texts
                WHERE corpus = ? AND author IS NOT NULL AND TRIM(author) != ''
                GROUP BY author
            """, [corpus_id]).fetchdf()
            author_counts = Counter()
            for _, row in raw_authors.iterrows():
                a = row['author'].strip().rstrip('.,').strip()
                # Title case if ALL CAPS or all lower
                if a == a.upper() or a == a.lower():
                    a = a.title()
                author_counts[a] += row['n']
            authors = [{'author': a, 'n': int(n)} for a, n in author_counts.most_common(20)]

            # Manifest info
            from lltk.corpus.utils import load_manifest
            manifest = load_manifest()
            minfo = {}
            for section, vals in manifest.items():
                if vals.get('id', '') == corpus_id:
                    minfo = {'name': vals.get('name', section), 'desc': vals.get('desc', ''), 'link': vals.get('link', '')}
                    break

            return {
                'corpus': corpus_id,
                'name': minfo.get('name', corpus_id),
                'desc': minfo.get('desc', ''),
                'link': minfo.get('link', ''),
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

    @app.get('/api/corpus/{corpus_id}/download')
    async def download_corpus_metadata(corpus_id: str):
        from fastapi.responses import StreamingResponse
        import io, json
        import pandas as pd
        try:
            df = db.conn.execute("""
                SELECT * FROM texts WHERE corpus = ?
                ORDER BY year, title
            """, [corpus_id]).fetchdf()
            # Expand meta JSON into separate columns
            if 'meta' in df.columns:
                meta_dicts = df['meta'].apply(lambda x: json.loads(x) if x else {})
                meta_df = pd.DataFrame(meta_dicts.tolist())
                # Drop columns already in the main table
                meta_df = meta_df.drop(columns=[c for c in meta_df.columns if c in df.columns], errors='ignore')
                df = pd.concat([df.drop(columns=['meta']), meta_df], axis=1)
            if not len(df):
                return JSONResponse({'error': 'Corpus not found'}, status_code=404)
            buf = io.StringIO()
            df.to_csv(buf, index=False)
            buf.seek(0)
            return StreamingResponse(
                buf,
                media_type='text/csv',
                headers={'Content-Disposition': f'attachment; filename="{corpus_id}_metadata.csv"'}
            )
        except Exception as e:
            return JSONResponse({'error': str(e)}, status_code=500)

    # ── Genres list ─────────────────────────────────────────────────────

    @app.get('/api/genres')
    async def get_genres():
        return {'genres': sorted(GENRE_VOCAB)}

    # ── Ngram API ───────────────────────────────────────────────────────

    @app.get('/api/ngram')
    async def get_ngram(
        words: str = Query('', description='Comma-separated words'),
        genre: str = Query(''),
        corpus: str = Query(''),
        year_min: int = Query(1500),
        year_max: int = Query(2020),
        normalize: str = Query('per_million', description='per_million or raw'),
        dedup: bool = Query(False),
        dedup_by: str = Query('rank', description='rank or oldest'),
        by_corpus: bool = Query(False, description='Break down by corpus'),
    ):
        if not words.strip():
            return {'data': [], 'words': [], 'series': []}
        if not db.has_word_index():
            return JSONResponse({'error': 'Word index not built. Run: lltk db-wordindex'}, status_code=404)
        try:
            df = db.ngram(
                words, genre=genre or None, corpus=corpus or None,
                year_min=year_min, year_max=year_max, normalize=normalize,
                dedup=dedup, dedup_by=dedup_by, by_corpus=by_corpus,
            )
            if df.empty:
                return {'data': [], 'words': [w.strip() for w in words.split(',')], 'series': []}

            word_list = [w.strip().lower() for w in words.split(',')]

            if by_corpus:
                # Series are word:corpus combinations
                series = sorted(set(
                    f"{r['word']}:{r['corpus']}" for _, r in df.iterrows()
                ))
                periods = sorted(df['period'].unique())
                rows = []
                for p in periods:
                    row = {'period': int(p)}
                    pdf = df[df['period'] == p]
                    for _, r in pdf.iterrows():
                        key = f"{r['word']}:{r['corpus']}"
                        row[key] = float(r['value']) if r['value'] is not None else 0
                        row[f'{key}_count'] = int(r['raw_count'])
                        row[f'{key}_texts'] = int(r['n_texts'])
                    rows.append(row)
                return {'data': rows, 'words': word_list, 'series': series}
            else:
                periods = sorted(df['period'].unique())
                rows = []
                for p in periods:
                    row = {'period': int(p)}
                    pdf = df[df['period'] == p]
                    for _, r in pdf.iterrows():
                        row[r['word']] = float(r['value']) if r['value'] is not None else 0
                        row[f'{r["word"]}_count'] = int(r['raw_count'])
                        row[f'{r["word"]}_texts'] = int(r['n_texts'])
                    rows.append(row)
                return {'data': rows, 'words': word_list, 'series': word_list}
        except Exception as e:
            return JSONResponse({'error': str(e)}, status_code=500)

    @app.get('/api/ngram/{word}/examples')
    async def get_ngram_examples(
        word: str,
        genre: str = Query(''),
        corpus: str = Query(''),
        year_min: Optional[int] = Query(None),
        year_max: Optional[int] = Query(None),
        limit: int = Query(20, ge=1, le=100),
        dedup: bool = Query(False),
        dedup_by: str = Query('rank'),
    ):
        if not db.has_word_index():
            return JSONResponse({'error': 'Word index not built'}, status_code=404)
        try:
            df = db.ngram_examples(
                word, genre=genre or None, corpus=corpus or None,
                year_min=year_min, year_max=year_max, limit=limit,
                dedup=dedup, dedup_by=dedup_by,
            )
            return {'examples': df.to_dict('records'), 'word': word}
        except Exception as e:
            return JSONResponse({'error': str(e)}, status_code=500)

    @app.get('/api/ngram/{word}/collocates')
    async def get_ngram_collocates(
        word: str,
        genre: str = Query(''),
        corpus: str = Query(''),
        year_min: Optional[int] = Query(None),
        year_max: Optional[int] = Query(None),
        limit: int = Query(50, ge=1, le=200),
        dedup: bool = Query(False),
        dedup_by: str = Query('rank'),
    ):
        if not db.has_word_index():
            return JSONResponse({'error': 'Word index not built'}, status_code=404)
        try:
            df = db.ngram_collocates(
                word, genre=genre or None, corpus=corpus or None,
                year_min=year_min, year_max=year_max, limit=limit,
                dedup=dedup, dedup_by=dedup_by,
            )
            return {'collocates': df.to_dict('records'), 'word': word}
        except Exception as e:
            return JSONResponse({'error': str(e)}, status_code=500)

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

            df = db.find_matches(search)
            if df.empty:
                return {'groups': [], 'total_groups': 0}

            # Group by group_id, cap members at 20
            MAX_MEMBERS = 20
            groups = []
            for gid, gdf in df.groupby('group_id'):
                members = gdf.to_dict('records')
                groups.append({
                    'group_id': int(gid),
                    'members': members[:MAX_MEMBERS],
                    'total_members': len(members),
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
            stats = db.match_stats()
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
            df = db.match_conn.execute("""
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
