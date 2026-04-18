"""
Genre enrichment + translation detection — ClickHouse-backed.

Writes per-text overrides to lltk.text_genres and lltk.text_translations
rather than UPDATE-ing lltk.texts (ClickHouse ALTER UPDATE is async and
expensive for many rows).

enrich_genres_ch:
  Starts from the corpus genre (baseline).
  For match groups containing an authority-corpus member (fiction_biblio,
  end, ravengarside), propagates the authority's genre to every group
  member. Highest-priority authority wins within a group.

detect_translations_ch:
  Finds match groups containing texts in multiple languages; the language
  with the earliest year is the original, others are translations.
"""

import pandas as pd
import pyarrow as pa

from lltk.tools.metadb import GENRE_AUTHORITY_CORPORA


def enrich_genres_ch(ch_adapter, progress=True):
    """Write enriched genre rows into lltk.text_genres.

    Step 1: baseline — one row per text with genre_corpus = texts.genre
    Step 2: for each match group with an authority corpus member, override
            all group members with the authority's genre (highest priority
            authority wins).
    """
    authority_list = ', '.join(f"'{c}'" for c in GENRE_AUTHORITY_CORPORA.keys())

    # Step 1: baseline rows (one per text, starting from corpus genre)
    print('enrich_genres: baseline from corpus genre...')
    ch_adapter.execute(f"""
        INSERT INTO lltk.text_genres (
            _id, genre, genre_raw, genre_corpus, genre_enriched_source
        )
        SELECT _id, genre, genre_raw, genre, 'corpus'
        FROM lltk.texts FINAL
        WHERE genre != ''
    """)

    # Step 2: authority override via match groups. For each group containing
    # at least one authority text, pick the highest-priority one's genre.
    print('enrich_genres: gathering authority match groups...')
    auth_groups = ch_adapter.query_df(f"""
        SELECT mg.group_id, t.genre AS authority_genre,
               t.genre_raw AS authority_genre_raw,
               t.corpus AS authority_corpus
        FROM (SELECT _id, group_id FROM lltk.match_groups FINAL) AS mg
        INNER JOIN (SELECT _id, corpus, genre, genre_raw FROM lltk.texts FINAL) AS t
          ON mg._id = t._id
        WHERE t.corpus IN ({authority_list})
          AND t.genre != ''
    """)

    if not len(auth_groups):
        print('enrich_genres: no authority-containing match groups found')
        return ch_adapter.query_df(
            "SELECT genre_enriched_source, count() AS n "
            "FROM lltk.text_genres FINAL GROUP BY genre_enriched_source ORDER BY n DESC"
        )

    # Deduplicate by group_id: keep the highest-priority authority
    auth_groups['priority'] = auth_groups['authority_corpus'].map(GENRE_AUTHORITY_CORPORA)
    auth_groups = (auth_groups.sort_values('priority', ascending=False)
                              .drop_duplicates('group_id', keep='first'))
    print(f'enrich_genres: {len(auth_groups):,} match groups have authority members')

    # For each group, pull all member _ids and write override rows
    # Build one big DataFrame of (_id, genre, genre_raw, source) rows then insert.
    group_ids_str = ', '.join(str(gid) for gid in auth_groups['group_id'].tolist())
    if not group_ids_str:
        return
    members = ch_adapter.query_df(f"""
        SELECT _id, group_id FROM lltk.match_groups FINAL
        WHERE group_id IN ({group_ids_str})
    """)

    # Join to get the authority genre per _id
    merged = members.merge(
        auth_groups[['group_id', 'authority_genre', 'authority_genre_raw',
                     'authority_corpus']],
        on='group_id', how='inner',
    )

    # Build override rows
    override_rows = pd.DataFrame({
        '_id': merged['_id'],
        'genre': merged['authority_genre'].astype(str),
        'genre_raw': merged['authority_genre_raw'].astype(str).fillna(''),
        'genre_corpus': '',  # preserved in baseline row; merged table will show it via dedup order
        'genre_enriched_source': 'bibliography:' + merged['authority_corpus'].astype(str),
    })

    # Fill genre_corpus from existing text_genres baseline
    baseline = ch_adapter.query_df(f"""
        SELECT _id, genre_corpus FROM lltk.text_genres FINAL
        WHERE _id IN (SELECT _id FROM lltk.match_groups FINAL
                      WHERE group_id IN ({group_ids_str}))
    """)
    override_rows = override_rows.merge(
        baseline.rename(columns={'genre_corpus': 'gc_base'}),
        on='_id', how='left',
    )
    override_rows['genre_corpus'] = override_rows['gc_base'].fillna('')
    override_rows = override_rows[['_id', 'genre', 'genre_raw', 'genre_corpus',
                                   'genre_enriched_source']]

    # Insert as arrow table — ReplacingMergeTree on _id with enriched_at version
    # will keep this newer row on query.
    print(f'enrich_genres: writing {len(override_rows):,} override rows...')
    tbl = pa.Table.from_pandas(override_rows, preserve_index=False)
    ch_adapter.client.insert_arrow('text_genres', tbl)

    # Also mark authority-corpus texts (even if no match group) as bibliography source
    ch_adapter.execute(f"""
        INSERT INTO lltk.text_genres
            (_id, genre, genre_raw, genre_corpus, genre_enriched_source)
        SELECT _id, genre, genre_raw, genre,
               concat('bibliography:', corpus)
        FROM lltk.texts FINAL
        WHERE corpus IN ({authority_list})
          AND genre != ''
    """)

    stats = ch_adapter.query_df("""
        SELECT genre_enriched_source, count() AS n
        FROM lltk.text_genres FINAL
        GROUP BY genre_enriched_source
        ORDER BY n DESC
    """)
    print('\nGenre enrichment source distribution:')
    print(stats.to_string(index=False))
    return stats


def detect_translations_ch(ch_adapter):
    """Find match groups with 2+ languages; earliest year per language wins.

    Non-winners get is_translated=1 in lltk.text_translations.
    """
    print('detect_translations: gathering cross-language match groups...')

    # One query: get group_id, _id, lang, year for groups with multiple langs
    cross_lang = ch_adapter.query_df("""
        WITH g AS (
            SELECT mg.group_id       AS group_id,
                   t._id             AS _id,
                   t.lang            AS lang,
                   t.year            AS year
            FROM (SELECT _id, group_id FROM lltk.match_groups FINAL) AS mg
            INNER JOIN (
                SELECT _id, lang, year FROM lltk.texts FINAL
                WHERE lang != '' AND lang != 'unknown'
            ) AS t ON mg._id = t._id
        )
        SELECT group_id, _id, lang, year FROM g
        WHERE group_id IN (
            SELECT group_id FROM g
            GROUP BY group_id
            HAVING countDistinct(lang) > 1
        )
    """)

    if not len(cross_lang):
        print('detect_translations: no cross-language match groups found')
        return {}

    # Per group, earliest year per lang (tie-breaker: lang with most texts)
    # Group → winner lang (earliest year; break ties by text count)
    grp_lang_counts = (cross_lang.groupby(['group_id', 'lang'])
                                 .agg(min_year=('year', 'min'),
                                      n_texts=('_id', 'count'))
                                 .reset_index())

    winners = (grp_lang_counts.sort_values(
        ['group_id', 'min_year', 'n_texts'],
        ascending=[True, True, False],
    ).drop_duplicates('group_id', keep='first')
       .set_index('group_id')['lang'].to_dict())

    cross_lang['winner_lang'] = cross_lang['group_id'].map(winners)
    # Anyone whose lang != winner_lang is a translation
    translations = cross_lang[cross_lang['lang'] != cross_lang['winner_lang']].copy()
    translations['is_translated'] = 1
    translations = translations.rename(columns={'winner_lang': 'original_lang'})
    translations = translations[['_id', 'is_translated', 'original_lang']].drop_duplicates('_id')

    print(f'detect_translations: {len(translations):,} texts marked as translations')
    tbl = pa.Table.from_pandas(translations, preserve_index=False)
    ch_adapter.client.insert_arrow('text_translations', tbl)

    # Summary by flow (original_lang → translated lang, inferred from winner)
    flow_counts = (cross_lang[cross_lang['lang'] != cross_lang['winner_lang']]
                   .groupby(['winner_lang', 'lang']).size()
                   .reset_index(name='n').sort_values('n', ascending=False))
    print('\nTop translation flows (original_lang → translated):')
    print(flow_counts.head(10).to_string(index=False))
    return flow_counts
