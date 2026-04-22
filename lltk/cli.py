import os, sys, argparse


def main():
	import lltk
	from lltk.imports import DEFAULT_NUM_PROC

	parser = argparse.ArgumentParser(
		prog='lltk',
		description='Literary Language Toolkit (LLTK)',
	)
	subparsers = parser.add_subparsers(dest='cmd')

	# show
	p_show = subparsers.add_parser('show', help='List all corpora')

	# status
	p_status = subparsers.add_parser('status', help='Check install status of all corpora')

	# info
	p_info = subparsers.add_parser('info', help='Get info about a corpus')
	p_info.add_argument('corpus')

	# load
	p_load = subparsers.add_parser('load', help='Load corpus in interactive session')
	p_load.add_argument('corpus')

	# compile
	p_compile = subparsers.add_parser('compile', help='Compile corpus from sources')
	p_compile.add_argument('corpus')
	p_compile.add_argument('--tar-path', help='Path to tar file (for corpora that need it)')
	p_compile.add_argument('--repos', help='Comma-separated repo names (for earlyprint: eebotcp,eccotcp,evanstcp)')
	p_compile.add_argument('--force', action='store_true')

	# preprocess
	p_preprocess = subparsers.add_parser('preprocess', help='Preprocess corpus (xml→txt, txt→freqs)')
	p_preprocess.add_argument('corpus')
	p_preprocess.add_argument('--parts', default='txt,freqs', help='Comma-separated: txt,freqs,mfw,dtm (default: txt,freqs)')
	p_preprocess.add_argument('--num-proc', type=int, default=DEFAULT_NUM_PROC, help=f'Number of processes (default: {DEFAULT_NUM_PROC})')
	p_preprocess.add_argument('--force', action='store_true', help='Reprocess even if output exists')
	p_preprocess.add_argument('--lim', type=int, default=None, help='Limit number of texts to process')

	# install
	p_install = subparsers.add_parser('install', help='Download corpus data')
	p_install.add_argument('corpus')
	p_install.add_argument('--parts', default='metadata', help='Comma-separated: metadata,txt,xml,freqs,raw')

	# db rebuild
	p_db_rebuild = subparsers.add_parser('db-rebuild', help='Rebuild ClickHouse texts from corpus CSVs')
	p_db_rebuild.add_argument('corpora', nargs='*', help='Corpus IDs to rebuild (default: all)')
	p_db_rebuild.add_argument('--force', action='store_true', help='Drop and rebuild (default when no corpora specified)')

	# db info
	p_db_info = subparsers.add_parser('db-info', help='Show DuckDB metadata store info and genre breakdown')

	# db match
	p_db_match = subparsers.add_parser('db-match', help='Find matching texts within and across corpora')
	p_db_match.add_argument('corpora', nargs='*', help='Corpus IDs to include (default: all)')
	p_db_match.add_argument('--fuzzy', action='store_true', help='Also run fuzzy title matching (slow)')

	# db matches
	p_db_matches = subparsers.add_parser('db-matches', help='Search for matches by title')
	p_db_matches.add_argument('query', help='Title search string')

	# db-tag-genres
	p_db_tg = subparsers.add_parser('db-tag-genres',
		help='Materialize lltk.text_genre_tags from annotations genre_raw via facets.yml')
	p_db_tg.add_argument('--no-recognition', action='store_true',
		help='Disable recognition gating (include all LLM sources regardless of author/year match)')
	p_db_tg.add_argument('--min-sources', type=int, default=1,
		help='Minimum number of sources agreeing on a tag (default: 1)')

	# db enrich-genres
	p_db_enrich = subparsers.add_parser('db-enrich-genres', help='Propagate genre from bibliography corpora via match groups')

	# db-detect-translations
	p_db_trans = subparsers.add_parser('db-detect-translations', help='Detect translations via cross-language match groups')

	# db-detect-langs
	p_db_lang = subparsers.add_parser('db-detect-langs',
		help='Per-text language detection via stopword JOIN on lltk.text_words')
	p_db_lang.add_argument('--min-tokens', type=int, default=50,
		help='Skip texts with fewer total tokens (default: 50)')
	p_db_lang.add_argument('--coverage', type=float, default=0.05,
		help="Min fraction of tokens hitting top-lang's stopwords (default: 0.05)")
	p_db_lang.add_argument('--confidence', type=float, default=2.0,
		help='Min ratio of top-lang hits to runner-up (default: 2.0)')
	p_db_lang.add_argument('--rebuild', action='store_true',
		help='Reprocess every text (default: skip _ids already in lltk.text_langs)')

	# search
	p_search = subparsers.add_parser('search', help='Full-text search across passages')
	p_search.add_argument('query', help='FTS5 query (word, "phrase", NEAR(a b, 5))')
	p_search.add_argument('--genre', default=None)
	p_search.add_argument('--corpus', default=None)
	p_search.add_argument('--lang', default=None)
	p_search.add_argument('--year-min', type=int, default=None)
	p_search.add_argument('--year-max', type=int, default=None)
	p_search.add_argument('-n', '--limit', type=int, default=20)
	p_search.add_argument('--offset', type=int, default=0)

	# db-passages
	p_db_passages = subparsers.add_parser('db-passages', help='Build lltk.passages (CH) from corpus txt files')
	p_db_passages.add_argument('-j', '--jobs', type=int, default=None, help='Number of parallel workers')
	p_db_passages.add_argument('-n', '--passage-size', type=int, default=500, help='Target words per passage (default: 500)')
	p_db_passages.add_argument('--force', action='store_true', help='Rebuild from scratch')
	p_db_passages.add_argument('--year-min', type=int, default=None, help='Only texts with year >= N')
	p_db_passages.add_argument('--year-max', type=int, default=None, help='Only texts with year <= N')
	p_db_passages.add_argument('--dedup', action='store_true', help='Only chunk rank-0 match-group representatives')
	p_db_passages.add_argument('corpora', nargs='*', default=None, help='Corpus IDs or SyntheticCorpus IDs (default: all)')

	# db-embed-passages
	p_db_embed = subparsers.add_parser('db-embed-passages',
		help='Compute passage embeddings -> lltk.passage_embeddings (requires sentence-transformers)')
	p_db_embed.add_argument('--model', default='intfloat/multilingual-e5-large',
		help='Encoder model (default: intfloat/multilingual-e5-large)')
	p_db_embed.add_argument('--device', default='auto', choices=['auto', 'cpu', 'mps', 'cuda'],
		help='Compute device (default: auto)')
	p_db_embed.add_argument('--batch-size', type=int, default=200,
		help='Texts per CH insert batch (default: 200)')
	p_db_embed.add_argument('--encode-batch-size', type=int, default=64,
		help='Passages per encoder forward pass (default: 64)')
	p_db_embed.add_argument('--force', action='store_true', help='Rebuild from scratch')
	p_db_embed.add_argument('corpora', nargs='*', default=None,
		help='Corpus IDs (default: all texts with passages)')

	# db-match-embeddings
	p_db_me = subparsers.add_parser('db-match-embeddings',
		help='Find duplicate/translation candidates via embedding cosine similarity')
	p_db_me.add_argument('--threshold', type=float, default=0.998,
		help='Minimum cosine similarity for a match (default: 0.998)')
	p_db_me.add_argument('--same-corpus', action='store_true',
		help='Include same-corpus pairs (default: cross-corpus only)')
	p_db_me.add_argument('--model', default='multilingual-e5-large',
		help='Embedding model short name (default: multilingual-e5-large)')

	# db wordcounts
	p_db_wc = subparsers.add_parser('db-wordcounts', help='Compute word counts from freqs files')
	p_db_wc.add_argument('-j', '--jobs', type=int, default=None, help='Number of parallel workers')

	# db-wordindex
	p_db_wi = subparsers.add_parser('db-wordindex',
		help='Build word_year_corpus + year_corpus_totals aggregation tables from lltk.text_freqs')
	p_db_wi.add_argument('--min-count', type=int, default=1, help='Min word count to include (default: 1)')
	p_db_wi.add_argument('--vocab-size', type=int, default=50_000,
		help='Trim to top N most-frequent words (default: 50000)')
	p_db_wi.add_argument('corpora', nargs='*', help='Specific corpora (default: all)')

	# db-text-words
	p_db_tw = subparsers.add_parser('db-text-words',
		help='Build flat (word, _id, count) text_words table for native-columnar word queries')
	p_db_tw.add_argument('--force', action='store_true',
		help='Truncate and rebuild (default: skip already-built corpora)')
	p_db_tw.add_argument('corpora', nargs='*', help='Specific corpora (default: all)')

	# db-freqs
	p_db_fq = subparsers.add_parser('db-freqs', help='Ingest per-text freqs JSONs into per-corpus freqs.parquet files')
	p_db_fq.add_argument('-j', '--jobs', type=int, default=None, help='Number of parallel workers')
	p_db_fq.add_argument('--batch-size', type=int, default=2000, help='Texts per worker batch (default: 2000)')
	p_db_fq.add_argument('--force', action='store_true', help='Drop and re-ingest all')
	p_db_fq.add_argument('corpora', nargs='*', help='Specific corpora (default: all)')

	# db-wordagg
	p_db_wa = subparsers.add_parser('db-wordagg', help='Build word aggregate tables from existing word index')

	# db match-stats
	p_db_match_stats = subparsers.add_parser('db-match-stats', help='Show matching statistics')

	# prosodic-parse
	p_pros = subparsers.add_parser('prosodic-parse', help='Parse a corpus with prosodic (metrical scansion)')
	p_pros.add_argument('corpus', help='Corpus ID')
	p_pros.add_argument('-j', '--jobs', type=int, default=1, help='Parallel workers (default: 1)')
	p_pros.add_argument('--device', default='auto', choices=['auto', 'cpu', 'gpu'], help='Compute device (default: auto)')
	p_pros.add_argument('--no-resume', action='store_true', help='Re-parse all texts (default: skip texts already parsed)')
	p_pros.add_argument('--syntax', action='store_true', help='Run syntactic parsing too (slower)')
	p_pros.add_argument('--limit', type=int, default=None, help='Parse only first N texts (testing)')
	# Output-shape knobs (TextModel.save in prosodic >= 3.2)
	p_pros.add_argument('--save-parses', default='unbounded',
		choices=['best', 'unbounded', 'all'],
		help='best=1 parse/line | unbounded=Pareto front (default) | all=every parse incl. dominated')
	p_pros.add_argument('--no-compression', action='store_true',
		help='Write uncompressed parquet + plain text.txt (default: gzip)')
	# Common meter knobs
	p_pros.add_argument('--max-s', type=int, default=None, help='Max strong positions per foot (default 2)')
	p_pros.add_argument('--max-w', type=int, default=None, help='Max weak positions per foot (default 2)')
	p_pros.add_argument('--combine-by', default=None, choices=['line', 'sent'],
		help='Parse-and-combine granularity (default: line)')
	p_pros.add_argument('--constraints', default=None,
		help='JSON dict overriding constraint weights, e.g. \'{"w_peak":2.0}\'')
	# Escape hatches
	p_pros.add_argument('--meter-kwargs', default=None,
		help='JSON dict of additional kwargs forwarded to text.parse(...)')
	p_pros.add_argument('--text-kwargs', default=None,
		help='JSON dict of additional kwargs forwarded to prosodic.Text(...)')

	# prosodic-aggregate
	p_pros_agg = subparsers.add_parser('prosodic-aggregate', help='Build {corpus.path}/prosodic.parquet from per-text parsed.parquet files')
	p_pros_agg.add_argument('corpus', help='Corpus ID')

	# annotate
	p_annotate = subparsers.add_parser('annotate', help='Launch annotation web app for a corpus')
	p_annotate.add_argument('corpus', help='Corpus ID (e.g. arc_fiction)')
	p_annotate.add_argument('--port', type=int, default=8989, help='Port (default: 8989)')

	# app (explorer)
	p_app = subparsers.add_parser('app', help='Launch LLTK explorer web app')
	p_app.add_argument('--port', type=int, default=8899, help='Port (default: 8899)')

	if len(sys.argv) == 1:
		parser.print_help(sys.stderr)
		sys.exit(1)

	args = parser.parse_args()

	if args.cmd == 'show':
		from lltk.corpus.utils import show
		show()

	elif args.cmd == 'status':
		from lltk.corpus.utils import check_corpora
		check_corpora()

	elif args.cmd == 'info':
		corpus = lltk.load(args.corpus)
		corpus.info()

	elif args.cmd == 'load':
		from shutil import which
		pythonexec = 'ipython' if which('ipython') else 'python'
		cmds = ['import lltk', f"C = corpus = lltk.load('{args.corpus}')"]
		cmdstr = '; '.join(cmds)
		cmd = f'{pythonexec} -i -c "{cmdstr}"'
		print('\n' + '\n'.join(cmds) + '\n')
		os.system(cmd)

	elif args.cmd == 'compile':
		corpus = lltk.load(args.corpus)
		kwargs = {}
		if args.tar_path:
			kwargs['tar_path'] = args.tar_path
		if args.repos:
			kwargs['repos'] = [r.strip() for r in args.repos.split(',')]
		if args.force:
			kwargs['force'] = True
		corpus.compile(**kwargs)

	elif args.cmd == 'preprocess':
		corpus = lltk.load(args.corpus)
		parts = [p.strip() for p in args.parts.split(',')]
		corpus.preprocess(parts=parts, num_proc=args.num_proc, force=args.force, lim=args.lim)

	elif args.cmd == 'install':
		corpus = lltk.load(args.corpus)
		parts = [p.strip() for p in args.parts.split(',')]
		for part in parts:
			corpus.install(part=part)

	elif args.cmd == 'db-rebuild':
		from lltk.tools.db_adapter import get_adapter
		from lltk.tools.clickhouse_rebuild import rebuild_clickhouse
		import os as _os
		ch_url = _os.environ.get(
			'LLTK_CLICKHOUSE_URL',
			'clickhouse://lltk:lltk@localhost:8123/lltk',
		)
		ch = get_adapter(ch_url)
		corpus_ids = args.corpora if args.corpora else None
		# `--force` (already present on the parser) is the full-rebuild flag.
		force = args.force or corpus_ids is None
		total = rebuild_clickhouse(ch, corpora=corpus_ids, force=force)
		print(f'\nTotal: {total:,} texts ingested into ClickHouse')

	elif args.cmd == 'db-info':
		import pandas as pd
		pd.set_option('display.max_rows', 200)
		pd.set_option('display.width', 200)

		print(repr(lltk.db))
		print()

		try:
			from lltk.tools.metadb import GENRE_VOCAB

			# Genre × corpus crosstab (harmonized genres only + None + Other)
			df = lltk.db.query("""
				SELECT corpus, genre, COUNT(*) as n
				FROM texts
				GROUP BY corpus, genre
				ORDER BY corpus, genre
			""")

			# Map non-standard genres to 'Other'
			standard = GENRE_VOCAB | {None}
			df['genre_display'] = df['genre'].apply(
				lambda g: g if g in standard else 'Other'
			)
			df_grouped = df.groupby(['corpus', 'genre_display'])['n'].sum().reset_index()

			pivot = df_grouped.pivot_table(
				index='corpus', columns='genre_display', values='n',
				fill_value=0, aggfunc='sum'
			)

			# Add totals column
			pivot['TOTAL'] = pivot.sum(axis=1)

			# Reorder: TOTAL, None, then standard genres alphabetically, then Other
			cols = ['TOTAL']
			if None in pivot.columns:
				cols.append(None)
			cols += sorted(c for c in pivot.columns if c in GENRE_VOCAB)
			if 'Other' in pivot.columns:
				cols.append('Other')
			pivot = pivot[[c for c in cols if c in pivot.columns]]

			# Rename None column for display
			pivot = pivot.rename(columns={None: '(none)'})

			# Add row totals
			pivot.loc['TOTAL'] = pivot.sum()
			pivot = pivot.astype(int)

			print(pivot.to_string())

			# Show non-standard genre values if any
			non_standard = df[df['genre_display'] == 'Other']
			if len(non_standard):
				print(f'\nNon-standard genre values (mapped to "Other" above):')
				for _, row in non_standard.sort_values('n', ascending=False).head(20).iterrows():
					print(f'  {row["corpus"]:25s} {row["genre"]:40s} {row["n"]:>6d}')

		except Exception as e:
			print(f'Error: {e}')
			print('Database may be empty. Run: lltk db-rebuild')

	elif args.cmd == 'db-match':
		corpora = args.corpora if args.corpora else None
		lltk.db.match(corpora=corpora, fuzzy=args.fuzzy)

	elif args.cmd == 'db-matches':
		import pandas as pd
		pd.set_option('display.max_rows', 200)
		pd.set_option('display.width', 200)
		pd.set_option('display.max_colwidth', 80)
		df = lltk.db.find_matches(args.query)
		if len(df):
			print(df.to_string(index=False))
		else:
			print(f'No matches found for "{args.query}"')

	elif args.cmd == 'db-wordcounts':
		lltk.db.wordcounts(num_proc=args.jobs)

	elif args.cmd == 'db-tag-genres':
		from lltk.tools.genre_tags import build_genre_tags
		from lltk.tools.db_adapter import get_adapter
		ch = get_adapter('clickhouse://lltk:lltk@localhost:8123/lltk')
		build_genre_tags(ch, recognized_only=not args.no_recognition,
		                 min_sources=args.min_sources)

	elif args.cmd == 'db-enrich-genres':
		stats = lltk.db.enrich_genres()
		if stats is not None and len(stats):
			print('\nGenre enrichment source distribution:')
			print(stats.to_string(index=False))

	elif args.cmd == 'db-detect-translations':
		lltk.db.detect_translations()

	elif args.cmd == 'db-detect-langs':
		lltk.db.detect_langs(
			min_tokens=args.min_tokens,
			coverage_threshold=args.coverage,
			confidence_threshold=args.confidence,
			skip_existing=not args.rebuild,
		)

	elif args.cmd == 'search':
		results = lltk.db.search(
			args.query,
			genre=args.genre, corpus=args.corpus, lang=args.lang,
			year_min=args.year_min, year_max=args.year_max,
			limit=args.limit, offset=args.offset,
		)
		if not results:
			print('No results.')
		else:
			n_total = lltk.db.search_count(args.query)
			print(f'{n_total} passages match "{args.query}" (showing {len(results)})\n')
			for r in results:
				print(f'[{r["corpus"]}] [{r.get("year","")}] {r["title"][:60]}  | {r["author"][:30]}')
				print(f'  {r["snippet"]}')
				print()

	elif args.cmd == 'db-passages':
		lltk.db.build_passages_db(
			n=args.passage_size,
			num_proc=args.jobs,
			corpora=args.corpora or None,
			force=args.force,
			year_min=args.year_min,
			year_max=args.year_max,
			dedup=args.dedup,
		)

	elif args.cmd == 'db-embed-passages':
		lltk.db.build_embeddings_db(
			model=args.model,
			device=args.device,
			batch_size=args.batch_size,
			encode_batch_size=args.encode_batch_size,
			corpora=args.corpora or None,
			force=args.force,
		)

	elif args.cmd == 'db-match-embeddings':
		lltk.db.match_embeddings(
			model=args.model,
			threshold=args.threshold,
			same_corpus=args.same_corpus,
		)

	elif args.cmd == 'db-wordindex':
		lltk.db.build_word_index_sql(
			vocab_size=args.vocab_size,
			min_count=args.min_count,
			corpora=args.corpora or None,
		)

	elif args.cmd == 'db-text-words':
		lltk.db.build_text_words(
			corpora=args.corpora or None,
			force=args.force,
		)

	elif args.cmd == 'db-wordagg':
		lltk.db.build_word_aggregates()

	elif args.cmd == 'db-freqs':
		lltk.db.build_freqs_db(
			num_proc=args.jobs,
			batch_size=args.batch_size,
			corpora=args.corpora or None,
			truncate_first=args.force,
		)

	elif args.cmd == 'db-match-stats':
		stats = lltk.db.match_stats()
		print(f"Total matches: {stats['total_matches']}")
		print(f"Total match groups: {stats['total_groups']}")
		print(f"\nBy match type:")
		print(stats['by_type'].to_string(index=False))
		print(f"\nGroup size distribution:")
		print(stats['group_sizes'].to_string(index=False))


	elif args.cmd == 'prosodic-parse':
		from lltk.tools.prosodic_tools import parse_corpus
		import json as _json
		def _parse_json(arg, name):
			if arg is None:
				return None
			try:
				return _json.loads(arg)
			except _json.JSONDecodeError as e:
				raise SystemExit(f'--{name}: invalid JSON ({e})')
		parse_corpus(
			args.corpus,
			n_workers=args.jobs,
			device=args.device,
			resume=not args.no_resume,
			syntax=args.syntax,
			limit=args.limit,
			save_parses=args.save_parses,
			compression=None if args.no_compression else 'gzip',
			max_s=args.max_s,
			max_w=args.max_w,
			combine_by=args.combine_by,
			constraints=_parse_json(args.constraints, 'constraints'),
			meter_kwargs=_parse_json(args.meter_kwargs, 'meter-kwargs'),
			text_kwargs=_parse_json(args.text_kwargs, 'text-kwargs'),
		)

	elif args.cmd == 'prosodic-aggregate':
		from lltk.tools.prosodic_tools import aggregate_corpus
		aggregate_corpus(args.corpus)

	elif args.cmd == 'annotate':
		from lltk.web.annotate import run_annotate
		run_annotate(args.corpus, port=args.port)

	elif args.cmd == 'app':
		from lltk.web.app import run_app
		run_app(port=args.port)


if __name__ == '__main__':
	main()
