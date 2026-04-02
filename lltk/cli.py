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
	p_db_rebuild = subparsers.add_parser('db-rebuild', help='Rebuild the DuckDB metadata store')
	p_db_rebuild.add_argument('corpora', nargs='*', help='Corpus IDs to rebuild (default: all)')

	# db info
	p_db_info = subparsers.add_parser('db-info', help='Show DuckDB metadata store info and genre breakdown')

	# db match
	p_db_match = subparsers.add_parser('db-match', help='Run cross-corpus title matching')
	p_db_match.add_argument('corpora', nargs='*', help='Corpus IDs to match (default: all)')
	p_db_match.add_argument('--tiers', default='1,2', help='Matching tiers (default: 1,2)')

	# db matches
	p_db_matches = subparsers.add_parser('db-matches', help='Search for matches by title')
	p_db_matches.add_argument('query', help='Title search string')

	# db match-stats
	p_db_match_stats = subparsers.add_parser('db-match-stats', help='Show matching statistics')

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
		corpus_ids = args.corpora if args.corpora else None
		if not corpus_ids:
			lltk.db.drop()  # full rebuild: drop everything
		results = lltk.db.rebuild(corpus_ids)
		print(f'\nIngested {sum(v for v in results.values() if isinstance(v, int))} texts from {len(results)} corpora')

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
		tiers = tuple(int(t.strip()) for t in args.tiers.split(','))
		corpora = args.corpora if args.corpora else None
		lltk.db.match(corpora=corpora, tiers=tiers)

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

	elif args.cmd == 'db-match-stats':
		stats = lltk.db.match_stats()
		print(f"Total matches: {stats['total_matches']}")
		print(f"Total match groups: {stats['total_groups']}")
		print(f"\nBy match type:")
		print(stats['by_type'].to_string(index=False))
		print(f"\nGroup size distribution:")
		print(stats['group_sizes'].to_string(index=False))


if __name__ == '__main__':
	main()
