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
		lltk.db.drop()
		results = lltk.db.rebuild(corpus_ids)
		print(f'\nIngested {sum(v for v in results.values() if isinstance(v, int))} texts from {len(results)} corpora')

	elif args.cmd == 'db-info':
		import pandas as pd
		pd.set_option('display.max_rows', 200)
		pd.set_option('display.width', 200)

		print(repr(lltk.db))
		print()

		# Genre × corpus crosstab
		try:
			df = lltk.db.query("""
				SELECT corpus, genre, COUNT(*) as n
				FROM texts
				GROUP BY corpus, genre
				ORDER BY corpus, genre
			""")
			pivot = df.pivot_table(index='corpus', columns='genre', values='n', fill_value=0, aggfunc='sum')
			# Add totals
			pivot['TOTAL'] = pivot.sum(axis=1)
			# Reorder columns: TOTAL first, then None, then alphabetical
			cols = ['TOTAL']
			if None in pivot.columns:
				cols.append(None)
			cols += sorted(c for c in pivot.columns if c not in cols)
			pivot = pivot[cols]
			# Add row totals
			pivot.loc['TOTAL'] = pivot.sum()
			print(pivot.to_string())
		except Exception as e:
			print(f'Error: {e}')
			print('Database may be empty. Run: lltk db-rebuild')


if __name__ == '__main__':
	main()
