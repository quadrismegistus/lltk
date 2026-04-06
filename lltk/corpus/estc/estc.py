from lltk.imports import *

# Re-export genre functions for backward compat (used by evans_tcp, tests)
from lltk.corpus.estc.parse_estc_genre import (
	classify_genres, _genres_to_harmonized, is_fiction, FICTION_GENRES,
)


class TextESTC(BaseText):
	META_SEP=' | '

	@property
	def json(self):
		with open(self.fnfn) as f:
			return json.loads(f.read())

	@property
	def estcid(self):
		return self.id.split('/')[-1]

	@property
	def id_estc(self):
		return self.estcid


## CORPUS ##

from lltk.corpus.corpus import BaseCorpus


class ESTC(BaseCorpus):
	TEXT_CLASS=TextESTC
	PATH_TXT = 'estc/_json_estc'
	EXT_TXT='.json.txt'
	PATH_METADATA = 'estc/metadata.csv'
	LINKS = {
		'ecco': ('id_estc', 'ESTCID'),
		'eebo_tcp': ('id_estc', 'id_stc'),
	}

	def load_metadata(self, *args, **kwargs):
		return super().load_metadata(*args, **kwargs)

	def compile(self):
		"""Parse ESTC MARC JSON bib + holdings files into metadata.csv.

		Reads ~481K bibliographic records from _json_estc/ and holdings
		records from _json_estc_holdings/. Produces a wide, pre-enriched
		metadata.csv with genre classification, translation detection,
		book format/extent parsing, Wing/STC reference extraction, and
		holdings counts.
		"""
		from lltk.corpus.estc.estc_json_parser import parse_bib_record, parse_holdings_record
		from lltk.corpus.estc.parse_estc_genre import (
			classify_genres, detect_translation, is_fiction, _genres_to_harmonized,
		)
		from lltk.corpus.estc.book_history import standardize_format, parse_extent
		from concurrent.futures import ThreadPoolExecutor

		bib_dir = os.path.join(self.path, '_json_estc')
		hol_dir = os.path.join(self.path, '_json_estc_holdings')

		# ── Step 1: parse holdings per ESTC ID ────────────────────────
		logger.info('Parsing holdings...')
		holdings_data = {}  # estc_id → list of {institution_code, shelfmark, ...}

		def _parse_one_hol(path):
			rec = parse_holdings_record(path)
			eid = rec.get('estc_id')
			if eid and rec['holdings']:
				entries = []
				for h in rec['holdings']:
					entry = {}
					if h.get('institution_code'):
						entry['code'] = h['institution_code']
					if h.get('institution_name'):
						entry['name'] = h['institution_name']
					if h.get('shelfmark'):
						entry['shelfmark'] = h['shelfmark']
					if entry:
						entries.append(entry)
				return (eid, entries)
			return None

		hol_paths = []
		for shard in sorted(os.listdir(hol_dir)):
			shard_path = os.path.join(hol_dir, shard)
			if not os.path.isdir(shard_path):
				continue
			for fname in os.listdir(shard_path):
				hol_paths.append(os.path.join(shard_path, fname))

		with ThreadPoolExecutor(max_workers=8) as pool:
			for result in pool.map(_parse_one_hol, hol_paths):
				if result:
					holdings_data[result[0]] = result[1]
		logger.info(f'  {len(holdings_data)} holdings records')

		# ── Step 2: collect bib file paths ────────────────────────────
		bib_paths = []
		for shard in sorted(os.listdir(bib_dir)):
			shard_path = os.path.join(bib_dir, shard)
			if not os.path.isdir(shard_path):
				continue
			for fname in os.listdir(shard_path):
				bib_paths.append(os.path.join(shard_path, fname))
		logger.info(f'Parsing {len(bib_paths)} bib records...')

		# ── Step 3: parse each record ─────────────────────────────────
		ansi_re = re.compile(r'\x1b.')

		def _process_bib(path):
			rec = parse_bib_record(path)
			eid = rec.get('estc_id')
			if not eid:
				return None

			# Fixed field (008)
			ff = rec['fixed_field'] or ''
			year = _parse_year(ff[7:11]) if len(ff) > 10 else None
			year_end = _parse_year(ff[11:15]) if len(ff) > 14 else None
			year_type = ff[6] if len(ff) > 6 else None
			lang = ff[35:38].strip() if len(ff) > 37 else None
			country = ff[15:18].strip() if len(ff) > 17 else None

			# Author: personal > corporate > meeting
			ap = rec['author_personal']
			ac = rec['author_corporate']
			am = rec['author_meeting']
			author = (ap['name'] if ap else
			          ac['name'] if ac else
			          am['name'] if am else None)
			author_dates = ap['dates'] if ap else None

			# Publication (260)
			pub = rec['publication']

			# Place of publication (752)
			places = rec['places_of_pub']
			pub_nation = places[0]['country'] if places else None
			pub_region = places[0]['state'] if places else None
			pub_city = places[0]['city'] if places else None

			# Physical description (300)
			phys = rec['physical_desc']
			extent_raw = phys['extent'] or ''
			raw_dims = phys['dimensions'] or ''
			# ANSI escape → ⁰ encoding (matches legacy behavior)
			dims = ansi_re.sub('', raw_dims).replace('0', '⁰') if raw_dims else ''

			# Genre/form terms (655$a)
			form_terms = [g['term'].rstrip('.') for g in rec['genres'] if g.get('term')]
			# Subject terms (650$a)
			subject_terms = [s['term'] for s in rec['subjects_topical'] if s.get('term')]
			# Subject places (651$a)
			subject_place_terms = [s['place'] for s in rec['subjects_geographic'] if s.get('place')]
			# Subject persons (600$a)
			subject_person_terms = [s['name'] for s in rec['subjects_personal'] if s.get('name')]
			# Added persons (700$a)
			added_person_strs = [p['name'] for p in rec['added_persons'] if p.get('name')]

			# ── Genre classification ──────────────────────────────────
			genre_result = classify_genres(
				form_terms=form_terms,
				subject_terms=subject_terms,
				title=rec['title'],
				title_sub=rec['subtitle'],
			)
			genres = genre_result['genres']
			genre_source = genre_result['source']
			genre_raw = ' | '.join(sorted(genres)) if genres else None
			genre = _genres_to_harmonized(genres) if genres else None

			# ── Translation detection ─────────────────────────────────
			is_trans = detect_translation(rec)

			# ── Book format & extent ──────────────────────────────────
			fmt = standardize_format(dims)
			ext = parse_extent(extent_raw)

			# ── References (510): extract STC/Wing IDs ────────────────
			id_stc = None
			id_wing = None
			ref_parts = []
			for ref in rec['references']:
				src = (ref.get('source') or '').strip().rstrip(',')
				ident = (ref.get('identifier') or '').strip()
				if not src or not ident:
					continue
				ref_parts.append(f'{src} {ident}')
				if 'STC' in src and not id_stc:
					id_stc = ident
				elif 'Wing' in src and not id_wing:
					id_wing = ident

			# ── URLs (856) ────────────────────────────────────────────
			url_list = [u['url'] for u in rec['urls'] if u.get('url')]

			# ── Assemble row ──────────────────────────────────────────
			return {
				'id': eid,
				'id_estc': eid,
				'title': rec['title'],
				'title_sub': rec['subtitle'],
				'author': author,
				'author_dates': author_dates,
				'year': year,
				'year_end': year_end,
				'year_type': year_type,
				'lang': lang,
				'country': country,
				'pub_place': pub['place'],
				'publisher': pub['publisher'],
				'pub_date': pub['date'],
				'pub_nation': pub_nation,
				'pub_region': pub_region,
				'pub_city': pub_city,
				'extent': extent_raw,
				'dimensions': dims,
				'illustrations': phys['illustrations'],
				'format_std': fmt['format_std'],
				'format_modifier': fmt['format_modifier'],
				'num_pages': ext['num_pages'],
				'num_volumes': ext['num_volumes'],
				'has_plates': ext['has_plates'],
				'extent_type': ext['extent_type'],
				'form': ' | '.join(form_terms) if form_terms else None,
				'subject_topic': ' | '.join(subject_terms) if subject_terms else None,
				'subject_place': ' | '.join(subject_place_terms) if subject_place_terms else None,
				'subject_person': ' | '.join(subject_person_terms) if subject_person_terms else None,
				'added_persons': ' | '.join(added_person_strs) if added_person_strs else None,
				'notes': ' | '.join(rec['notes']) if rec['notes'] else None,
				'genre': genre,
				'genre_raw': genre_raw,
				'genre_source': genre_source,
				'is_translated': is_trans,
				'is_fiction': is_fiction(genres),
				'n_holdings': len(holdings_data.get(eid, [])),
				'holdings': json.dumps(holdings_data[eid]) if eid in holdings_data else None,
				'id_stc': id_stc,
				'id_wing': id_wing,
				'references': ' | '.join(ref_parts) if ref_parts else None,
				'urls': ' | '.join(url_list) if url_list else None,
			}

		rows = []
		with ThreadPoolExecutor(max_workers=8) as pool:
			for i, row in enumerate(pool.map(_process_bib, bib_paths)):
				if row:
					rows.append(row)
				if (i + 1) % 100000 == 0:
					logger.info(f'  {i+1}/{len(bib_paths)}...')

		# ── Step 4: write metadata.csv ────────────────────────────────
		logger.info(f'Writing {len(rows)} records to metadata.csv...')
		df = pd.DataFrame(rows)
		out_path = os.path.join(self.path, 'metadata.csv')
		df.to_csv(out_path, index=False)

		# Clear stale parquet caches
		for fname in ('metadata.parquet', 'metadata_enriched.parquet'):
			p = os.path.join(self.path, fname)
			if os.path.exists(p):
				os.remove(p)

		logger.info(f'Done. {len(df)} records written.')
		return df


def _parse_year(s):
	"""Parse a 4-char year from MARC 008. Returns int or None."""
	if not s:
		return None
	s = s.strip()
	if not s or s in ('||||', '    ', '^^^^', 'uuuu'):
		return None
	try:
		y = int(s)
		if 1000 <= y <= 2100:
			return y
	except (ValueError, TypeError):
		pass
	return None
