from __future__ import annotations

import gzip
import os
import re
from typing import Union

from logmap import logmap
import pandas as pd
from lxml import etree

from lltk.imports import BaseCorpus, BaseText, clean_text, get_tqdm, log, tools


# ── lxml-based ECCO XML parser ────────────────────────────────────────

def parse_ecco_xml(source: Union[str, etree._Element]) -> etree._Element:
	"""Accept an xml.gz path, xml path, or lxml Element. Return the root Element."""
	if isinstance(source, etree._Element):
		return source
	path = str(source)
	if path.endswith('.gz'):
		with gzip.open(path, 'rb') as f:
			return etree.fromstring(f.read())
	else:
		return etree.parse(path).getroot()


def ecco_lines(
	source: Union[str, etree._Element],
	page_types: set[str] | None = None,
) -> list[dict]:
	"""Parse ECCO XML into a flat list of line records.

	Each record: page_num, page_type, page_ocr, para_num, line_num, line_txt.

	Line breaks detected via y-coordinate jumps (>20px) in the ``pos``
	attribute on ``<wd>`` tags.  Paragraph breaks from ``<p>`` parent changes.
	"""
	if page_types is not None:
		page_types = set(page_types)
	root = parse_ecco_xml(source)
	body = root.find('.//text')
	if body is None:
		return []

	lines = []
	page_i = 0
	for page_el in body.iterfind('.//page'):
		page_type = page_el.get('type', '')
		if page_types is not None and page_type not in page_types:
			page_i += 1
			continue

		sp_el = page_el.find('pageInfo/sourcePage')
		ocr_el = page_el.find('pageInfo/ocr')
		page_num = int(sp_el.text) if sp_el is not None and sp_el.text.isdigit() else None
		page_ocr = float(ocr_el.text) if ocr_el is not None else None

		para_num = 0
		line_num = 0
		line_words = []
		last_parent = None
		last_y = None

		def _flush_line():
			nonlocal line_words, line_num
			if line_words:
				lines.append({
					'page_i': page_i,
					'page_num': page_num,
					'page_type': page_type,
					'page_ocr': page_ocr,
					'para_num': para_num,
					'line_num': line_num,
					'line_txt': ' '.join(line_words),
				})
				line_words = []
				line_num += 1

		for wd in page_el.iter('wd'):
			parent = wd.getparent()
			if parent is not last_parent:
				_flush_line()
				if last_parent is not None:
					para_num += 1
					line_num = 0
			last_parent = parent

			pos = wd.get('pos', '')
			if pos:
				try:
					y = int(pos.split(',')[1])
					if last_y is not None and y - last_y > 20:
						_flush_line()
					last_y = y
				except (IndexError, ValueError):
					pass

			line_words.append(wd.text or '')

		_flush_line()
		page_i += 1

	return lines


def ecco_df(source: Union[str, etree._Element], **kwargs) -> pd.DataFrame:
	"""Parse ECCO XML into a pandas DataFrame of line records."""
	return pd.DataFrame(ecco_lines(source, **kwargs))


def ecco_page_texts(source: Union[str, etree._Element], **kwargs) -> list[dict]:
	"""Extract per-page text strings with metadata from ECCO XML.

	Returns list of dicts: page_i, page_num, page_type, page_ocr, text.
	Lines within paragraphs joined with spaces, paragraphs with double newlines.
	"""
	lines = ecco_lines(source, **kwargs)
	if not lines:
		return []

	pages = []
	current_page_i = lines[0]['page_i']
	current_meta = {
		'page_i': lines[0]['page_i'],
		'page_num': lines[0]['page_num'],
		'page_type': lines[0]['page_type'],
		'page_ocr': lines[0]['page_ocr'],
	}
	current_paras = []
	current_para_num = lines[0]['para_num']
	current_lines = []

	def _flush_page():
		if current_lines:
			current_paras.append(' '.join(current_lines))
		if current_paras:
			pages.append({**current_meta, 'text': '\n\n'.join(current_paras)})

	for rec in lines:
		if rec['page_i'] != current_page_i:
			_flush_page()
			current_paras = []
			current_lines = []
			current_page_i = rec['page_i']
			current_para_num = rec['para_num']
			current_meta = {
				'page_i': rec['page_i'],
				'page_num': rec['page_num'],
				'page_type': rec['page_type'],
				'page_ocr': rec['page_ocr'],
			}
		elif rec['para_num'] != current_para_num:
			if current_lines:
				current_paras.append(' '.join(current_lines))
				current_lines = []
			current_para_num = rec['para_num']

		current_lines.append(rec['line_txt'])

	_flush_page()
	return pages


def ecco_xml2txt(
	source: Union[str, etree._Element],
	page_types: set[str] | None = None,
	remove_catchwords: bool = True,
) -> str:
	"""Convert ECCO XML to plain text.

	Accepts an xml.gz path, xml path, or lxml Element.
	``\\n\\n\\n`` between pages, ``\\n\\n`` between paragraphs, ``\\n`` between lines.
	"""
	lines = ecco_lines(source, page_types=page_types)
	if not lines:
		return ''

	if remove_catchwords:
		lines = _remove_catchwords(lines)

	parts = []
	prev_page_i = None
	prev_para = None
	for rec in lines:
		if prev_page_i is not None and rec['page_i'] != prev_page_i:
			parts.append('\n\n\n')
		elif prev_para is not None and rec['para_num'] != prev_para:
			parts.append('\n\n')
		elif parts:
			parts.append('\n')
		parts.append(rec['line_txt'])
		prev_page_i = rec['page_i']
		prev_para = rec['para_num']

	plain = ''.join(parts)
	plain = _fix_dangling_hyphens(plain)
	plain = re.sub(r'(?m)^\s*\(\d+\)\s*$', '', plain)
	return plain


def _remove_catchwords(lines: list[dict]) -> list[dict]:
	"""Remove catchwords: last word of page N == first word of page N+1."""
	if not lines:
		return lines

	page_groups = []
	current_page_i = lines[0]['page_i']
	current_group = []
	for rec in lines:
		if rec['page_i'] != current_page_i:
			page_groups.append(current_group)
			current_group = []
			current_page_i = rec['page_i']
		current_group.append(rec)
	page_groups.append(current_group)

	result = []
	for i, group in enumerate(page_groups):
		if i + 1 < len(page_groups) and group and page_groups[i + 1]:
			last_line = group[-1]['line_txt']
			first_line = page_groups[i + 1][0]['line_txt']
			last_word = last_line.split()[-1] if last_line.split() else ''
			first_word = first_line.split()[0] if first_line.split() else ''
			if last_word and last_word == first_word:
				remaining = ' '.join(last_line.split()[:-1])
				if remaining:
					group[-1] = {**group[-1], 'line_txt': remaining}
				else:
					group = group[:-1]
		result.extend(group)
	return result


def _remove_catchwords_from_text(text):
	"""Remove catchwords from cleaned text (split on triple-newline pages)."""
	pages = text.split('\n\n\n')
	for i in range(len(pages) - 1):
		words_this = pages[i].split()
		words_next = pages[i + 1].split()
		if words_this and words_next and words_this[-1] == words_next[0]:
			pages[i] = ' '.join(words_this[:-1])
	return '\n\n\n'.join(pages)


def _fix_dangling_hyphens(text, hyphens=frozenset({'¬', '-'})):
	"""Rejoin words broken across lines with a hyphen or negation obelus."""
	lines = [l.rstrip() for l in text.split('\n')]
	for i, line in enumerate(lines):
		if i + 1 >= len(lines) or not lines[i + 1]:
			continue
		for hyph in hyphens:
			if line.endswith(hyph):
				next_words = lines[i + 1].split()
				if next_words:
					lines[i] = line[:-1] + next_words[0]
					lines[i + 1] = ' '.join(next_words[1:])
				break
	return '\n'.join(lines)


class TextECCO(BaseText):
	XML2TXT = ecco_xml2txt

	@property
	def meta_by_file(self):
		if not hasattr(self,'_meta'):
			import gzip
			mtxt=''
			f = gzip.open(self.fnfn,'rb')
			for line in f:
				line=line.decode('iso-8859-1').encode('utf8')
				mtxt+=line
				if '</citation>' in line:
					break

			md=self.extract_metadata(mtxt)
			md['id']=self.id
			self._meta=md
		return self._meta

	def extract_metadata(self,mtxt,word_stats=True):
		_ = word_stats  # unused; word stats always computed below
		from lltk.text.utils import load_english
		ENGLISH=load_english()

		md={}
		## IDs
		import bs4
		dom=bs4.BeautifulSoup(mtxt,'lxml')
		md={}

		simples = ['documentID','ESTCID','pubDate','releaseDate','sourceLibrary','language','model','documentType','marcName','birthDate','deathDate','marcDate','fullTitle','currentVolume','totalVolumes','imprintFull','imprintCity','imprintPublisher','imprintYear','collation','publicationPlace','totalPages']

		for x in simples:
			try:
				md[x]=dom.find(x.lower()).text
			except AttributeError:
				md[x]=''


		## Num Holdings
		md['holdings_num_libraries']=mtxt.count('</holdings>')
		#md['libraries'] = [tag.text for tag in dom('libraryname')]
		md['notes'] = ' | '.join([tag.text for tag in dom('notes')])

		for subjhead in dom('locsubjecthead'):
			subtype=subjhead.get('type','')
			for subj in subjhead('locsubject'):
				subfield=subj.get('subfield','')
				val=subj.text
				md[subtype+'_'+subfield]=val

		try:
			md['year']=int(''.join([x for x in md['pubDate'] if x.isdigit()][:4]))
		except (ValueError,TypeError) as e:
			md['year']=0

		## if word_stats

		words=[w for w,p in self.tokens]
		md['num_words']=len(words)
		md['ocr_accuracy']=len([w for w in words if w in ENGLISH]) / float(len(words)) if len(words) else 0.0

		return md

	@property
	def text(self):
		import gzip
		try:
			with gzip.open(self.fnfn, 'rb') as f:
				file_content = f.read()
		except IOError:
			with logmap('Reading ECCO text') as _log:
				_log.debug(f'Error on gzip for file id {self.id}')
			return ''
		return file_content

	@property
	def fnfn_txt(self):
		return os.path.join(self.corpus.path_txt,self.id+'.txt.gz')

	@property
	def fnfn(self):
		return os.path.join(self.corpus.path_xml,self.id+'.xml.gz')

	@property
	def has_plain_text_file(self):
		return os.path.exists(self.fnfn_txt)

	@property
	def text_plain_from_file(self):
		if not self.has_plain_text_file:
			return False

		import gzip
		try:
			with gzip.open(self.fnfn_txt,'rb') as f:
				txt=f.read().decode('utf-8')
		except:
			with logmap('Reading ECCO text') as _log:
				_log.debug(f'ERROR: could not decompress: {self.id}')
			return ''
		return txt


	def text_plain(self, **kw):
		clean_dir = getattr(self.corpus, 'path_txt_clean', None)
		if clean_dir:
			clean_path = os.path.join(clean_dir, self.id + '.txt')
			if os.path.exists(clean_path):
				with open(clean_path, encoding='utf-8') as f:
					return f.read()
		cache = self.text_plain_from_file
		if cache:
			return cache
		if self.path_txt and os.path.exists(self.path_txt):
			from lltk.text.text import _open_file
			with _open_file(self.path_txt) as f:
				return f.read()
		if os.path.exists(self.fnfn):
			return ecco_xml2txt(self.fnfn)
		return ''

	def clean_txt(self, task=None, model=None, force=False, num_workers=1):
		"""Clean OCR via LLM using ECCO XML page structure.

		Returns cleaned text string, or None if nothing to clean.
		"""
		import json as _json

		clean_dir = getattr(self.corpus, 'path_txt_clean', None)
		if not clean_dir:
			return None
		clean_path = os.path.join(clean_dir, self.id + '.txt')
		meta_path = os.path.join(clean_dir, self.id + '.json')
		if not force and os.path.exists(clean_path):
			with open(clean_path, encoding='utf-8') as f:
				self._txt = f.read()
			return self._txt

		if task is None:
			from largeliterarymodels.tasks import OCRCleanTask
			task = OCRCleanTask(**({'model': model} if model else {}))

		if not os.path.exists(self.fnfn):
			return None
		pages = ecco_page_texts(self.fnfn)
		if not pages:
			return None

		cleaned_pages = task.map([p['text'] for p in pages], num_workers=num_workers)

		cleaned_text = '\n\n\n'.join(cleaned_pages)
		cleaned_text = _remove_catchwords_from_text(cleaned_text)
		cleaned_text = _fix_dangling_hyphens(cleaned_text)

		os.makedirs(os.path.dirname(clean_path), exist_ok=True)
		with open(clean_path, 'w', encoding='utf-8') as f:
			f.write(cleaned_text)

		meta = [{'page_i': p['page_i'], 'page_num': p['page_num'],
				 'page_type': p['page_type'], 'page_ocr': p['page_ocr']}
				for p in pages]
		with open(meta_path, 'w', encoding='utf-8') as f:
			_json.dump(meta, f, indent=2)
		self._txt = cleaned_text
		return cleaned_text


class ECCO(BaseCorpus):
	TEXT_CLASS=TextECCO
	LINKS = {'estc': ('id_estc', 'id_estc')}

	@property
	def path_metadata_enriched(self):
		return os.path.join(self.path, 'metadata_enriched.parquet')

	def load_metadata(self, force=False, **kwargs):
		if not force and self._metadf is not None:
			return self._metadf

		# Fast path: enriched parquet cache
		enriched_path = self.path_metadata_enriched
		if not force and os.path.exists(enriched_path) and os.path.exists(self.path_metadata):
			if os.path.getmtime(enriched_path) >= os.path.getmtime(self.path_metadata):
				try:
					meta = pd.read_parquet(enriched_path)
					if self.col_id in meta.columns:
						meta = meta.set_index(self.col_id)
					self._metadf = meta
					return meta
				except Exception:
					pass

		meta = super().load_metadata(force=force)
		if not len(meta):
			return meta
		# Normalize ESTC IDs: zero-pad to Letter+6 digits
		if 'ESTCID' in meta.columns:
			from lltk.corpus.eebo_tcp.eebo_tcp import _normalize_estc_id
			meta['id_estc_orig'] = meta['ESTCID']
			meta['id_estc'] = meta['ESTCID'].apply(_normalize_estc_id)
		meta = self.merge_linked_metadata(meta)
		# Inherit genre from linked ESTC
		if 'estc_genre' in meta.columns:
			meta['genre'] = meta['estc_genre']
		if 'estc_genre_raw' in meta.columns:
			meta['genre_raw'] = meta['estc_genre_raw']
		meta['title'] = meta.get('estc_title', pd.Series(dtype=object)).fillna(meta.get('fullTitle', ''))
		meta['author'] = meta.get('estc_author', pd.Series(dtype=object)).fillna(meta.get('marcName', ''))

		if 'estc_is_translated' in meta.columns:
			meta['is_translated'] = meta['estc_is_translated']

		try:
			meta.to_parquet(enriched_path)
		except Exception:
			pass

		self._metadf = meta
		return meta

	def compile(self, tar_path=None, **kwargs):
		"""Extract xml/*.xml from ECCO tar, gzip, and save to path_xml.

		Args:
			tar_path: Full path to ecco.tar file.
		"""
		import tarfile, gzip

		if not tar_path:
			raise ValueError('tar_path is required (full path to ecco.tar)')
		if not os.path.exists(tar_path):
			raise FileNotFoundError(f'Tar file not found: {tar_path}')

		xml_dir = self.path_xml
		os.makedirs(xml_dir, exist_ok=True)

		count = 0
		skipped = 0
		with tarfile.open(tar_path, 'r') as tar:
			for member in get_tqdm(tar, total=219295, desc='Extracting ECCO XMLs'):
				if not member.isfile():
					continue
				# match paths like .../ecco/<subcorpus>/<text_id>/xml/<text_id>.xml
				parts = member.name.split('/')
				try:
					ecco_idx = parts.index('ecco')
				except ValueError:
					continue
				tail = parts[ecco_idx + 1:]  # e.g. ['LitAndLang1', '0072502100', 'xml', '0072502100.xml']
				if len(tail) != 4 or tail[2] != 'xml' or not tail[3].endswith('.xml'):
					continue

				subcorpus = tail[0]
				text_id = tail[1]
				out_dir = os.path.join(xml_dir, subcorpus)
				out_path = os.path.join(out_dir, f'{text_id}.xml.gz')

				if os.path.exists(out_path):
					skipped += 1
					continue

				os.makedirs(out_dir, exist_ok=True)
				f = tar.extractfile(member)
				if f is None:
					continue
				with gzip.open(out_path, 'wb') as gz:
					gz.write(f.read())
				count += 1

		log(f'Done. Extracted {count} XML files, skipped {skipped} (already existed).')

	def match_estc(self):
		from lltk.corpus.estc import ESTC
		estc=ESTC()

		self.match_records(estc, id_field_1='ESTCID', id_field_2='id', match_by_id=True, match_by_title=False)

	def match_ravengarside(self):
		from lltk.corpus.ravengarside import RavenGarside
		rg=RavenGarside()
		self.match_records(rg, match_by_title=True)

	def match_eccotcp(self):
		from lltk.corpus.ecco import ECCO_TCP
		etcp = ECCO_TCP()
		self.match_records(etcp, match_by_title=True, match_by_id=True, id_field_1='id_ESTC', id_field_2='id_ESTC')







# Legacy alias — gale_amfic imports this name with a bs4 dom.
# Convert bs4 dom to lxml on the fly.
def gale_xml2txt(dom_or_source, OK_page=['bodyPage'], remove_catchwords=True, correct_ocr=False, **kw):
	"""Legacy wrapper: accepts bs4 dom or lxml Element, returns plain text."""
	if hasattr(dom_or_source, 'encode'):
		source = etree.fromstring(dom_or_source.encode())
	elif isinstance(dom_or_source, etree._Element):
		source = dom_or_source
	else:
		source = etree.fromstring(str(dom_or_source).encode())
	return ecco_xml2txt(source, page_types=set(OK_page), remove_catchwords=remove_catchwords)
