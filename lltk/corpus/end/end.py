"""
Early Novels Database (END) corpus.

2,002 MARCXML records of early English novels (1660-1830) from the University
of Pennsylvania's Collection of British and American Fiction. Rich metadata
including narrative form, author gender, paratexts, epigraphs, and bibliography
cross-references (ESTC, Raven, Garside, Block, McBurney, etc.).

Source: github.com/earlynovels/end-dataset
"""
from lltk.imports import *
import re
import xml.etree.ElementTree as ET
import urllib.request

GITHUB_XML_URL = (
    'https://raw.githubusercontent.com/earlynovels/end-dataset/master/'
    'end-dataset-master-11282018/11282018-full.xml'
)

MARC_NS = {'marc': 'http://www.loc.gov/MARC21/slim'}


class TextEND(BaseText):
    pass


class END(BaseCorpus):
    TEXT_CLASS = TextEND
    LINKS = {'estc': ('id_estc', 'id_estc')}

    def compile(self, xml_path=None):
        """
        Parse END MARCXML and write metadata.csv.

        Downloads XML from GitHub if not provided or cached locally.
        """
        if xml_path is None:
            xml_path = os.path.join(self.path, 'end-full.xml')

        if not os.path.exists(xml_path):
            if log: log(f'Downloading END XML from GitHub...')
            urllib.request.urlretrieve(GITHUB_XML_URL, xml_path)
            if log: log(f'Saved to {xml_path}')

        if log: log(f'Parsing {xml_path}')
        tree = ET.parse(xml_path)
        root = tree.getroot()
        records = root.findall('marc:record', MARC_NS)
        if log: log(f'Found {len(records)} records')

        rows = []
        for rec in records:
            row = _parse_record(rec)
            if row:
                rows.append(row)

        meta = pd.DataFrame(rows)
        if log: log(f'Parsed {len(meta)} records')

        # Assign IDs
        meta['id'] = ['end_{:05d}'.format(i + 1) for i in range(len(meta))]
        meta = meta.set_index('id')

        # Normalize ESTC IDs to standard format (letter + 6 zero-padded digits)
        if 'id_estc' in meta.columns:
            meta['id_estc'] = meta['id_estc'].apply(_normalize_estc_id)

        # Parse year
        meta['year'] = pd.to_numeric(meta['year'], errors='coerce').astype('Int64')

        # All entries are fiction
        meta['genre'] = 'Fiction'

        # Save
        out_path = os.path.join(self.path, 'metadata.csv')
        meta.to_csv(out_path)
        if log: log(f'Wrote {len(meta)} records to {out_path}')

        # Clear parquet cache
        parquet_path = os.path.join(self.path, 'metadata.parquet')
        if os.path.exists(parquet_path):
            os.remove(parquet_path)

        return meta

    def load_metadata(self, *x, **y):
        df = super().load_metadata(*x, **y)
        if not len(df):
            return df
        meta = self.merge_linked_metadata(df)
        # Prefer ESTC title/author/year when available, keep originals as _end suffix
        for c in ['author', 'title', 'year']:
            c2 = 'estc_' + c
            if c2 in meta.columns:
                meta[c + '_end'] = meta[c]
                meta[c] = meta[c2].where(meta[c2].notna() & (meta[c2] != ''), meta[c])
        meta['genre'] = 'Fiction'
        if 'estc_genre_raw' in meta.columns:
            meta['genre_raw'] = meta['estc_genre_raw']

        # Enrich genre_raw from narrative_form where genre_raw is missing
        if 'narrative_form' in meta.columns:
            needs_genre = meta['genre_raw'].isna() | (meta['genre_raw'] == '')
            nf = meta['narrative_form'].fillna('')
            is_epistolary = nf.str.contains(r'Epistolary|Letters', case=False, na=False)
            is_first = nf.str.contains(r'First-person', case=False, na=False) & ~is_epistolary
            is_third = nf.str.contains(r'Third-person', case=False, na=False) & ~is_epistolary
            meta.loc[needs_genre & is_epistolary, 'genre_raw'] = 'Novel, epistolary'
            meta.loc[needs_genre & (is_first | is_third), 'genre_raw'] = 'Novel'

        return meta


# ═══════════════════════════════════════════════════════════════════
# MARC record parsing
# ═══════════════════════════════════════════════════════════════════

def _get_controlfield(rec, tag):
    """Get text of a control field."""
    el = rec.find(f'marc:controlfield[@tag="{tag}"]', MARC_NS)
    return el.text if el is not None else None


def _get_datafields(rec, tag):
    """Get all datafield elements with a given tag."""
    return rec.findall(f'marc:datafield[@tag="{tag}"]', MARC_NS)


def _get_subfields(df_el, code):
    """Get all subfield text values with a given code from a datafield element."""
    return [sf.text for sf in df_el.findall(f'marc:subfield[@code="{code}"]', MARC_NS)
            if sf.text]


def _get_first_subfield(df_el, code):
    """Get first subfield text value with a given code."""
    vals = _get_subfields(df_el, code)
    return vals[0] if vals else None


def _parse_record(rec):
    """Parse a single END MARCXML record into a flat dict."""
    row = {}

    # ── Control fields ───────────────────────────────────────────
    row['catalog_id'] = _get_controlfield(rec, '001')

    # 008: fixed-length data
    f008 = _get_controlfield(rec, '008')
    if f008 and len(f008) >= 15:
        date_str = f008[7:11].strip()
        if date_str.isdigit():
            row['year'] = int(date_str)
        lang = f008[35:38].strip() if len(f008) >= 38 else ''
        if lang:
            row['lang'] = lang

    # ── 035: System identifiers ──────────────────────────────────
    end_id = None
    for df in _get_datafields(rec, '035'):
        a = _get_first_subfield(df, 'a')
        if a and a.startswith('(END)'):
            end_id = a[5:]
    row['id_end'] = end_id or ''

    # ── 041: Language codes ──────────────────────────────────────
    for df in _get_datafields(rec, '041'):
        row['lang_text'] = _pipe_join(_get_subfields(df, 'a'))
        row['lang_original'] = _pipe_join(_get_subfields(df, 'h'))

    # ── 100: Author ──────────────────────────────────────────────
    for df in _get_datafields(rec, '100'):
        row['author'] = _get_first_subfield(df, 'a') or ''
        row['author_dates'] = _clean_trailing_punct(_get_first_subfield(df, 'd') or '')
        row['author_qualifier'] = _get_first_subfield(df, 'c') or ''
        row['author_fuller'] = _get_first_subfield(df, 'q') or ''

    # ── 240: Uniform title ───────────────────────────────────────
    for df in _get_datafields(rec, '240'):
        row['uniform_title'] = _get_first_subfield(df, 'a') or ''
        row['uniform_title_lang'] = _get_first_subfield(df, 'l') or ''

    # ── 245: Title ───────────────────────────────────────────────
    for df in _get_datafields(rec, '245'):
        row['title'] = _clean_trailing_punct(_get_first_subfield(df, 'a') or '')
        row['title_sub'] = _clean_trailing_punct(_get_first_subfield(df, 'b') or '')
        row['title_statement'] = _get_first_subfield(df, 'c') or ''

    # ── 246: Title variations (full title from first instance) ───
    dfs_246 = _get_datafields(rec, '246')
    if dfs_246:
        row['title_full'] = _get_first_subfield(dfs_246[0], 'a') or ''
        row['num_volumes_title'] = len(dfs_246)

    # ── 250: Edition ─────────────────────────────────────────────
    for df in _get_datafields(rec, '250'):
        row['edition'] = _get_first_subfield(df, 'a') or ''
        row['edition_controlled'] = _get_first_subfield(df, 'b') or ''

    # ── 260: Publication ─────────────────────────────────────────
    for df in _get_datafields(rec, '260'):
        row['pub_place'] = _get_first_subfield(df, 'a') or ''
        row['publisher'] = _clean_trailing_punct(_get_first_subfield(df, 'b') or '')
        row['pub_date'] = _clean_trailing_punct(_get_first_subfield(df, 'c') or '')

    # ── 261: Printer/bookseller ──────────────────────────────────
    for df in _get_datafields(rec, '261'):
        row['printer_location'] = _get_first_subfield(df, 'a') or ''
        row['printer'] = _get_first_subfield(df, 'b') or ''

    # ── 300: Physical description ────────────────────────────────
    for df in _get_datafields(rec, '300'):
        row['extent'] = _get_first_subfield(df, 'a') or ''
        row['dimensions'] = _get_first_subfield(df, 'c') or ''
        row['format'] = _get_first_subfield(df, 'x') or ''
        row['illustrations'] = _get_first_subfield(df, 'b') or ''
        row['phys_note'] = _get_first_subfield(df, 'z') or ''
        # Parse num_volumes from extent
        extent = row['extent']
        m = re.match(r'(\d+)\s*v', extent)
        if m:
            row['num_volumes'] = int(m.group(1))
        else:
            m2 = re.match(r'\[(\d+)\s*v', extent)
            if m2:
                row['num_volumes'] = int(m2.group(1))

    # ── 500: Notes ───────────────────────────────────────────────
    notes = []
    for df in _get_datafields(rec, '500'):
        a = _get_first_subfield(df, 'a')
        if a:
            notes.append(a)
    if notes:
        row['notes'] = ' || '.join(notes)

    # ── 505: Contents ────────────────────────────────────────────
    contents = []
    for df in _get_datafields(rec, '505'):
        a = _get_first_subfield(df, 'a')
        if a:
            contents.append(a)
    if contents:
        row['contents'] = ' || '.join(contents)

    # ── 510: Bibliography references ─────────────────────────────
    refs = []
    estc_ids = []
    biblio_refs = {}
    for df in _get_datafields(rec, '510'):
        a = _get_first_subfield(df, 'a') or ''
        c = _get_first_subfield(df, 'c') or ''
        ref_str = f"{a.strip()} {c.strip()}".strip()
        if ref_str:
            refs.append(ref_str)

        a_upper = a.upper().strip()
        if 'ESTC' in a_upper and c.strip():
            estc_ids.append(c.strip())
        else:
            # Classify bibliography reference
            a_low = a.lower().strip()
            if 'mcburney' in a_low:
                biblio_refs['mcburney'] = c.strip()
            elif 'beasley' in a_low:
                biblio_refs['beasley'] = c.strip()
            elif a_low.startswith('english novel, 1770'):
                biblio_refs['garside'] = c.strip()
            elif 'raven' in a_low:
                biblio_refs['raven'] = c.strip()
            elif 'block' in a_low:
                biblio_refs['block'] = c.strip()
            elif 'halkett' in a_low:
                biblio_refs['halkett_laing'] = c.strip()
            elif 'esdaile' in a_low:
                biblio_refs['esdaile'] = c.strip()
            elif 'evans' in a_low and 'novel' not in a_low:
                biblio_refs['evans'] = c.strip()
            elif 'wright' in a_low:
                biblio_refs['wright'] = c.strip()
            elif 'shaw' in a_low or 'shoemaker' in a_low:
                biblio_refs['shaw_shoemaker'] = c.strip()
            elif 'teerink' in a_low:
                biblio_refs['teerink'] = c.strip()
            elif 'black' in a_low and 'epistolary' in a_low:
                biblio_refs['black_epistolary'] = c.strip()
            elif 'blakey' in a_low or 'minerva' in a_low:
                biblio_refs['blakey_minerva'] = c.strip()
            elif 'sabin' in a_low:
                biblio_refs['sabin'] = c.strip()
            elif 'bal' == a_low.rstrip('.').strip():
                biblio_refs['bal'] = c.strip()

    if refs:
        row['references'] = ' || '.join(refs)
    if estc_ids:
        row['id_estc'] = estc_ids[0]  # Take first ESTC ID
        if len(estc_ids) > 1:
            row['id_estc_all'] = '|'.join(estc_ids)
    for key, val in biblio_refs.items():
        row['ref_' + key] = val

    # ── 520: Paratexts ───────────────────────────────────────────
    paratexts = []
    for df in _get_datafields(rec, '520'):
        para_type = _get_first_subfield(df, 'a') or ''
        para_first_sentence = _get_first_subfield(df, 'b') or ''
        para_position = _get_first_subfield(df, 'c') or ''
        if para_type:
            paratexts.append({
                'type': para_type.strip(),
                'first_sentence': para_first_sentence.strip(),
                'position': para_position.strip(),
            })

    # Count paratext types
    para_types = [p['type'].lower() for p in paratexts]
    row['has_preface'] = any('preface' in t for t in para_types)
    row['has_dedication'] = any('dedication' in t for t in para_types)
    row['has_advertisement'] = any('advertisement' in t for t in para_types)
    row['has_introduction'] = any('intro' in t for t in para_types)
    row['has_footnotes'] = any('footnote' in t for t in para_types)
    row['paratexts'] = _encode_list_of_dicts(paratexts) if paratexts else ''

    # ── 591: Epigraphs ───────────────────────────────────────────
    epigraphs = []
    for df in _get_datafields(rec, '591'):
        text = _get_first_subfield(df, 'a') or ''
        source_author = _get_first_subfield(df, 'c') or ''
        source_work = _get_first_subfield(df, 'b') or ''
        location = _get_first_subfield(df, 'd') or ''
        if text:
            epigraphs.append({
                'text': text.strip(),
                'source_author': source_author.strip(),
                'source_work': source_work.strip(),
                'location': location.strip(),
            })
    row['num_epigraphs'] = len(epigraphs)
    row['epigraphs'] = _encode_list_of_dicts(epigraphs) if epigraphs else ''

    # ── 592: Narrative form ──────────────────────────────────────
    for df in _get_datafields(rec, '592'):
        primary = _get_subfields(df, 'a')
        secondary = _get_subfields(df, 'b')
        row['narrative_form'] = _pipe_join(primary)
        row['narrative_form_secondary'] = _pipe_join(secondary)

    # ── 593: Subject matter ──────────────────────────────────────
    for df in _get_datafields(rec, '593'):
        row['subject_matter'] = _pipe_join(_get_subfields(df, 'a'))

    # ── 594: Inscriptions ────────────────────────────────────────
    inscriptions = []
    for df in _get_datafields(rec, '594'):
        text = _get_first_subfield(df, 'a') or ''
        medium = _get_first_subfield(df, 'b') or ''
        if text:
            inscriptions.append(f"{medium}: {text}" if medium else text)
    if inscriptions:
        row['inscriptions'] = ' || '.join(inscriptions)

    # ── 595: Marginalia ──────────────────────────────────────────
    marginalia = []
    for df in _get_datafields(rec, '595'):
        medium = _get_first_subfield(df, 'a') or ''
        content = _get_first_subfield(df, 'b') or ''
        if medium or content:
            marginalia.append(f"{medium}: {content}" if medium and content else medium or content)
    row['has_marginalia'] = len(marginalia) > 0
    if marginalia:
        row['marginalia'] = ' || '.join(marginalia)

    # ── 596: Translation ─────────────────────────────────────────
    for df in _get_datafields(rec, '596'):
        trans_type = _get_first_subfield(df, 'a') or ''
        trans_source = _get_first_subfield(df, 'b') or ''
        trans_lang = _get_first_subfield(df, 'c') or ''
        trans_evidence = _get_first_subfield(df, 'e') or ''
        if trans_type.lower().startswith(('translation', 'adaptation', 'abridg',
                                          'revision', 'translated', 'continuation')):
            row['is_translated'] = True
            row['translated_from'] = trans_lang.strip()
            row['translation_type'] = trans_type.strip()
            row['translation_evidence'] = trans_evidence.strip()
            row['translation_source'] = trans_source.strip()

    # ── 599: Authorship ──────────────────────────────────────────
    for df in _get_datafields(rec, '599'):
        claim = _get_first_subfield(df, 'a') or ''
        claim_type = _get_first_subfield(df, '3') or ''
        gender_claim = _get_first_subfield(df, '5') or ''
        gender = _get_first_subfield(df, '6') or ''
        row['author_claim'] = claim.strip()
        row['author_claim_type'] = claim_type.strip()
        row['author_gender_claim'] = _normalize_gender(gender_claim)
        row['author_gender'] = _normalize_gender(gender)

    # ── 600: Subject persons ─────────────────────────────────────
    subj_persons = []
    for df in _get_datafields(rec, '600'):
        name = _get_first_subfield(df, 'a') or ''
        title = _get_first_subfield(df, 'c') or ''
        dates = _get_first_subfield(df, 'd') or ''
        parts = [p for p in [name, title, dates] if p]
        if parts:
            subj_persons.append(', '.join(parts))
    if subj_persons:
        row['subject_persons'] = '|'.join(subj_persons)

    # ── 650: Subject topics ──────────────────────────────────────
    # In END, 650 is used for CHR (chronological) and PRO (provenance)
    chr_dates = []
    for df in _get_datafields(rec, '650'):
        a = _get_first_subfield(df, 'a') or ''
        if a.startswith('CHR '):
            chr_dates.append(a[4:])
    if chr_dates:
        row['chr_date'] = '|'.join(chr_dates)

    # ── 655: Genre/form terms ────────────────────────────────────
    forms = []
    for df in _get_datafields(rec, '655'):
        a = _get_first_subfield(df, 'a')
        if a:
            forms.append(a)
    if forms:
        row['form_terms'] = '|'.join(forms)

    # ── 656: Advertisement genres ────────────────────────────────
    adverts = []
    for df in _get_datafields(rec, '656'):
        genre = _get_first_subfield(df, 'a') or ''
        position = _get_first_subfield(df, 'b') or ''
        publisher = _get_first_subfield(df, 'c') or ''
        if genre:
            adverts.append({
                'genre': genre.strip(),
                'position': position.strip(),
                'publisher': publisher.strip(),
            })
    if adverts:
        row['advertisements'] = _encode_list_of_dicts(adverts)

    # ── 700: Added persons (publishers, booksellers, etc.) ───────
    publishers = []
    for df in _get_datafields(rec, '700'):
        name = _get_first_subfield(df, 'a') or ''
        role = _get_first_subfield(df, '4') or ''
        auth = _get_first_subfield(df, '5') or ''
        if name and role:
            publishers.append(f"{name} ({role})")
    for df in _get_datafields(rec, '710'):
        name = _get_first_subfield(df, 'a') or ''
        role = _get_first_subfield(df, '4') or ''
        # Skip institutional entries (Early Novels, Collection of British...)
        if name and role and 'Early Novels' not in name and 'Collection of' not in name:
            publishers.append(f"{name} ({role})")
    if publishers:
        row['added_persons'] = '|'.join(publishers)

    # ── 740: Alternate titles ────────────────────────────────────
    alt_titles = []
    for df in _get_datafields(rec, '740'):
        a = _get_first_subfield(df, 'a')
        if a:
            alt_titles.append(a)
    if alt_titles:
        row['alt_titles'] = '|'.join(alt_titles)

    # ── 752: Place ───────────────────────────────────────────────
    for df in _get_datafields(rec, '752'):
        row['pub_country'] = _get_first_subfield(df, 'a') or ''
        row['pub_city'] = _get_first_subfield(df, 'd') or ''

    # ── 856: URLs ────────────────────────────────────────────────
    urls = []
    for df in _get_datafields(rec, '856'):
        u = _get_first_subfield(df, 'u')
        if u:
            urls.append(u)
    if urls:
        row['urls'] = '|'.join(urls)

    # ── 852: Holdings ────────────────────────────────────────────
    holdings = []
    for df in _get_datafields(rec, '852'):
        inst = _get_first_subfield(df, 'b') or _get_first_subfield(df, 'a') or ''
        if inst:
            holdings.append(inst)
    if holdings:
        row['holdings'] = '|'.join(holdings)

    # ── Defaults ─────────────────────────────────────────────────
    row.setdefault('author', '')
    row.setdefault('title', '')
    row.setdefault('is_translated', False)

    return row


# ═══════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════

def _pipe_join(vals):
    """Join a list of strings with pipe separator, filtering empty."""
    return '|'.join(v.strip() for v in vals if v and v.strip())


def _clean_trailing_punct(s):
    """Strip trailing punctuation common in MARC fields."""
    return s.rstrip(' /.:,;')


def _normalize_estc_id(val):
    """Normalize ESTC ID to letter + 6 zero-padded digits."""
    if not val or (isinstance(val, float) and val != val):
        return ''
    val = str(val).strip()
    if not val:
        return ''
    m = re.match(r'([A-Za-z])(\d+)', val)
    if m:
        return m.group(1).upper() + m.group(2).zfill(6)
    return val


def _normalize_gender(val):
    """Normalize gender strings to Male/Female/Indeterminate."""
    if not val:
        return ''
    v = val.strip().rstrip('."').strip()
    v_low = v.lower()
    if v_low.startswith('female'):
        return 'Female'
    elif v_low.startswith('male'):
        return 'Male'
    elif v_low.startswith('indetermin'):  # includes typo "indetermiante"
        return 'Indeterminate'
    elif v_low in ('unidentified', 'unknown'):
        return 'Indeterminate'
    elif v_low == 'to the reader':
        return ''  # data entry error
    return v


def _encode_list_of_dicts(lst):
    """Encode a list of dicts as a JSON string for CSV storage."""
    import json
    return json.dumps(lst, ensure_ascii=False)
