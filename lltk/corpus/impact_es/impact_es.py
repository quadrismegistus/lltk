import os
import re

import pandas as pd
from lltk.imports import BaseCorpus, BaseText, get_tqdm, log

IMPACT_ES_URL = 'https://www.digitisation.eu/knowledge/language-resources/impact-es-2/'

TEI_NS = 'http://www.tei-c.org/ns/1.0'


def xml2txt_impact_es(xmlpath):
    """Convert IMPACT-es TEI XML to plain text.

    Handles two formats found in the corpus:
    - GT section: plain text in <p> elements (skip <head> and <ab> page-numbers)
    - BVC section: <w>/<choice>/<reg> + <pc> annotated format (prefer <reg>)
    """
    try:
        from lxml import etree
    except ImportError:
        raise ImportError('Install lxml: pip install lxml')

    W = f'{{{TEI_NS}}}w'
    PC = f'{{{TEI_NS}}}pc'
    P = f'{{{TEI_NS}}}p'

    try:
        tree = etree.parse(xmlpath)
    except Exception:
        return ''

    body = tree.find(f'.//{{{TEI_NS}}}body')
    if body is None:
        return ''

    is_annotated = body.find(f'.//{W}') is not None

    if is_annotated:
        # BVC: whole doc is one annotated block; w/pc carry the text
        tokens = []
        for el in body.iter():
            if el.tag == W:
                reg_el = el.find(f'{{{TEI_NS}}}choice/{{{TEI_NS}}}reg')
                text = (reg_el.text if reg_el is not None else el.text) or ''
                text = text.strip()
                if text:
                    tokens.append(('w', text))
            elif el.tag == PC:
                if el.text:
                    tokens.append(('pc', el.text))
        parts = []
        for typ, text in tokens:
            if typ == 'pc' and parts:
                parts.append(text)
            else:
                if parts:
                    parts.append(' ')
                parts.append(text)
        return ''.join(parts)

    else:
        # GT: plain text paragraphs; <head> and <ab> are not <p>, so iter(P) skips them
        # De-hyphenate: if a paragraph ends with a hyphen (page-break word split),
        # strip the hyphen and join directly to the next paragraph's first word.
        paragraphs = []
        for p in body.iter(P):
            text = ' '.join(p.itertext()).strip()
            text = ' '.join(text.split())
            if not text:
                continue
            if paragraphs and paragraphs[-1].endswith('-'):
                paragraphs[-1] = paragraphs[-1][:-1] + text
            else:
                paragraphs.append(text)
        return '\n\n'.join(paragraphs)

# Zip name → subdirectory under xml/
ZIP_SECTIONS = {
    'BVC': 'BVC',
    'GT': 'GT',
}


class TextImpactES(BaseText):
    XML2TXT = xml2txt_impact_es


class ImpactES(BaseCorpus):
    TEXT_CLASS = TextImpactES

    def compile(self, force=False):
        """Download IMPACT-es corpus from digitisation.eu, unzip TEI sections."""
        try:
            import requests
            from bs4 import BeautifulSoup
        except ImportError:
            raise ImportError('Install requests and beautifulsoup4: pip install requests beautifulsoup4')

        raw_dir = os.path.join(self.path, 'raw')
        os.makedirs(raw_dir, exist_ok=True)

        # 1. Scrape download links from the corpus page
        log(f'Fetching {IMPACT_ES_URL}')
        resp = requests.get(IMPACT_ES_URL, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')

        links = []
        for a in soup.find_all('a', href=True):
            href = a['href']
            # Collect all file download links (zip, txt, dtd)
            if any(href.endswith(ext) for ext in ('.zip', '.txt', '.dtd')):
                if not href.startswith('http'):
                    from urllib.parse import urljoin
                    href = urljoin(IMPACT_ES_URL, href)
                links.append(href)

        if not links:
            raise RuntimeError(
                f'No download links found at {IMPACT_ES_URL}. '
                'The page structure may have changed — check manually.'
            )

        log(f'Found {len(links)} download links.')

        # 2. Download all files to raw/
        for url in get_tqdm(links, desc='[ImpactES] Downloading'):
            fname = url.rstrip('/').rsplit('/', 1)[-1]
            dest = os.path.join(raw_dir, fname)
            if os.path.exists(dest) and not force:
                continue
            r = requests.get(url, timeout=120, stream=True)
            r.raise_for_status()
            with open(dest, 'wb') as f:
                for chunk in r.iter_content(chunk_size=65536):
                    f.write(chunk)

        # 3. Unzip BVC and GT sections to xml/BVC/ and xml/GT/
        import zipfile
        for fname in os.listdir(raw_dir):
            if not fname.endswith('.zip'):
                continue
            section = None
            for key in ZIP_SECTIONS:
                if key in fname:
                    section = ZIP_SECTIONS[key]
                    break
            if section is None:
                continue
            xml_dest = os.path.join(self.path_xml, section)
            if os.path.isdir(xml_dest) and os.listdir(xml_dest) and not force:
                continue
            os.makedirs(self.path_xml, exist_ok=True)
            log(f'Unzipping {fname} → xml/')
            with zipfile.ZipFile(os.path.join(raw_dir, fname)) as zf:
                zf.extractall(self.path_xml)

        # 4. Build and save metadata.csv from TEI headers
        meta = self._parse_tei_metadata()
        if len(meta):
            meta.to_csv(self.path_metadata)
            log(f'[ImpactES] Saved {len(meta)} records to {self.path_metadata}')
        else:
            log('[ImpactES] Warning: no TEI metadata parsed — check xml/ dirs.')

        log('[ImpactES] Compile done.')

    def _parse_tei_metadata(self):
        try:
            from lxml import etree
        except ImportError:
            raise ImportError('Install lxml: pip install lxml')

        TEI_NS = 'http://www.tei-c.org/ns/1.0'

        def _text(el):
            return ' '.join(el.itertext()).strip() if el is not None else ''

        def _parse_header(xmlpath):
            try:
                tree = etree.parse(xmlpath)
            except Exception:
                return None
            root = tree.getroot()
            ns = {'t': TEI_NS} if TEI_NS in (root.nsmap or {}).values() else {}
            q = lambda tag: f'{{{TEI_NS}}}{tag}' if ns else tag

            bibl = root.find(f'.//{q("bibl")}')
            if bibl is None:
                return None

            title = _text(bibl.find(q('title')))
            author = _text(bibl.find(q('author')))

            # Prefer source-edition date; fall back to first-edition
            date_str = ''
            for date_el in bibl.findall(q('date')):
                dtype = date_el.get('type', '')
                val = _text(date_el) or date_el.get('when', '')
                if dtype == 'source-edition':
                    date_str = val
                    break
                elif dtype == 'first-edition' and not date_str:
                    date_str = val

            year_m = re.search(r'(\d{4})', date_str)
            year = int(year_m.group(1)) if year_m else None
            return {'title': title, 'author': author, 'year': year, 'date_raw': date_str}

        rows = []
        for section in ZIP_SECTIONS.values():
            section_dir = os.path.join(self.path_xml, section)
            if not os.path.isdir(section_dir):
                continue
            xml_files = [
                os.path.join(r, fn)
                for r, _, files in os.walk(section_dir)
                for fn in sorted(files) if fn.endswith('.xml')
            ]
            for xmlpath in get_tqdm(xml_files, desc=f'[ImpactES] Parsing {section} headers'):
                parsed = _parse_header(xmlpath)
                if parsed is None:
                    continue
                rel = os.path.relpath(xmlpath, self.path_xml)
                text_id = rel.replace(os.sep, '/').removesuffix('.xml')
                rows.append({'id': text_id, 'section': section, 'lang': 'spa', **parsed})

        if not rows:
            return pd.DataFrame()
        meta = pd.DataFrame(rows).set_index('id')
        meta['year'] = pd.to_numeric(meta['year'], errors='coerce')
        return meta

    def load_metadata(self):
        meta = super().load_metadata()
        if meta is not None and len(meta):
            return meta
        # CSV not yet built — parse on the fly (e.g. after manual unzip)
        return self._parse_tei_metadata()
