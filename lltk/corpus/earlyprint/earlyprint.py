"""
EarlyPrint corpus — linguistically tagged TCP texts from the EarlyPrint Project.

Combines EEBO-TCP (~60K texts), ECCO-TCP (~2K), and Evans-TCP (~4K) with
enhanced XML tagging (lemmatization, POS, regularized spelling).

Source: https://earlyprint.org
Repos: bitbucket.org/eads004/{eebotcp,eccotcp,evanstcp,eptexts}

TCP ID prefixes:
  A = EEBO-TCP
  K = ECCO-TCP
  N = Evans-TCP

Usage:
    lltk compile earlyprint     # clones repos, symlinks XML, builds metadata
    lltk preprocess earlyprint  # generates txt and freqs from XML
"""

from lltk.imports import *
from lltk.corpus.tcp import TextTCP, TCP, extract_metadata, xml2txt_tcp


EARLYPRINT_REPOS = {
    'eebotcp': 'https://bitbucket.org/eads004/eebotcp.git',
    'eccotcp': 'https://bitbucket.org/eads004/eccotcp.git',
    'evanstcp': 'https://bitbucket.org/eads004/evanstcp.git',
}

# Map TCP ID prefix to source collection
TCP_PREFIX_TO_SOURCE = {
    'A': 'eebo',
    'B': 'eebo',
    'K': 'ecco',
    'N': 'evans',
}


def xml2txt_earlyprint(xmlfn, use_reg=True):
    """Extract plain text from EarlyPrint TEI XML.

    Uses linguistically tagged <w> elements with optional regularized spelling
    (reg attribute) for modernized output. Falls back to surface form when reg
    is absent. Punctuation from <pc> elements is attached without leading space.

    Extracts from <p> (paragraph) and <l> (verse line) elements within <body>.

    Args:
        xmlfn: Path to .xml or .xml.gz file.
        use_reg: If True, prefer reg attribute over surface text on <w> tags.
    """
    from lxml import etree
    import gzip as _gzip

    TEI = 'http://www.tei-c.org/ns/1.0'
    W = f'{{{TEI}}}w'
    PC = f'{{{TEI}}}pc'
    BLOCK_TAGS = {f'{{{TEI}}}p', f'{{{TEI}}}l'}

    opener = _gzip.open if xmlfn.endswith('.gz') else open
    with opener(xmlfn, 'rb') as f:
        tree = etree.parse(f)

    body = tree.find(f'.//{{{TEI}}}body')
    if body is None:
        return ''

    paragraphs = []
    for block in body.iter():
        if block.tag not in BLOCK_TAGS:
            continue
        tokens = []
        for el in block.iter():
            if el.tag == W:
                text = (el.get('reg') if use_reg and el.get('reg') else el.text) or ''
                if text:
                    tokens.append(('w', text))
            elif el.tag == PC:
                if el.text:
                    tokens.append(('pc', el.text))
        # Join: space before words, no space before punctuation
        parts = []
        for typ, text in tokens:
            if typ == 'pc' and parts:
                parts.append(text)
            else:
                if parts:
                    parts.append(' ')
                parts.append(text)
        line = ''.join(parts).strip()
        if line:
            paragraphs.append(line)

    return '\n\n'.join(paragraphs)


def _gzip_copy_one(src_dst):
    """Gzip-copy a single XML file. Worker for parallel compress."""
    import gzip as _gzip
    import shutil
    src, dst = src_dst
    with open(src, 'rb') as f_in:
        with _gzip.open(dst, 'wb', compresslevel=6) as f_out:
            shutil.copyfileobj(f_in, f_out)


class TextEarlyPrint(TextTCP):
    XML2TXT = xml2txt_earlyprint


class EarlyPrint(TCP):
    TEXT_CLASS = TextEarlyPrint
    XML2TXT = xml2txt_earlyprint
    EXT_XML = '.xml.gz'

    LINKS = {
        'estc': ('id_estc', 'id_estc'),
    }
    MATCH_LINKS = {
        'eebo_tcp': ('id', 'id'),
        'ecco_tcp': ('id_tcp', 'id_TCP'),
        'evans_tcp': ('id', 'id'),
    }

    @property
    def path_repos(self):
        """Directory where git repos are cloned."""
        return os.path.join(self.path, 'repos')

    def compile(self, repos=None, **kwargs):
        """
        Clone EarlyPrint repos and set up XML symlinks + metadata.

        Steps:
        1. Clone each TCP repo (eebotcp, eccotcp, evanstcp) into repos/
        2. Init and update git submodules
        3. Symlink all XML files into xml/{repo}/{prefix}/{ID}.xml
        4. Build metadata.csv from XML headers
        """
        if repos is None:
            repos = list(EARLYPRINT_REPOS.keys())

        # Step 1-2: Clone repos
        for repo_name in repos:
            url = EARLYPRINT_REPOS.get(repo_name)
            if not url:
                print(f'Unknown repo: {repo_name}')
                continue
            self._clone_repo(repo_name, url)

        # Step 3: Gzip-copy XMLs (10x compression)
        self._gzip_copy_xmls(repos)

        # Step 4: Build metadata
        self._build_metadata()

    def update(self, repos=None, **kwargs):
        """Pull latest changes from EarlyPrint repos, re-gzip new files, rebuild metadata."""
        if repos is None:
            repos = [r for r in EARLYPRINT_REPOS if os.path.exists(os.path.join(self.path_repos, r, '.git'))]
        if not repos:
            print('No repos found. Run compile first.')
            return

        for repo_name in repos:
            repo_dir = os.path.join(self.path_repos, repo_name)
            print(f'  {repo_name}: pulling updates...')
            os.system(f'cd {repo_dir} && git pull')
            os.system(f'cd {repo_dir} && git submodule update --depth 1')

        # Re-gzip any new/changed files
        self._gzip_copy_xmls(repos)
        # Rebuild metadata
        self._build_metadata()

    def _clone_repo(self, name, url):
        """Clone a single EarlyPrint repo with shallow submodules. Resumable."""
        repo_dir = os.path.join(self.path_repos, name)
        os.makedirs(self.path_repos, exist_ok=True)

        if not os.path.exists(os.path.join(repo_dir, '.git')):
            print(f'  {name}: cloning {url} (shallow)...')
            ret = os.system(f'git clone --depth 1 {url} {repo_dir}')
            if ret != 0:
                print(f'  {name}: clone failed')
                return
            print(f'  {name}: initializing submodules...')
            os.system(f'cd {repo_dir} && git submodule init')
            # Convert SSH URLs to HTTPS for public access
            os.system(
                f"cd {repo_dir} && perl -pi -e "
                f"'s|git\\@bitbucket.org:|https://bitbucket.org/|' .git/config"
            )
        else:
            print(f'  {name}: repo exists, resuming submodule update...')

        print(f'  {name}: updating submodules (shallow, resumable)...')
        os.system(f'cd {repo_dir} && git submodule update --depth 1')
        print(f'  {name}: done')

    def _gzip_copy_xmls(self, repos=None):
        """Gzip-copy XMLs from repos to xml/{ID}.xml.gz (flat directory)."""
        if repos is None:
            repos = list(EARLYPRINT_REPOS.keys())

        xml_dir = self.path_xml
        os.makedirs(xml_dir, exist_ok=True)
        total = 0

        for repo_name in repos:
            repo_texts = os.path.join(self.path_repos, repo_name, 'texts')
            if not os.path.exists(repo_texts):
                print(f'  {repo_name}: no texts/ directory found')
                continue

            # Collect files to compress — flat destination: xml/{ID}.xml.gz
            to_compress = []
            for root, dirs, files in os.walk(repo_texts):
                for fn in files:
                    if not fn.endswith('.xml'):
                        continue
                    src = os.path.join(root, fn)
                    dst = os.path.join(xml_dir, fn + '.gz')
                    if not os.path.exists(dst):
                        to_compress.append((src, dst))

            if not to_compress:
                # Count existing files for this repo by checking IDs
                print(f'  {repo_name}: all XML files already compressed')
                continue

            print(f'  {repo_name}: compressing {len(to_compress)} XML files...')
            pmap(_gzip_copy_one, to_compress, use_threads=True,
                 num_proc=DEFAULT_NUM_PROC, desc=f'  [{repo_name}] Gzipping')
            total += len(to_compress)

        existing = sum(1 for fn in os.listdir(xml_dir) if fn.endswith('.xml.gz'))
        print(f'  Total: {existing} XML.gz files in {xml_dir}')

    def _build_metadata(self):
        """Build metadata.csv by parsing XML headers."""
        print('  Building metadata from XML headers...')
        xml_dir = self.path_xml
        objs = []

        for root, dirs, files in os.walk(xml_dir):
            for fn in files:
                if fn.endswith('.xml') or fn.endswith('.xml.gz'):
                    objs.append(os.path.join(root, fn))

        print(f'  Parsing {len(objs)} XML files...')
        ld = pmap(_parse_earlyprint_meta, objs, num_proc=DEFAULT_NUM_PROC,
                  use_threads=True, desc='[EarlyPrint] Parsing metadata')

        df = pd.DataFrame([d for d in ld if d])
        if not len(df):
            print('  No metadata extracted')
            return

        df = fix_meta(df)
        if 'id' in df.columns:
            df = df.set_index('id')
        df.to_csv(self.path_metadata)
        print(f'  Saved {len(df)} records to {self.path_metadata}')

    def load_metadata(self, *x, **y):
        from lltk.corpus.eebo_tcp.eebo_tcp import _normalize_estc_id
        meta = super().load_metadata(*x, **y)
        if not len(meta):
            return meta

        # Normalize ESTC IDs for linking: strip "ESTC " prefix, zero-pad to Letter+6 digits
        if 'id_estc' in meta.columns:
            meta['id_estc_orig'] = meta['id_estc']
            meta['id_estc'] = meta['id_estc'].apply(_normalize_estc_id)

        # Merge ESTC metadata for genre etc.
        meta = self.merge_linked_metadata(meta)

        # Inherit genre from ESTC
        if 'estc_genre' in meta.columns:
            meta['genre'] = meta['estc_genre']
        if 'estc_genre_raw' in meta.columns:
            meta['genre_raw'] = meta['estc_genre_raw']
        if 'estc_is_translated' in meta.columns:
            meta['is_translated'] = meta['estc_is_translated']

        # Medium overrides genre (Verse→Poetry, Drama→Drama)
        if 'medium' in meta.columns:
            meta.loc[meta['medium'] == 'Verse', 'genre'] = 'Poetry'
            meta.loc[meta['medium'] == 'Drama', 'genre'] = 'Drama'

        # Determine source collection from TCP ID prefix
        if meta.index.name == 'id' or 'id' in meta.columns:
            ids = meta.index if meta.index.name == 'id' else meta['id']
            meta['tcp_source'] = ids.map(
                lambda x: TCP_PREFIX_TO_SOURCE.get(str(x)[0], 'unknown')
            )

        return meta


def _parse_earlyprint_meta(fnfn):
    """Parse metadata from an EarlyPrint TEI XML file."""
    try:
        import bs4
        import gzip as _gzip
        opener = _gzip.open if fnfn.endswith('.gz') else open
        with opener(fnfn, 'rt', encoding='utf-8', errors='ignore') as f:
            full_txt = f.read()

        # Split header for metadata parsing
        header_end = full_txt.lower().find('</teiheader>')
        header_txt = full_txt[:header_end + 12] if header_end > 0 else full_txt[:10000]

        dom = bs4.BeautifulSoup(header_txt, 'lxml')
        meta = {}

        # ID from filename
        basename = os.path.basename(fnfn)
        # Strip .xml.gz or .xml
        if basename.endswith('.xml.gz'):
            basename = basename[:-7]
        elif basename.endswith('.xml'):
            basename = basename[:-4]
        meta['id'] = basename

        # ── IDs from <idno> tags ──
        for idno in dom.find_all('idno'):
            id_type = (idno.get('type') or '').strip()
            id_val = idno.get_text(strip=True)
            if id_type and id_val:
                key = 'id_' + id_type.lower().replace(' ', '_').replace('-', '_')
                meta[key] = id_val

        # Normalize ESTC ID: "ESTC S112788" → "S112788"
        estc_raw = meta.get('id_stc', '')
        if estc_raw.startswith('ESTC '):
            meta['id_estc'] = estc_raw[5:].strip()
            meta['id_stc'] = estc_raw  # keep original too
        elif estc_raw.startswith('STC '):
            meta['id_stc_wing'] = estc_raw

        # ── Title from <titleStmt> (first title only) ──
        title_stmt = dom.find('titlestmt')
        if title_stmt:
            title_tag = title_stmt.find('title')
            if title_tag:
                meta['title'] = title_tag.get_text(strip=True)

        # ── Author from <biblFull> <titleStmt> <author> ──
        biblfull = dom.find('biblfull')
        if biblfull:
            author_tag = biblfull.find('author')
            if author_tag:
                meta['author'] = author_tag.get_text(strip=True)
            # Extent
            extent_tag = biblfull.find('extent')
            if extent_tag:
                meta['extent'] = extent_tag.get_text(strip=True)
            # Publisher info from biblFull publicationStmt
            pub_stmt = biblfull.find('publicationstmt')
            if pub_stmt:
                pub = pub_stmt.find('publisher')
                if pub:
                    meta['publisher'] = pub.get_text(strip=True)
                place = pub_stmt.find('pubplace')
                if place:
                    meta['pubplace'] = place.get_text(strip=True)
                date = pub_stmt.find('date')
                if date:
                    meta['date'] = date.get_text(strip=True)
            # Notes
            notes = biblfull.find('notesstmt')
            if notes:
                note_texts = [n.get_text(strip=True) for n in notes.find_all('note')]
                meta['notes'] = ' | '.join(note_texts)

        # ── Language ──
        lang_tag = dom.find('language')
        if lang_tag:
            meta['language'] = lang_tag.get('ident', '') or lang_tag.get_text(strip=True)

        # ── Subject keywords ──
        keywords = dom.find('keywords')
        if keywords:
            terms = [t.get_text(strip=True) for t in keywords.find_all('term')]
            meta['subject'] = ' | '.join(terms)

        # ── EarlyPrint epHeader (structured metadata) ──
        ep_header = dom.find('ep:epheader') or dom.find('epheader')
        if ep_header:
            ep_fields = {
                'ep:corpus': 'ep_corpus',
                'ep:title': 'ep_title',
                'ep:author': 'ep_author',
                'ep:publicationyear': 'year',
                'ep:creationyear': 'year_creation',
                'ep:pagecount': 'num_pages',
                'ep:wordcount': 'num_words',
                'ep:defectivetokencount': 'ep_defective_tokens',
                'ep:finalgrade': 'ep_quality_grade',
                'ep:defectrate': 'ep_defect_rate',
            }
            for tag_name, meta_key in ep_fields.items():
                # Try namespaced and non-namespaced
                tag = ep_header.find(tag_name) or ep_header.find(tag_name.split(':')[-1])
                if tag:
                    val = tag.get_text(strip=True)
                    if val and val != '[no entry]':
                        meta[meta_key] = val

        # ── Parse year to int ──
        year_str = meta.get('year', meta.get('date', ''))
        if year_str:
            digits = ''.join(c for c in str(year_str) if c.isdigit())[:4]
            if digits:
                try:
                    meta['year'] = int(digits)
                except ValueError:
                    pass

        # ── Numeric fields ──
        for key in ('num_pages', 'num_words', 'ep_defective_tokens'):
            if key in meta:
                try:
                    meta[key] = int(meta[key])
                except (ValueError, TypeError):
                    pass

        # ── Detect medium (Prose/Verse/Drama) from body tag counts ──
        txt_lower = full_txt.lower()
        n_speaker = txt_lower.count('</speaker>')
        n_l = txt_lower.count('</l>')
        n_p = txt_lower.count('</p>')
        if n_speaker > 100:
            meta['medium'] = 'Drama'
        elif n_l > n_p:
            meta['medium'] = 'Verse'
        else:
            meta['medium'] = 'Prose'

        # ── Track source repo from TCP ID prefix ──
        prefix_map = {'A': 'eebotcp', 'B': 'eebotcp', 'C': 'eccotcp', 'E': 'eebotcp', 'K': 'eccotcp', 'N': 'evanstcp'}
        tcp_id = meta.get('id', '')
        if tcp_id and tcp_id[0] in prefix_map:
            meta['ep_repo'] = prefix_map[tcp_id[0]]

        return meta
    except Exception as e:
        return None
