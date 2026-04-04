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


class TextEarlyPrint(TextTCP):
    pass


class EarlyPrint(TCP):
    TEXT_CLASS = TextEarlyPrint
    XML2TXT = xml2txt_tcp

    LINKS = {
        'estc': ('id_stc', 'id_estc'),
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

        # Step 3: Symlink XMLs
        self._symlink_xmls(repos)

        # Step 4: Build metadata
        self._build_metadata()

    def _clone_repo(self, name, url):
        """Clone a single EarlyPrint repo with submodules."""
        repo_dir = os.path.join(self.path_repos, name)
        if os.path.exists(repo_dir):
            print(f'  {name}: already cloned at {repo_dir}')
            return

        print(f'  {name}: cloning {url}...')
        os.makedirs(self.path_repos, exist_ok=True)
        ret = os.system(f'git clone {url} {repo_dir}')
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
        print(f'  {name}: updating submodules (this may take a while)...')
        os.system(f'cd {repo_dir} && git submodule update')
        print(f'  {name}: done')

    def _symlink_xmls(self, repos=None):
        """Create symlinks from xml/{repo}/{prefix}/{ID}.xml to repo texts."""
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

            repo_xml_dir = os.path.join(xml_dir, repo_name)
            os.makedirs(repo_xml_dir, exist_ok=True)
            count = 0

            for root, dirs, files in os.walk(repo_texts):
                for fn in files:
                    if not fn.endswith('.xml'):
                        continue
                    src = os.path.join(root, fn)
                    # Preserve the prefix subdirectory: texts/A00/A00001.xml -> xml/eebotcp/A00/A00001.xml
                    rel = os.path.relpath(src, repo_texts)
                    dst = os.path.join(repo_xml_dir, rel)
                    os.makedirs(os.path.dirname(dst), exist_ok=True)
                    if not os.path.exists(dst):
                        os.symlink(os.path.abspath(src), dst)
                    count += 1

            print(f'  {repo_name}: {count} XML files linked')
            total += count

        print(f'  Total: {total} XML files in {xml_dir}')

    def _build_metadata(self):
        """Build metadata.csv by parsing XML headers."""
        print('  Building metadata from XML headers...')
        xml_dir = self.path_xml
        objs = []

        for root, dirs, files in os.walk(xml_dir):
            for fn in files:
                if fn.endswith('.xml'):
                    objs.append(os.path.join(root, fn))

        print(f'  Parsing {len(objs)} XML files...')
        ld = pmap(_parse_earlyprint_meta, objs, num_proc=DEFAULT_NUM_PROC,
                  desc='[EarlyPrint] Parsing metadata')

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
        meta = super().load_metadata(*x, **y)
        if not len(meta):
            return meta

        # Determine source collection from TCP ID prefix
        if meta.index.name == 'id' or 'id' in meta.columns:
            ids = meta.index if meta.index.name == 'id' else meta['id']
            meta['tcp_source'] = ids.map(
                lambda x: TCP_PREFIX_TO_SOURCE.get(str(x)[0], 'unknown')
            )

        # Genre from medium detection
        # (TCP texts use estimate_genre based on tag counts, already in parent)

        return meta


def _parse_earlyprint_meta(fnfn):
    """Parse metadata from an EarlyPrint XML file."""
    try:
        with open(fnfn, encoding='utf-8', errors='ignore') as f:
            txt = f.read()

        meta = extract_metadata(txt)
        # ID from filename (strip .xml)
        meta['id'] = os.path.splitext(os.path.basename(fnfn))[0]
        # Track which repo it came from
        parts = fnfn.split(os.sep)
        for repo_name in EARLYPRINT_REPOS:
            if repo_name in parts:
                meta['ep_repo'] = repo_name
                break
        return meta
    except Exception as e:
        return None
