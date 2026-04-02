from lltk.imports import *
import re as _re

HATHI_FULL_META_NUMLINES = 17430652
HATHI_FULL_META_PATH=os.path.join(PATH_CORPUS,'hathi','metadata.csv.gz')
# HATHI_FULL_META_URL = 'https://www.hathitrust.org/filebrowser/download/297178'
HATHI_FULL_META_URL = 'https://www.hathitrust.org/filebrowser/download/344452'

# HATHI_TITLE_QUERY_URL='https://catalog.hathitrust.org/Search/Home?adv=1&lookfor%5B%5D=[[[QSTR_TITLE]]]&type%5B%5D=title&lookfor%5B%5D=[[[QSTR_AUTHOR]]]&type%5B%5D=author&bool%5B%5D=AND&yop=after&page=1&pagesize=100&sort=yearup'
HATHI_TITLE_QUERY_URL='https://catalog.hathitrust.org/Search/Home?adv=1&lookfor%5B%5D=[[[QSTR_AUTHOR]]]&type%5B%5D=author&bool%5B%5D=AND&lookfor%5B%5D=[[[QSTR_TITLE]]]&type%5B%5D=title&bool%5B%5D=AND&yop=after&page=1&pagesize=100'#&sort=&sort=yearup'

def htid2id(x):
    a,b=x.split('.',1)
    return a+'/'+b


def hathi_id_normalize(raw_id):
    """Normalize a HathiTrust ID to canonical flat form: {library}/{volume_id}

    Collapses 3-char directory splits and normalizes ark identifiers.

    Handles all known variants:
        mdp.39015009144422       → mdp/39015009144422       (dot separator)
        mdp/390/15009144422      → mdp/39015009144422       (3-char dir split)
        bc/ark/+=13960=t0bv7v96f → bc/ark+=13960=t0bv7v96f  (3-char split ark)
        aeu/ark:/13960/t0000ds1j → aeu/ark+=13960=t0000ds1j (colon-slash ark)
        uc2/ark+=13960=t0js9xc1p → uc2/ark+=13960=t0js9xc1p (already canonical)
        hvd/hxjh5g               → hvd/hxjh5g               (short alphanumeric)

    Idempotent: canonical IDs pass through unchanged.
    """
    s = str(raw_id).strip()

    # Handle dot separator (htid format): mdp.39015... → mdp/39015...
    m = _re.match(r'^([a-z][a-z0-9]*)\.(.*)', s, _re.IGNORECASE)
    if m:
        s = m.group(1) + '/' + m.group(2)

    parts = s.split('/')
    if len(parts) < 2:
        return s

    library = parts[0].lower()
    rest = '/'.join(parts[1:])

    # Normalize ark IDs to ark+=NNNNN=XXXXX
    ark_match = _re.match(r'ark[:/]*\+?=?(\d+)[=/](.*)', rest)
    if ark_match:
        rest = f'ark+={"=".join(ark_match.groups())}'
    elif '/' in rest:
        # Collapse 3-char directory splits for non-ark IDs
        subparts = rest.split('/')
        if all(_re.match(r'^[\w$+]+$', p) for p in subparts):
            rest = ''.join(subparts)

    return f'{library}/{rest}'


# Cache for freqs index: maps canonical_id → actual filepath
_FREQS_INDEX_CACHE = {}

def _build_freqs_index(freqs_dir):
    """Walk a freqs directory and build a mapping: normalized_id → filepath."""
    if freqs_dir in _FREQS_INDEX_CACHE:
        return _FREQS_INDEX_CACHE[freqs_dir]

    index = {}
    if not os.path.exists(freqs_dir):
        _FREQS_INDEX_CACHE[freqs_dir] = index
        return index

    for root, dirs, files in os.walk(freqs_dir):
        for f in files:
            if not f.endswith('.json'):
                continue
            path = os.path.join(root, f)
            raw_id = os.path.relpath(path, freqs_dir).removesuffix('.json')
            norm_id = hathi_id_normalize(raw_id)
            index[norm_id] = path

    _FREQS_INDEX_CACHE[freqs_dir] = index
    if log: log(f'Built freqs index for {freqs_dir}: {len(index)} files')
    return index

class TextHathi(BaseText):
    @property
    def path_freqs(self):
        """Resolve freqs path through the corpus's freqs index."""
        path = self.corpus.freqs_path_for(self.id)
        if path:
            return path
        # Fallback to default path construction
        return super().get_path('freqs')

    def metadata(self,**kwargs):
        d=super().metadata(**kwargs)
        au=d.get('contributor')
        if au:
            if type(au)==list: au=au[0]
            d['author']=au
        yr=d.get('publishDate')
        if yr: d['year']=pd.to_numeric(yr,errors='coerce')
        return d



class TextHathiRecord(TextHathi):
    

    @property
    def htrn(self): return self.id.split('/')[-1]


    def id_is_valid(self):
        return self.id and self.htrn and self.htrn.isdigit()
    
    def meta_is_valid(self,meta={}):
        return bool((meta if meta else self._meta).get('url'))
    
    # def get_remote_sources(self, *args, remote=REMOTE_REMOTE_DEFAULT, lim=1, **kwargs):
    #     #if log: log(f'<- remote = {remote} ?')
    #     o=[]
    #     for htid in self.htid_l:
    #         t=self.corpus.text(f'htid/{htid}',_source=self).init_(remote=remote,**kwargs)
    #         if log: log(f'-> {t}')
    #         o.append(t)
    #         if lim and len(o)>=lim: break
    #     return o
    
    # def have_remote_sources(self,*x,**y):
    #     print([self.sources])
    #     print([self.id])
    #     return any('htid' in src.id for src in self.sources if src and src.id)

    def init(self,*x,remote=None,**y):
        remote=is_logged_on()
        #if log: log(f'<- remote = {remote} ?')
        super().init(*x,remote=remote,**y)
        htid_srcs=set(t.htid for t in self.dsources if t.corpus.id.startswith('htid/'))
        for htid in set(self.htid_l) - htid_srcs:
            if log: log(htid)
            t=self.corpus.text(f'htid/{htid}').init(remote=remote,**y)
            t.add_source(self)
            if log: log(t)
        return self


    def query(self,force=False,force_inner=False,sep='  - ',**kwargs):
        odx=self.qdb.get(self.id)
        if force or not odx:
            if log: log(self)
            import rispy
            mapd=rispy.TAG_KEY_MAPPING
            mapd['SN']='isbn'
            mapd['TP']='type'
            url=f'https://catalog.hathitrust.org/Search/SearchExport?handpicked={self.htrn}&method=ris'
            txt=self.qdb.query(url).strip()
            if not txt: return {}
            odx=OrderedSetDict()
            for ln in txt.strip().split('\n'):
                try:
                    if sep in ln:
                        k,v = ln.strip().split(sep,1)
                        k2=mapd.get(k.strip())
                        if k2:
                            odx[k2] = v
                except AssertionError as e:
                    log.error(e)
            
            for url in odx.get('url',[]):
                if 'hdl.handle.net' in url:
                    htid='/'.join(url.split('/')[4:])
                    htid=htid.strip().split()[0].strip()
                    odx['htid']=htid
            
            odx['isbn']=[x.strip().split()[0].replace('-','') for x in odx['isbn']]
            odx=odx.to_dict()
            self.qdb.set(self.id,odx)
        return odx


class TextHathiVolume(TextHathi):
    @property
    def htid(self): return self.id.split('/',1)[-1]

    def id_is_valid(self):
        return self.id and self.htid and '.' in self.htid
    
    def meta_is_valid(self,meta={}):
        return bool((meta if meta else self._meta).get('recordURL'))

    def query(self,force=False,**kwargs):
        odx = self.qdb.get(self.id) if not force else None


        if not odx or not just_meta_no_id(odx):
            if log: log(self)
            odx={}
            try:
                with Capturing() as __:
                    from htrc_features import Volume
                    vol = Volume(self.htid)
                    odx={
                        k:v
                        for k,v in vol.__dict__.items()
                        if k and k[0]!='_' and type(v) in {int,float,str,list,dict,set,tuple} and v
                    }
                odx['htid']=odx.get('id','')
                if odx['htid']: odx=self.ensure_id_addr(odx)
                self.qdb.set(self.id, odx)
            except Exception as e:
                log.error(f'query failed: {e}')

        if log: log(f'-> {odx}')
        return odx
        







class Hathi(BaseCorpus):
    TEXT_CLASS=TextHathi
    id='hathi'
    name='Hathi'
    LANGS=['eng']
    name=[]
    REMOTE_SOURCES=[]
    METADATA_HEADER='htid	access	rights	ht_bib_key	description	source	source_bib_num	oclc_num	isbn	issn	lccn	title	imprint	rights_reason_code	rights_timestamp	us_gov_doc_flag	rights_date_used	pub_place	lang	bib_fmt	collection_code	content_provider_code	responsible_entity_code	digitization_agent_code	access_profile_code	author'.split('\t')

    @property
    def path_full_metadata(self): return HATHI_FULL_META_PATH

    def stream_full_meta(self):
        self.download_full_metadata()
        yield from readgen_csv(self.path_full_metadata, num_lines=HATHI_FULL_META_NUMLINES, desc='Scanning through giant Hathi Trust CSV file')

    def download_full_metadata(self):
        if not os.path.exists(self.path_full_metadata):
            if not os.path.exists(os.path.dirname(self.path_full_metadata)):
                os.makedirs(os.path.dirname(self.path_full_metadata))
            tools.download(self.url_full_metadata, self.path_full_metadata)

    def load_metadata(self,*x,**y):
        df=super().load_metadata()
        if 'rights_date_used' in set(df.columns):
            df['year']=df['rights_date_used']
        elif 'imprint' in set(df.columns):
            df['year']=df['imprint'].apply(get_date)
        else:
            df['year']=np.nan
        # Normalize IDs to canonical flat form
        df.index = df.index.map(hathi_id_normalize)
        df.index.name = 'id'
        return df

    @property
    def freqs_index(self):
        """Mapping of canonical text ID → actual freqs filepath on disk."""
        return _build_freqs_index(self.path_freqs)

    def freqs_path_for(self, text_id):
        """Resolve a canonical text ID to its actual freqs filepath."""
        norm_id = hathi_id_normalize(text_id)
        path = self.freqs_index.get(norm_id)
        if path:
            return path
        # Fallback: try direct path (for corpora where filenames already match)
        direct = os.path.join(self.path_freqs, norm_id + '.json')
        if os.path.exists(direct):
            return direct
        return None

    def compile(self,**attrs):
        """
        This is a custom installation function. By default, it will simply try to download itself,
        unless a custom function is written here which either installs or provides installation instructions.
        """
        from tqdm import tqdm

        if not os.path.exists(self.path_root): os.makedirs(self.path_root)
        if not os.path.exists(self.path_home): os.makedirs(self.path_home)


        if not os.path.exists(self.path_metadata):
            self.download_full_metadata()
            print('>> finding metadata matching search terms:',self.name)
            old=[]
            for dx in self.stream_full_meta():
                title=dx.get('title')
                if not title: continue
                title=title.strip().lower()
                titlewords=set(re.findall(r"[\w']+|[.,!?;]", title))
                match = bool(self.searchwords & titlewords)
                if not match: continue
                old+=[dx]

            # fix!
            df=pd.DataFrame(old)
            df=df[df.lang.isin(self.LANGS)]
            df['id']=df['htid'].apply(htid2id)
            df.to_csv(self.path_metadata)

        # get ids
        print('>> loading metadata')
        df=pd.read_csv(self.path_metadata,error_bad_lines=False)
        # df['period']=df['year']//1*1
        # ids=list(set(df.groupby('period').sample(n=10,replace=True).id))
        # random.shuffle(ids)

        # compile!
        tools.pmap(compile_text, df.id, num_proc=1)






    def init_text(self,*args,**kwargs):
        t=super().init_text(*args,**kwargs)
        if t.id.startswith('htrn/'):
            t.__class__ = TextHathiRecord
        elif t.id.startswith('htid/'):
            t.__class__ = TextHathiVolume
        return t

    def get_text_id(self,text,**kwargs):
        if log>0: log(f'<- {get_imsg(text,**kwargs)}')
        if is_addr_str(text): text=to_corpus_and_id(text)[1]
        if type(text)==str and text.split('/')[0] in {'htrn','htid'}: return text

        ids = self.query_for_ids(text)
        o=f'htrn/{ids[0]}' if ids else ERROR
        if log>0: log(f'-> {o}')
        return o

    def query_db(self,fn='query_for_ids'):
        return DB(os.path.join(self.path,'data',fn), engine='sqlite')

    # Corpus, Hathi
    def query_for_ids(self,text):
        import bs4
        if log: log(f'<- {text}')
        text=Text(text)
        if not text.shorttitle or not text.au: return []

        url = HATHI_TITLE_QUERY_URL
        url = url.replace('[[[QSTR_TITLE]]]', quote_plus(text.shorttitle))
        url = url.replace('[[[QSTR_AUTHOR]]]', quote_plus(text.au))
        if log: log(f'<- {url}')

        html = self.qdb.query(url)
        dom = bs4.BeautifulSoup(html,'lxml')

        o = [
            a.attrs['href'].split('/Record/')[-1].split('?')[0]
            for a in dom('a')
            if '/Record/' in a.attrs['href']
        ]
        if o:
            if log: log(f'found result on website ({len(o)} records)')
            self.query_db().set(url,o)
        if log: log(f'-> {o}')
        return o

    def text_from(self,text,**kwargs):
        for newtext in self.texts_from(text,**kwargs):
            return newtext

    def texts_from(self,text,remote=REMOTE_REMOTE_DEFAULT,**kwargs):
        for htrn in self.query_for_ids(text):
            htrnid = f'htrn/{htrn}'
            t=self.text(htrnid).init_(remote=remote,**kwargs)
            t.add_source(text)
            yield t
        #if log: log(f'<- remote = {remote} ?')
        # tobjs = [src for src in text.sources if src.corpus==self and src.id.startswith('htrn/')]
        # if tobjs:
        #     yield from tobjs
        # else:
        #     for htrn in self.query_for_ids(text):
        #         htrnid = f'htrn/{htrn}'
        #         t=self.text(htrnid,_source=text).init_(remote=remote,**kwargs)
        #         yield t












def get_date(imprint):
    for x in tools.ngram(str(imprint),4):
        x=''.join(x)
        if x.isdigit(): return int(x)
    return np.nan

class HathiSubcorpus(Hathi):
    # Subclasses set genre_raw (fine-grained) and genre (harmonized GENRE_VOCAB)
    genre_raw = None
    genre = None

    def load_metadata(self,*x,**y):
        meta=super().load_metadata()
        meta['genre_raw']=self.genre_raw
        meta['genre']=self.genre
        return meta


class HathiSermons(HathiSubcorpus):
    id='hathi_sermons'
    name='HathiSermons'
    searchwords={'sermon','sermons'}
    genre_raw='Sermon'
    genre='Sermon'


class HathiProclamations(HathiSubcorpus):
    id='hathi_proclamations'
    name='HathiProclamations'
    searchwords={'proclamation','proclamation'}
    genre_raw='Proclamation'
    genre='Legal'


class HathiEssays(HathiSubcorpus):
    id='hathi_essays'
    name='HathiEssays'
    searchwords={'essay','essays'}
    genre_raw='Essay'
    genre='Essay'


class HathiLetters(HathiSubcorpus):
    id='hathi_letters'
    name='HathiLetters'
    searchwords={'letter','letters'}
    genre_raw='Letters'
    genre='Letters'

class HathiTreatises(HathiSubcorpus):
    id='hathi_treatises'
    name='HathiTreatises'
    searchwords={'treatise','treatises'}
    genre_raw='Treatise'
    genre='Treatise'


class HathiTales(HathiSubcorpus):
    id='hathi_tales'
    name='HathiTales'
    searchwords={'tale','tales'}
    genre_raw='Tale'
    genre='Fiction'

class HathiNovels(HathiSubcorpus):
    id='hathi_novels'
    name='HathiNovels'
    searchwords={'novel','novels'}
    genre_raw='Novel'
    genre='Fiction'

class HathiStories(HathiSubcorpus):
    id='hathi_stories'
    name='HathiStories'
    searchwords={'story','stories'}
    genre_raw='Story'
    genre='Fiction'

class HathiAlmanacs(HathiSubcorpus):
    id='hathi_almanacs'
    name='HathiAlmanacs'
    searchwords={'almanac','almanack','almanach'}
    genre_raw='Almanac'
    genre='Almanac'

class HathiRomances(HathiSubcorpus):
    id='hathi_romances'
    name='HathiRomances'
    searchwords={'romance','romances'}
    genre_raw='Romance'
    genre='Fiction'




def compile_text(idx,by_page=False):
    from htrc_features import Volume
    from urllib.error import HTTPError

    try:
        path_freqs = os.path.join(load_corpus('Hathi').path_freqs,idx+'.json')
        path_freqs_dir = os.path.dirname(path_freqs)
        if os.path.exists(path_freqs): return
        if not os.path.exists(path_freqs_dir):
            try:
                os.makedirs(path_freqs_dir)
            except FileExistsError:
                pass

        # print('compiling!')
        htid=idx.replace('/','.',1)
        # print('Getting: ',htid)
        vol=Volume(htid)
        vol_freqs=vol.term_volume_freqs(pos=False,case=True)
        vol_freqs_d=dict(zip(vol_freqs['token'],vol_freqs['count']))
        with open(path_freqs,'w') as of:
            json.dump(vol_freqs_d, of)

    # except (HTTPError,FileNotFoundError,KeyError) as e:
    except AssertionError as e:
        # print('!!',e)
        pass









def hathi_clean_query_meta(rdd):
    singd = {
        k[:-1]:v[0]
        for k,v in rdd.items()
        if type(v)==list and v and k.endswith('s') and k[0]!='_'
    }
    plurd = {
        k:v
        for k,v in rdd.items()
        if k[:-1] not in singd or len(v)!=1
    }
    return {
        k:v
        for k,v in sorted(merge_dict(singd, plurd).items())
    }





