from lltk.imports import *

HATHI_FULL_META_NUMLINES = 17430652
HATHI_FULL_META_PATH=os.path.join(PATH_CORPUS,'hathi','metadata.csv.gz')
# HATHI_FULL_META_URL = 'https://www.hathitrust.org/filebrowser/download/297178'
HATHI_FULL_META_URL = 'https://www.hathitrust.org/filebrowser/download/344452'

# HATHI_TITLE_QUERY_URL='https://catalog.hathitrust.org/Search/Home?adv=1&lookfor%5B%5D=[[[QSTR_TITLE]]]&type%5B%5D=title&lookfor%5B%5D=[[[QSTR_AUTHOR]]]&type%5B%5D=author&bool%5B%5D=AND&yop=after&page=1&pagesize=100&sort=yearup'
HATHI_TITLE_QUERY_URL='https://catalog.hathitrust.org/Search/Home?adv=1&lookfor%5B%5D=[[[QSTR_AUTHOR]]]&type%5B%5D=author&bool%5B%5D=AND&lookfor%5B%5D=[[[QSTR_TITLE]]]&type%5B%5D=title&bool%5B%5D=AND&yop=after&page=1&pagesize=100'#&sort=&sort=yearup'

def htid2id(x):
    a,b=x.split('.',1)
    return a+'/'+b

class TextHathi(BaseText):
    def metadata(self,**kwargs):
        d=super().metadata(self,**kwargs)
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
        return bool((meta if meta else self._meta).get('recordURL'))

    
    def query(self,force=False,**kwargs):
        if log: log(f'? {self.id}')

        from hathitrust_api import BibAPI
        bib = BibAPI()
 
        bibd=bib.get_single_record_json('recordnumber',self.htrn)
        recdd=bibd.get('records',{})
        if not recdd: return {}

        recd=recdd.get(self.htrn,{})
        log(f'? {pf(recd)}')
        recd['htids']=[
            d.get('htid')
            for d in bibd.get('items',[])
            if d.get('fromRecord')==self.htrn
        ]
        recd['_sources']=['_hathi/htid/'+x for x in recd['htids']]
        recd['num_vols']=len(recd['_sources'])
        recd = hathi_clean_query_meta(recd)
        if log: log(f'-> {pf(recd)}')
        return recd






class TextHathiVolume(TextHathi):
    @property
    def htid(self): return self.id.split('/',1)[-1]

    def id_is_valid(self):
        return self.id and self.htid and '.' in self.htid
    
    def meta_is_valid(self,meta={}):
        return bool((meta if meta else self._meta).get('recordURL'))

    def query(self,force=False,**kwargs):
        log(f'? {self.id}')
        odx={}
        try:
            from htrc_features import Volume
            vol = Volume(self.htid)
            odx={
                k:v
                for k,v in vol.__dict__.items()
                if k and k[0]!='_' and type(v) in {int,float,str,list,dict,set,tuple} and v
            }
            odx['htid']=odx.get('id','')
            if odx['htid']: 
                odx=self.ensure_id_addr(odx)
        except Exception as e:
            log.error(e)
        if log: log(f'-> {pf(odx)}')
        return odx
        







class Hathi(BaseCorpus):
    TEXT_CLASS=TextHathi
    id='hathi'
    name='Hathi'
    LANGS=['eng']
    name=[]
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
        return df

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
        if type(text)==str and text.split('/')[0] in {'htrn','htid'}: return text

        ids = self.query_for_ids(text)
        o=f'htrn/{ids[0]}' if ids else ERROR
        if log>0: log(f'-> {o}')
        return o

    def query_db(self,fn='query_for_ids'):
        return DB(os.path.join(self.path,'data',fn))

    # Corpus, Hathi
    def query_for_ids(self,text):
        import bs4
        from urllib.parse import quote_plus
        log(f'<- {text}')
        if not text.shorttitle or not text.au: return []

        url = HATHI_TITLE_QUERY_URL
        url = url.replace('[[[QSTR_TITLE]]]', quote_plus(text.shorttitle))
        url = url.replace('[[[QSTR_AUTHOR]]]', quote_plus(text.au))
        log(f'<- {url}')

        res = self.query_db().get(url)
        if res: 
            if log: log(f'found result on db ({len(res)} records)')
            return res

        html = gethtml(url)
        dom = bs4.BeautifulSoup(html,'lxml')

        o = [
            a.attrs['href'].split('/Record/')[-1].split('?')[0]
            for a in dom('a')
            if '/Record/' in a.attrs['href']
        ]
        if o:
            if log: log(f'found result on website ({len(o)} records)')
            self.query_db().set(url,o)
        log(f'-> {o}')
        return o

    def text_from(self,text,**kwargs):
        for newtext in self.texts_from(text,**kwargs):
            newtext.add_source(text)
            return newtext

    def texts_from(self,text,remote=True,**kwargs):
        if not is_text_obj(text) or text.corpus == self: return text
        for htrn in self.query_for_ids(text):
            htrnid = f'htrn/{htrn}'
            t=self.text(htrnid).init(remote=remote,**kwargs)
            yield t












def get_date(imprint):
    for x in tools.ngram(str(imprint),4):
        x=''.join(x)
        if x.isdigit(): return int(x)
    return np.nan

class HathiSubcorpus(Hathi):
    def load_metadata(self,*x,**y):
        meta=super().load_metadata()
        meta['genre']=self.genre
        return meta


class HathiSermons(HathiSubcorpus):
    id='hathi_sermons'
    name='HathiSermons'
    searchwords={'sermon','sermons'}
    genre='Sermon'


class HathiProclamations(HathiSubcorpus):
    id='hathi_proclamations'
    name='HathiProclamations'
    searchwords={'proclamation','proclamation'}
    genre='Proclamation'


class HathiEssays(HathiSubcorpus):
    id='hathi_essays'
    name='HathiEssays'
    searchwords={'essay','essays'}
    genre='Essay'


class HathiLetters(HathiSubcorpus):
    id='hathi_letters'
    name='HathiLetters'
    searchwords={'letter','letters'}
    genre='Letters'

class HathiTreatises(HathiSubcorpus):
    id='hathi_treatises'
    name='HathiTreatises'
    searchwords={'treatise','treatises'}
    genre='Treatise'


class HathiTales(HathiSubcorpus):
    id='hathi_tales'
    name='HathiTales'
    searchwords={'tale','tales'}
    genre='Tale'

class HathiNovels(HathiSubcorpus):
    id='hathi_novels'
    name='HathiNovels'
    searchwords={'novel','novels'}
    genre='Novel'

class HathiStories(HathiSubcorpus):
    id='hathi_stories'
    name='HathiStories'
    searchwords={'story','stories'}
    genre='Story'

class HathiAlmanacs(HathiSubcorpus):
    id='hathi_almanacs'
    name='HathiAlmanacs'
    searchwords={'almanac','almanack','almanach'}
    genre='Almanac'

class HathiRomances(HathiSubcorpus):
    id='hathi_romances'
    name='HathiRomances'
    searchwords={'romance','romances'}
    genre='Romance'




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
    except Exception as e:
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





