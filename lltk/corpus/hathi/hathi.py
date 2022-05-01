from lltk.imports import *

HATHI_FULL_META_NUMLINES = 17430652
HATHI_FULL_META_PATH=os.path.join(PATH_CORPUS,'hathi','metadata.csv.gz')
# HATHI_FULL_META_URL = 'https://www.hathitrust.org/filebrowser/download/297178'
HATHI_FULL_META_URL = 'https://www.hathitrust.org/filebrowser/download/344452'

# HATHI_TITLE_QUERY_URL='https://catalog.hathitrust.org/Search/Home?adv=1&lookfor%5B%5D=[[[QSTR_TITLE]]]&type%5B%5D=title&lookfor%5B%5D=[[[QSTR_AUTHOR]]]&type%5B%5D=author&bool%5B%5D=AND&yop=after&page=1&pagesize=100&sort=yearup'
HATHI_TITLE_QUERY_URL='https://catalog.hathitrust.org/Search/Home?adv=1&lookfor%5B%5D=[[[QSTR_AUTHOR]]]&type%5B%5D=author&bool%5B%5D=AND&lookfor%5B%5D=[[[QSTR_TITLE]]]&type%5B%5D=title&bool%5B%5D=AND&yop=after&page=1&pagesize=100&sort=&sort=yearup'

def htid2id(x):
    a,b=x.split('.',1)
    return a+'/'+b

class TextHathi(BaseText): pass

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

    def text(self,id: Union[str,BaseText,None], **kwargs):
        if is_text_obj(id) and id.corpus != self:
            return self.text_from(id,**kwargs)
        return super().text(id,**kwargs)


    def query_db(self,fn='query_for_ids'):
        return DB(os.path.join(self.path,'data',fn))

    # Corpus, Hathi
    def query_for_ids(self,text):
        import bs4
        from urllib.parse import quote_plus

        url = HATHI_TITLE_QUERY_URL
        url = url.replace('[[[QSTR_TITLE]]]', quote_plus(text.shorttitle))
        url = url.replace('[[[QSTR_AUTHOR]]]', quote_plus(text.au))

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
        return o

    def from_record_ids(self,record_ids):
        from hathitrust_api import BibAPI
        bib = BibAPI()
        db=self.query_db('query_record_jsons')
        for recnum in record_ids:
            idres = db.get(recnum)
            if idres is not None:
                yield idres
            else:
                bibd=bib.get_single_record_json('recordnumber',id)
                recd=bibd.get('records',{}).get(recnum,{})
                volld=bibd.get('items',{})
                volld=[d for d in volld if d.get('fromRecord')==id]
                yield (recd,volld)



    def process_rec_json(self,recnum,recnumd):
        log(pf(recnumd))
        rdd=recnumd.get('records',{}).get(recnum,{})
        vld=[d for d in recnumd.get('items',[]) if d.get('fromRecord')==recnum]


        if rdd:
            singd = {
                k[:-1]:v[0]
                for k,v in rdd.items()
                if type(v)==list and v and k.endswith('s')
            }
            plurd = {
                k:v
                for k,v in rdd.items()
                if k[:-1] not in singd or len(v)!=1
            }
            odx={
                'id':f'htrn/{recnum}',
                **{
                    k:v
                    for k,v in sorted(merge_dict(singd, plurd).items())
                    if k!='id'
                }
            }
            odx['vols'] = vld
            return odx
        return {}

        
    def text_from(self,text,**kwargs):
        x=None
        for x in self.texts_from(text,**kwargs): break
        return x

    def texts_from(self,text,add_source=True,**kwargs):
        if not is_text_obj(text) or text.corpus == self: return text
        ids = self.query_for_ids(text)
        for htrn,htrn_d in self.query_record_jsons(ids):
            for recd in self.process_rec_json(htrn,htrn_d):
                if add_source: recd['_source'] = text.addr
                t=self.text(**recd)

                # vol ld
                for vold in volld in recd.get('vols',[]):
                    htid = vold.get('htid')
                    if htid:
                        vold['id']=f'htid/{htid}'
                        log(vold)
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















