from lltk.imports import *

HATHI_FULL_META_NUMLINES = 17430652
HATHI_FULL_META_PATH=os.path.join(PATH_CORPUS,'hathi','metadata.csv.gz')
HATHI_FULL_META_URL = 'https://www.hathitrust.org/filebrowser/download/297178'


def htid2id(x):
    a,b=x.split('.',1)
    return a+'/'+b

class TextHathi(Text): pass

class Hathi(Corpus):
    TEXT_CLASS=TextHathi
    id='hathi'
    name='Hathi'
    LANGS=['eng']
    name=[]
    METADATA_HEADER='htid	access	rights	ht_bib_key	description	source	source_bib_num	oclc_num	isbn	issn	lccn	title	imprint	rights_reason_code	rights_timestamp	us_gov_doc_flag	rights_date_used	pub_place	lang	bib_fmt	collection_code	content_provider_code	responsible_entity_code	digitization_agent_code	access_profile_code	author'.split('\t')

    @property
    def path_full_metadata(self): return HATHI_FULL_META_PATH
    @property
    def url_full_metadata(self): return HATHI_FULL_META_URL

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
        if 'imprint' in df.columns: df['year']=df['imprint'].apply(get_date)
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
        # df['year']=df['imprint'].apply(get_date)
        # df['period']=df['year']//1*1
        # ids=list(set(df.groupby('period').sample(n=10,replace=True).id))
        # random.shuffle(ids)

        # compile!
        tools.pmap(compile_text, df.id, num_proc=6)


    def download(self,**attrs):
        """
        This function is used to download the corpus. Leave as-is to use built-in LLTK download system.
        Provide a

        So far, downloadable data types (for certain corpora) are:
            a) `txt` files
            b) `xml` files
            c) `metadata` files
            d) `freqs` files

        If you have another zip folder of txt files you'd like to download,
        you can specify with `url_txt` (i.e. url_`type`, where type is in `quotes` in (a)-(d) above):
            corpus.download(url_txt="https://www.etcetera.com/etc.zip")
        """
        return super().download(**attrs)

    def preprocess(self,parts=['metadata','txt','freqs','mfw','dtm'],force=False,**attrs):
        """
        This function is used to boot the corpus, taking it from its raw (just downloaded) to refined condition:
            - metadata: Save metadata (if necessary)
            - txt: Save plain text versions (if necessary)
            - freqs: Save json frequency files per text
            - mfw: Save a long list of all words sorted by frequency
            - dtm: Save a document-term matrix
        """
        return super().install(parts=parts,force=force,**attrs)

def get_date(imprint):
    for x in tools.ngram(str(imprint),4):
        x=''.join(x)
        try:
            return int(x)
        except Exception:
            return x

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

    except (HTTPError,FileNotFoundError,KeyError) as e:
        # print('!!',e)
        pass


