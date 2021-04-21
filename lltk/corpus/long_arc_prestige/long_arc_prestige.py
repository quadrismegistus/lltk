from lltk.imports import *
from lltk.corpus.hathi import htid2id

GIT_REPO = 'https://github.com/tedunderwood/horizon.git'
REPO_NAME='horizon'
REPO_META_DIR='horizon/chapter3/metadata'
URL_DATA='https://www.ideals.illinois.edu/bitstream/handle/2142/99556/data_for_chapter3.tar.gz?sequence=2&isAllowed=y'
DATA_FN='data_for_chapter3.tar.gz'
META_COLS=['id',
 'author',
 'title',
 'year',
 'docid',
 'actualdate',
 'earliestdate',
 'firstpub',
 'tags',
 'recordid',
 'OCLC',
 'imprint',
 'enumcron',
 'pubrev',
 'judge',
 'impaud',
 'yrrev',
 'pubname',
 'birthdate',
 'gender',
 'nationality',
 'othername',
 'notes',
 'canon',
 'path',
 'authsvols',
 'publisher',
 'decade']


class TextLongArcPrestige(Text):
    def text_plain(self,*x,**y):
        # By default will load from file
        txt=super().text_plain(*x,**y)
        # Any last minute fixes here?
        # txt = ...
        return txt

def xml2txt(xml_txt_or_fn,*x,**y):
    return default_xml2txt(xml_txt_or_fn,*x,**y)

def tsv2json(inp):
    ifnfn,ofnfn=inp
    freqd={}
    with open(ifnfn,encoding='utf-8',errors='ignore') as f:
        for i,ln in enumerate(f):
            if not i: continue
            try:
                word,count=ln.strip().split('\t')
                freqd[word]=float(count)
            except ValueError:
                pass
    if not os.path.exists(os.path.dirname(ofnfn)): os.makedirs(os.path.dirname(ofnfn))
    with open(ofnfn,'w') as of: json.dump(freqd,of)


class LongArcPrestige(Corpus):
    XML2TXT = xml2txt
    TEXT_CLASS=TextLongArcPrestige
    COL_ID = 'id'
    url_raw=URL_DATA

    def compile(self, num_proc=DEFAULT_NUM_PROC, force=False):
        """
        This is the installation function.
        """

        # First clone repo
        if not os.path.exists(self.path_raw): os.makedirs(self.path_raw)
        path_repo=os.path.join(self.path_raw,REPO_NAME)
        if not os.path.exists(path_repo) or force:
            gitcmd=f'cd {self.path_raw} && git clone {GIT_REPO} && cd {os.getcwd()}'
            self.log(f'''Cloning repo to {path_repo}''')
            os.system(gitcmd)
        else:
            gitcmd=f'cd {self.path_raw} && git pull && cd {os.getcwd()}'
            self.log(f'Pulling updates for repository at {path_repo}')
            os.system(gitcmd)
            

        # get metadata from repo
        dfs=[]
        meta_dir=os.path.join(self.path_raw,REPO_META_DIR)
        for i,fn in enumerate(os.listdir(meta_dir)):
            if not fn.endswith('.csv'): continue
            if fn.startswith('.'): continue
            fnfn=os.path.join(meta_dir,fn)
            _df=pd.read_csv(fnfn)
            _df=_df[[x for x in META_COLS if x in set(_df.columns)]]
            _df['subcorpus']=fn.split('.')[0]
            self.log(f'Found in {os.path.join(REPO_META_DIR,fn)}: of shape {_df.shape}')
            dfs.append(_df)
            

        # save metadata    
        df=pd.concat(dfs).fillna('')
        for col in df:
            df[col]=df[col].apply(lambda x: x if x!=' ' else '')
        df['reviewed']=df.pubrev.apply(lambda x: 'Reviewed' if x=='rev' else 'Unreviewed')
        df['id']=df.docid#.apply(htid2id)
        df['year']=df['earliestdate']
        df['genre']=df.subcorpus.apply(lambda x: 'Fiction' if 'fic' in x else 'Poetry') 
        self.log(f'Compiled metadata table with shape {df.shape}')
        self.log(f'Saving metadata to {self.path_metadata}')
        save_df(fix_meta(df), self.path_metadata, index=False,verbose=False)

        # download texts
        self.path_raw_tar=os.path.join(self.path_raw, DATA_FN)
        self.log(f'Downloading word count files to {self.path_raw_tar}')
        download(self.url_raw, self.path_raw_tar)

        # extract to raw folder
        self.path_raw_freqs=os.path.join(self.path_raw,'freqs')
        if not os.path.exists(self.path_raw_freqs): os.makedirs(self.path_raw_freqs)
        self.log(f'Extracting word count files to {self.path_raw_freqs}')
        extract(self.path_raw_tar, self.path_raw_freqs)

        # convert to json
        self.log(f'Converting frequency files to json and saving to {self.path_freqs}')
        if not os.path.exists(self.path_freqs): os.makedirs(self.path_freqs)
        
        objs=[]
        for fn in os.listdir(self.path_raw_freqs):
            if not fn.endswith('.tsv'): continue
            if fn.startswith('.'): continue

            # build freqd
            ifnfn=os.path.join(self.path_raw_freqs,fn)
            ofnfn=os.path.join(self.path_freqs, os.path.splitext(fn)[0] + '.json')
            objs+=[(ifnfn,fnfn)]
        pmap(tsv2json, objs, desc='Converting TSVs to JSONs', num_proc=num_proc)
        

        self.log('Finished installing')
        return

    def preprocess_txt(self,*args,**attrs):
        """
        Custom function here to produce txt files at `path_txt`
        from xml files at `path_xml`. By default this will map
        TextLongArcPrestige.xml2txt(xml_fn) over files at `path_xml`.
        """
        # By default will
        return super().preprocess_txt(*args,**attrs)

    def load_metadata(self,*args,**attrs):
        """
        Magic attribute loading metadata, and doing any last minute customizing
        """
        # This will save to `self.path_metadata`, set in the manifest path_metadata
        meta=super().load_metadata(*args,**attrs)
        # ?
        return meta

