#encoding=utf-8
from lltk.imports import *


def estimate_genre(text_xml):
    speaker_tag=False
    l_tag=False
    p_tag=False
    txt=text_xml.upper()
    tags = ['</SPEAKER>', '</L>', '</P>']
    tag_counts={}
    for tag in tags: tag_counts[tag]=txt.count(tag)
    drama = tag_counts['</SPEAKER>'] > 100
    verse = tag_counts['</L>'] > tag_counts['</P>']
    prose = tag_counts['</L>'] <= tag_counts['</P>']
    medium = 'Verse' if verse else 'Prose'
    genre = 'Drama' if drama else medium
    return genre,medium

def extract_metadata(mtxt):
    md={}
    ## IDs
    for idnox in mtxt.split('</IDNO>')[1:-1]:
        idno_type=idnox.split('TYPE="')[1].split('">')[0]
        idno_id=idnox.split('">')[-1]

        md['id_'+idno_type.replace(' ','_')]=idno_id

    biblio = tools.yank(mtxt,'BIBLFULL')
    if biblio:
        md['title'] = biblio.split('</TITLE>')[0].split('">')[-1]
        md['author'] = tools.yank(biblio,'AUTHOR')
        md['extent'] = tools.yank(biblio,'EXTENT')
        md['pubplace'] = tools.yank(biblio,'PUBPLACE')
        md['publisher'] = tools.yank(biblio,'PUBLISHER')
        md['date'] = tools.yank(biblio,'DATE')
        md['notes']=tools.yank(biblio,'NOTESSTMT')

    try:
        md['year']=int(''.join([x for x in md.get('date','') if x.isdigit()][:4]))
    except (ValueError,TypeError) as e:
        md['year']=0
    return md


def xml2txt_tcp(xmlfn,OK=['p','l'], BAD=[], body_tag='text', force_xml=False, text_only_within_medium=False):
    import bs4
    # get dom
    with open(xmlfn,encoding='utf-8',errors='ignore') as f: xml=f.read()
    dom = bs4.BeautifulSoup(xml,'lxml')
    ## remove bad tags
    for tag in BAD: [x.extract() for x in dom.findAll(tag)]
    # find tags in body
    txt=[]
    for doc in dom.find_all(body_tag):
        for tag in doc.find_all():
            if tag.name in OK:
                txt+=[clean_text(tag.text)]
    return '\n\n'.join(txt).replace(u'∣','')




class TextTCP(Text):
    @property
    def sections_xml(self,divider_tag='DIV1'):
        return super(TextTCP,self).sections_xml(divider_tag=divider_tag,text_section_class=self.corpus.TEXT_SECTION_CLASS)

    




class TCP(Corpus):
    XML2TXT=xml2txt_tcp

    

    def compile_extract(self, extract_in=['XML','headers'],walks=2):
        # download if nec
        self.compile_download()
        # loop
        for n in range(walks):
            for root,dirs,fns in tqdm(sorted(os.walk(self.path_raw)),desc='Unzipping raw data archives'):
                if os.path.basename(root) in set(extract_in):
                    # only get xml for now
                    for fn in fns:
                        tools.extract(os.path.join(root,fn), root, progress=False)

    def compile_texts(self,fn_startswith='',exts={'xml'},replacements={}):
        for root,dirs,fns in tqdm(sorted(os.walk(self.path_raw)),desc='Moving and renaming texts'):
            for fn in fns:
                if fn_startswith and not fn.startswith(fn_startswith): continue
                ext=fn.split('.')[-1]
                if not ext in exts: continue            
                ofn=fn
                for rk,rv in replacements.items(): ofn=ofn.replace(rk,rv)
                opath=os.path.join(self.path_root, ext)
                if not os.path.exists(opath): os.makedirs(opath)
                ofnfn=os.path.join(opath,ofn)
                ifnfn=os.path.join(root,fn)
                os.rename(ifnfn,ofnfn)
                # print(ifnfn,'\n',ofnfn,'\n')


    def compile_metadata(self, path_meta=None, exts={'xml','xml.gz'}):
        if path_meta is None: path_meta=self.path_xml
        objs = [os.path.join(path_meta,fn) for fn in os.listdir(path_meta) if fn.split('.')[-1] in exts]
        ld = pmap(_do_compile_meta, objs, num_proc=DEFAULT_NUM_PROC)
        df = pd.DataFrame(ld)
        df = fix_meta(df)
        tools.save_df(df, self.path_metadata)
        print(f'Saved metadata: {tools.ppath(self.path_metadata)}')
        return df

def _do_compile_meta(fnfn):
    with open(fnfn) as f: meta=extract_metadata(f.read())
    meta['id']=os.path.splitext(os.path.basename(fnfn))[0]
    return meta