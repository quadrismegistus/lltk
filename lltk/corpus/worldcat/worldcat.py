from lltk.imports import *




class TextWorldcat(BaseText):

    def meta_is_valid(self,meta={}): return {'num_editions','edition_oclcs'} & (set(meta.keys())|set(self._meta.keys()))

    def query(self,force=False,progress=True,allow_alternate=False,**kwargs):
        odx=self.qdb.get(self.id)
        if force or not odx:
            if log: log(f'? {self.id}')
            odx={}

            def get_oclcs(urlx):
                res=self.qdb.get(urlx) if not force else None
                if log: log(f'db res: {res}')
                if not res:
                    res=(0,[])
                    
                    html = gethtml(urlx)
                    if html:
                        import bs4
                        dom = bs4.BeautifulSoup(html,'lxml')

                        div = dom.select_one('#fial-numresults')
                        if div:
                            num_results = div.find('td').text.split()[-1]
                            if num_results.isdigit():
                                num_results=int(num_results)
                                oclcs=[
                                    a.attrs.get('href').split('/oclc/')[1].split('?')[0]
                                    for a in dom('a')
                                    if '/oclc/' in a.attrs.get('href','')
                                    and 'ht=edition' in a.attrs.get('href','')
                                ]
                                res=(num_results,oclcs)
                                if log: log(f'web res: {res}')
                
                    self.qdb.set(urlx,res)
                return res

            def get_editions(): 
                url_editions=self.corpus.get_query_url_oclc_editions(self.id)
                oclcs = []
                num_results, _oclcs = get_oclcs(url_editions)
                for oclc in _oclcs:
                    if oclc not in set(oclcs): oclcs.append(oclc)
                
                if allow_alternate:
                    iterr=list(range(11,num_results+1,10))
                    if iterr:
                        if progress: iterr=get_tqdm(iterr,desc='Querying for alternate editions')
                        for start in iterr:
                            urlx=self.corpus.get_query_url_oclc_editions(self.id, start=start)
                            _num_results, _oclcs = get_oclcs(urlx)
                            if log: log(f'_oclcs = {_oclcs}')
                            for oclc in _oclcs:
                                if oclc not in set(oclcs): oclcs.append(oclc)
                    
                return (num_results, [x for x in oclcs if x!=self.id])
            
            num_editions, edition_oclcs = get_editions()
            if log: log(f'NUM: {len(edition_oclcs)}, {edition_oclcs}')

            odx['num_editions']=num_editions
            odx['num_editions_found']=len(edition_oclcs)
            odx['edition_oclcs']=edition_oclcs
            odx['_sources']=[self.corpus.get_addr(x) for x in edition_oclcs]
            self.qdb.set(self.id,odx)
        return odx







class Worldcat(BaseCorpus):
    NAME='Worldcat'
    ID='worldcat'
    TEXT_CLASS=TextWorldcat
    REMOTE_SOURCES=[]

    def get_query_url_oclc(self,oclc):
        return f'https://www.worldcat.org/oclc/{oclc}'
    def get_query_url_oclc_editions(self,oclc,start=1,format='book',lang='eng'):
        return f'https://www.worldcat.org/oclc/{oclc}/editions?sd=asc&start_edition={start}&referer=null&se=yr&qt=sort_yr_asc&editionsView=true&fq=x0%3A{format}+>+ln%3A{lang}&qt=advanced'
    
    def get_query_url_author_title(self,author,title,format='book',lang='eng'):
        return f'https://www.worldcat.org/search?q=ti%3A+{quote_plus(title)}+au%3A{quote_plus(author)}&fq=x0%3A{format}+>+ln%3A{lang}&qt=advanced'


    def texts_from(self,text,format='book',lang='eng',remote=REMOTE_REMOTE_DEFAULT,add_source=True,**kwargs):
        # id already stored?
        #if log: log(f'<- remote = {remote} ?')
        # tobjs = [src for src in text.sources if src.corpus==self]
        # if tobjs:
        #     yield from tobjs
        # else:
        res = text.get('oclc')
        if res:
            if log: log(f'found oclc in metadata: {res}')
            oclcs = [res]
        else:
            oclcs=[]
            ti,au=text.shorttitle,text.au
            if log:log(f'querying for oclc by title ({ti}) and author ({au})')
            if ti and au:
                url = self.get_query_url_author_title(au,ti,format=format,lang=lang)
                oclcs = self.qdb.get(url)
                if not oclcs:
                    html = gethtml(url)
                    if html:
                        import bs4
                        dom = bs4.BeautifulSoup(html,'lxml')
                        oclcs=[x for x in [div.text.strip().split()[0] for div in dom.select('.oclc_number')] if x and x.isdigit()]
                        if log: log(f'found oclcs = {oclcs}')
                        self.qdb.set(url,oclcs)
            
        for oclc in oclcs:
            t=self.text(id=oclc).init_(remote=remote,**kwargs)
            t.add_source(text)
            yield t