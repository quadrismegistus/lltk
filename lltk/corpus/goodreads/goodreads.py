from lltk.imports import *




class TextGoodreads(BaseText):
    def meta_is_valid(self,meta={}): return 'page_url' in merge_dict(meta,self._meta)
    # def id_is_valid(self): return '-' in str(self.id)
    
    def query(self,force=False,force_inner=False,**kwargs):
        odx = self.qdb.get(self.id)
        if force or odx is None:
            url = f'https://www.goodreads.com/book/show/{self.id}'
            if log: log(f'? {url}')
            html = self.qdb.query(url)
            
            import bs4
            if log: log('parsing html!')
            dom=bs4.BeautifulSoup(html,'lxml')
            key='itemprop'
            odx=OrderedSetDict()
            for tag in dom.find_all(attrs={key:True}):
                keyname = tag.get(key)
                keyval1 = tag.get('content')
                keyval2 = clean_text(tag.text.strip())
                keyval = keyval1 if keyval1 else keyval2
                if keyname and keyval: odx[keyname]=keyval
            
            # hidden
            for tag in dom.find_all('input', attrs={'type':'hidden'}):
                keyname = tag.get('name')
                keyval = tag.get('value')
                if keyname and keyval: odx[keyname]=keyval

            # clean
            odx2=odx.to_dict()
            odx2['title'] = odx['name'][0] if odx['name'] else ''
            odx2['author']=odx['author'][0] if odx['author'] else ''
            odx2['aggregateRating'] = odx['aggregateRating'][0].strip().split()[0] if odx['aggregateRating'] else ''
            odx2['reviewers']=odx['url'][1:] if len(odx['url'])>1 else ''
            odx2['page_url']=odx['page_url'][0] if odx['page_url'] else ']'
            odx2={
                k:(v[0] if type(v)==list and len(v)==1 else v)
                for k,v in odx2.items() 
                if k not in self.corpus.BAD_KEYS
            }
            odx2['id'] = odx2.get('page_url','').split('/')[-1]
            odx2=odx2 if odx2['id'] and odx2['id']==self.id else {}
            self.qdb.set(self.id,odx2)
            if log: log('done parsing html')
            if log: log(f'-> {odx2.get("id","?")}')
            odx=odx2
        return odx








class Goodreads(BaseCorpus):
    NAME='Goodreads'
    ID='isbn'
    TEXT_CLASS=TextGoodreads
    BAD_KEYS = {'authenticity_token','utf8','url','reviews'}
    REMOTE_SOURCES=[]

    def texts_from_title(self,text):
        url=f'https://www.goodreads.com/search?utf8=%E2%9C%93&q={quote_plus(text.qstr)}&search_type=books'
        html = self.qdb.query(url)
        # <tr itemscope="" itemtype="http://schema.org/Book">
        import bs4
        if log: log('parsing html!')

        O=[]
        sepp='<tr itemscope itemtype="http://schema.org/Book">'
        for htmlsec in html.split(sepp):
            div=bs4.BeautifulSoup(htmlsec,'lxml')
            url = div.select_one('a', attrs={'class':'bookTitle'})
            if not url: continue
            href = url.attrs.get('href')
            dvid = href.split('?')[0].split('/')[-1]
            title = div.select_one('span',attrs={'itemprop':'name'}).text
            authordiv = div.select_one('.authorName')
            if not authordiv: continue
            author=authordiv.select_one('span',attrs={'itemprop':'name'}).text
            author_id = div.select_one('.authorName').attrs.get('href').split('/author/show/')[-1].split('?')[0]
            ratingsdiv = div.select_one('.minirating')
            if not ratingsdiv: continue
            ratingstr=ratingsdiv.text
            rsep = ' avg rating — '
            if rsep in ratingstr:
                rstr1,rstr2 = ratingstr.split(rsep,1)
                avgrating=pd.to_numeric(rstr1.strip().split()[-1],errors='coerce')
                numrating=pd.to_numeric(rstr2.strip().split()[0].replace(",",""),errors='coerce')
                if safebool(avgrating) and safebool(numrating):
                    for yrdiv in div.select('.greyText'):
                        yrtxt=yrdiv.text
                        ysep='published'
                        if ysep in yrtxt:
                            yr=yrtxt.split(ysep,1)[-1].strip().split()[0]
                            yrnum=pd.to_numeric(yr,errors='coerce')

                            if safebool(yrnum):
                                odx=dict(
                                    id=dvid,
                                    author_id=author_id,
                                    author=author,
                                    title=title,
                                    year=yr,
                                    ratingValue=avgrating,
                                    ratingCount=numrating,
                                )
                                if log: log(odx)
                                # t=self.text(_cache=False, **odx)
                                O.append(odx)
        O.sort(key=lambda t: -float(t['ratingCount']))
        return O

    def texts_from_isbn(self,isbn,force=False,**kwargs):
        odx = self.qdb.get(isbn)
        if not force and odx is not None: return odx
        url = f'https://www.goodreads.com/search?q={isbn}'
        return self.query_meta(url)

    #     return odx2

    def texts_from(self,text,force=False,remote=REMOTE_REMOTE_DEFAULT,cache=True,**kwargs):
        texts_ld = self.texts_from_title(text)
        if log: log(f'got matches = {texts_ld}')
        for d in texts_ld:
            t = self.text(**d)
            t.init(remote=REMOTE_REMOTE_DEFAULT)
            t.add_source(text)
            yield t
            break

    # def texts_from(self,text,force=False,remote=REMOTE_REMOTE_DEFAULT,cache=True,**kwargs):
    #     #if log: log(f'<- remote = {remote} ?')
    #     for i,isbn in enumerate(text.isbn_l):
    #         if log: log(f'{i} {isbn}')
    #         isbn=str(isbn).replace('-','').strip().split()[0]
    #         isbn_meta = self.query_meta(isbn,force=force,**kwargs)
    #         if isbn_meta and isbn_meta.get('id') and '-' in isbn_meta.get('id'):
    #             if log: log(f'got goodreads metadata (id={isbn_meta.get("id")}): {len(isbn_meta)} keys')
    #             t=self.text(**isbn_meta).init_(remote=remote,cache=cache,**kwargs)
    #             t.add_source(text)
    #             yield t


