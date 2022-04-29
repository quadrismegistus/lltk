
from lltk.imports import *
from lltk.model.characters import CharacterSystem


class ModelBookNLP(CharacterSystem):
    CHARACTER_SYSTEM_ID='booknlp'

    def __init__(self,
            text=None,
            prefer_sections=True,
            language=BOOKNLP_DEFAULT_LANGUAGE,
            pipeline=BOOKNLP_DEFAULT_PIPELINE,
            model=BOOKNLP_DEFAULT_MODEL):

        self.text=text
        self.prefer_sections=prefer_sections
        self.language=language
        self.pipeline=pipeline
        self.model=model
        
        self._chardata_all=None
        self._chartokd=None
        self._df_tokens=None
        self._df_chars=None
        self._interactions=None

    def __repr__(self): return f'[{self.__class__.__name__}]({self.text.addr})'

    def init(self,**kwargs):
        self.parse(**kwargs)

    @property
    def path(self):
        return get_booknlp_text_path(
            self.text.path_txt,
            language=self.language,
            model=self.model,
        )
    
    @property
    def paths(self):
        return dict(
            tokens=os.path.join(self.path,'text.tokens'),
            entities=os.path.join(self.path,'text.entities'),
            supersense=os.path.join(self.path,'text.supersense'),
            quotes=os.path.join(self.path,'text.quotes'),
            chardata=os.path.join(self.path,'text.book'),
            data_tokens=os.path.join(self.path,'data.tokens.pkl'),
            data_chars=os.path.join(self.path,'data.chars.pkl'),
        )

    def iter_models(self,section_type=None,progress=True):
        if not self.prefer_sections or self.text is None or issubclass(self.text.__class__, TextSection) or self.text.sections(section_type) is None:
            yield self
        else:
            oiterr=self.text.sections(section_type).texts()
            for tx in get_tqdm(oiterr):
                yield ModelBookNLP(tx)
    
    def models(self,**kwargs):
        return list(self.iter_models(**kwargs))
    
    
    def get_paths_to_parse(self,prefer_sections=True):
        if not prefer_sections or self.text.sections() is None: return [self.text.path_txt]
        return [t_sec.path_txt for t_sec in self.text.sections().texts()]

    def get_char_tok(self,char_id):
        char_id=f'C{int(char_id):02}' if type(char_id) in {int,float} else char_id
        if self._chartokd is None:
            chardat=self.chardata(allow_empty=True)
            if len(chardat):
                self._chartokd=dict(zip(chardat.char_id, chardat.char_tok))
            else:
                self._chartokd={}
        return self._chartokd.get(char_id)

    def get_char_toks(self,char_id):
        char_id=f'C{int(char_id):02}' if type(char_id) in {int,float} else char_id
        if self._chartokds is None:
            chardat=self.chardata(allow_empty=True)
            if len(chardat):
                self._chartokds=dict(zip(chardat.char_id, chardat.char_toks))
            else:
                self._chartokds={}
        return self._chartokds.get(char_id,[])
        
    
    def parse(self,progress=True,**kwargs):
        models=self.models()
        paths_to_parse = [model.text.path_txt for model in self.models()]
        pmap(
            parse_booknlp,
            paths_to_parse,
            num_proc=1,
            progress=False if len(paths_to_parse)<2 else progress,
            kwargs=kwargs,
            desc='Parsing texts via BookNLP'
        )
        
    def init_chardata(self,char_tok_only_capitalized=True,**kwargs):
        ## init
        o=[]
        for model in self.models():
            if not os.path.exists(model.path_chardata): model.init()
            if not os.path.exists(model.path_chardata): continue

            with open(model.path_chardata) as f: chardata=json.load(f)

            for chardat in chardata['characters']:
                if not chardat['id']: continue
                odx={}
                odx['text_id']=model.text.id
                odx['char_id']=f"C{chardat['id']:02}"
                odx['char_i']=chardat['id']
                # pprint(chardat)
                # stop
                ctoks=[(xd['n'],xd['c']) for xd in chardat['mentions']['proper']]
                #isprop=odx['is_proper']=bool(ctoks)
                if not ctoks:
                    ctok=''
                else:
                    ctok=ctoks[0][0]
                    if char_tok_only_capitalized:
                        ctok=' '.join(xw for xw in ctok.split() if xw and xw[0]==xw[0].upper())
                # if not allow_empty and not ctok: continue
                    
                    
                # set names
                odx['char_tok']=ctok
                odx['char_toks']=ctoks
                
                for mentiontype in ['proper','common','pronoun']:
                    odx[f'char_tok_{mentiontype}']=[
                        (x['n'],x['c']) for x in chardat['mentions'][mentiontype]
                    ]
                
                # set words
                for wdtype in ['agent','patient','poss']:
                    odx[f'token_i_{wdtype}'] = [
                        (x['i'],x['w']) for x in chardat[wdtype]
                    ]
                
                try:
                    odx['char_pronouns']=chardat['g']['argmax']
                    odx['char_pronouns_inference']=chardat['g']['inference']
                except (KeyError,TypeError) as e:
                    odx['char_pronouns']=''
                    odx['char_pronouns_inference']={}
                
                odx['count']=chardat['count'] if chardat['count'] else 0
                # pprint(odx)
                o.append(odx) 
        return pd.DataFrame(o)

    def chardata(self,allow_empty=False,force=False,progress=True,**kwargs):
        if self._df_chardata is None:
            o=[]
            for model in self.iter_models(progress=progress):
                if not force and model._df_chars is not None: 
                    mdf=model._df_chars
                elif not force and os.path.exists(model.path_data_chars):
                    mdf=read_df(model.path_data_chars)
                else:
                    mdf=model._df_chars=model.init_chardata(**kwargs)
                    save_df(mdf,model.path_data_chars)
                if mdf is not None: o.append(mdf)
            self._df_chardata=pd.concat(o).fillna('') if o else pd.DataFrame()
        odf=self._df_chardata
        if not allow_empty and 'char_tok' in set(odf.columns):
            odf=odf[odf.char_tok!=""]
        return odf

    @property
    def text_root(self): return self.text.text_root
            
    ### TOKEN LEVEL DATA
            
    def supersenses(self):
        o=[]
        for model in self.models():
            ipath=model.path_supersense
            if not ipath: continue
            if not os.path.exists(ipath): model.init()
            if not os.path.exists(ipath): continue
            
            idf=read_df(ipath,fmt='tsv')
            idf['text_id']=model.text.id
            o.append(idf)
        return pd.concat(o).fillna('') if o else pd.DataFrame()


    def quotes(self):
        o=[]
        for model in self.models():
            ipath=model.path_quotes
            if not ipath: continue
            if not os.path.exists(ipath): model.init()
            if not os.path.exists(ipath): continue
            
            idf=read_df(ipath,fmt='tsv')
            idf['text_id']=model.text.id
            o.append(idf)
        odf=pd.concat(o).fillna('') if o else pd.DataFrame()
        if 'char_id' in set(odf.columns):
            odf['char_i']=odf['char_id']
            odf['char_id']=odf['char_i'].apply(lambda x: f'C{x:02}')
        return odf


    def entities(self,only_propn=True):
        o=[]
        for model in self.models():
            ipath=model.path_entities
            if not ipath: continue
            if not os.path.exists(ipath): model.init()
            if not os.path.exists(ipath): continue
            mdf=read_df(ipath,fmt='tsv')
            mdf['text_id']=model.text.id
            o.append(mdf)
        odf=pd.concat(o).fillna('') if o else pd.DataFrame()
        if 'COREF' in set(odf.columns):
            odf['char_i']=odf['COREF']
            odf['char_id']=odf['char_i'].apply(lambda x: f'C{x:02}')
        
        return odf if not only_propn else odf[odf.prop=="PROP"]
        

    ### Getting data
        # booknlp
    def get_character_id(self, char_tok_or_id, force=False):
        char_toks=self.get_char_toks(char_tok_or_id)
        for ctok,cnum in char_toks:
            res=super().get_character_id(ctok, force=force)
            if res: return res
        return super().get_character_id(char_tok_or_id, force=force)
        

    def tokens(self,force=False,progress=True,only_propn=True,return_df=False,**kwargs):
        if not force and self._df_tokens is not None:
            odf=self._df_tokens
        elif not force and os.path.exists(self.path_data_tokens):
            odf=read_df(self.path_data_tokens)
        else:
            o=[]
            iterr=models=self.models()
            if progress and len(models)>1: iterr=get_tqdm(models,desc='Iterating models')
            for model in iterr:
                if not force and model._df_tokens is not None: 
                    mdf=model._df_tokens
                elif not force and os.path.exists(model.path_data_tokens):
                    mdf=read_df(model.path_data_tokens)
                else:
                    mdf=model._df_tokens=model.init_tokens(**kwargs)
                    save_df(mdf,model.path_data_tokens)   
                if mdf is not None: o.append(mdf)
            odf=self._df_tokens=pd.concat(o).fillna('') if o else pd.DataFrame()
            save_df(odf, self.path_data_tokens)
        return odf

    def init_tokens(self,allow_empty_chars=False,progress=True,**kwargs):
        o=[]
        iterr=self.models()
        if progress and len(iterr)>1: iterr=get_tqdm(iterr,desc='Iterating models')
        for model in iterr:
            if not os.path.exists(model.path_tokens): model.init()
            if not os.path.exists(model.path_tokens): continue

            mdf=pd.read_csv(model.path_tokens,sep='\t')
            mdf.columns=[BOOKNLP_RENAME_COLS.get(colx,colx) for colx in mdf.columns]
            mdf['text_id']=model.text.id
            i2d=defaultdict(dict)
            
            # other in chardata
            chardata=self.chardata(allow_empty=allow_empty_chars)
            ok_char_ids=set(chardata.char_id) if len(chardata) and 'char_id' in set(chardata.columns) else {}
            
            # supersenses
            ssdf=self.supersenses()
            if len(ssdf):
                for start,end,supersense_category in zip(ssdf.start_token, ssdf.end_token, ssdf.supersense_category):
                    for xi in range(start,end+1):
                        i2d[xi]['supersense']=str(supersense_category)

            # entities
            entdf = self.entities()
            if len(entdf):
                for start,end,char_id in zip(entdf.start_token, entdf.end_token, entdf.char_id):
                    if char_id not in ok_char_ids: continue
                    for xi in range(start,end+1):
                        i2d[xi]['char_id_mentioned']=char_id
            
            # spoken by
            qdf = self.quotes()
            if len(qdf):
                for start,end,char_id in zip(qdf.quote_start, qdf.quote_end, qdf.char_id):
                    if char_id not in ok_char_ids: continue
                    for xi in range(start,end+1):
                        i2d[xi]['char_id_speaking']=char_id
                for start,end,char_id in zip(qdf.mention_start, qdf.mention_end, qdf.char_id):
                    if char_id not in ok_char_ids: continue
                    for xi in range(start,end+1):
                        i2d[xi]['char_id_speaking_mentioned']=char_id
                    
            # other
            if len(chardata) and 'char_id' in set(chardata.columns):
                cpref='token_i_'
                ccols=[x for x in chardata.columns if x.startswith(cpref)]
                for ccol in ccols:
                    ccol_name='char_id_'+ccol[len(cpref):]
                    for char_id,crow in zip(chardata.char_id, chardata[ccol]):
                        if char_id not in ok_char_ids: continue
                        for crow_i,crow_w in crow:
                            i2d[crow_i][ccol_name]=char_id

            # any char appearance at all
            for xi,xid in i2d.items():
                chars_in_i=[xid[xk] for xk in xid.keys() if xk.startswith('char_id_')]
                chars_in_i=sorted(list(set(chars_in_i)))
                i2d[xi]['char_id']='; '.join(chars_in_i)
                i2d[xi]['char_tok']='; '.join(self.get_char_tok(charx) for charx in chars_in_i)

            mdf_ext = pd.DataFrame(
                list(mdf.token_i.apply(lambda x: i2d.get(x,{}))),
                index=mdf.token_i
            ).fillna('')
            mdf=mdf.set_index('token_i').join(mdf_ext,how='left').reset_index().fillna('')
            mdf=mdf.set_index(['text_id','token_i'])
            o.append(mdf)
        return pd.concat(o).fillna('') if o else pd.DataFrame()


        
    # booknlp
    

    ### CHARACTERS
    def get_character_token_counts(self,allow_empty_chars=False,**kwargs):
        chardf=self.chardata(allow_empty=allow_empty_chars,**kwargs)
        od=Counter()
        # for ctok,ccount in zip(chardf['char_tok'], chardf['count']):
            # od[ctok]+=ccount

        for ctoks in chardf.char_toks:
            for ctok,ccount in ctoks:
                od[ctok]+=ccount
        return od

    # def characters(self):
    #     from lltk.model.characters import CharacterSystem
    #     self._character_system=CharacterSystem(
    #         self.text,
    #         id='booknlp',
    #         _char_tokd=self.get_character_token_counts,
    #         )
    #     return self._character_system


    # booknlp

    def iter_interactions(
            self,
            narrator_id=BOOKNLP_NARRATOR_ID,
            mentioned_near_window=100,
            col_mentioned='char_id_mentioned',
            col_speaking='char_id_speaking',
            col_pref='char_id',
            progress=True,
            **kwargs):

        def format_d(_d):
            return dict(
                (_k,_v)
                for _k,_v in _d.items()
                if not _k.startswith(col_pref)
                and (_k.endswith('_id') or _k.endswith('_i')    )
            )

        for model in self.iter_models(progress=progress, **kwargs):
            dftokens = model.tokens().reset_index()
            ldtokens = dftokens.to_dict(orient='records')
            mentioned_last=None
            mentioned_last_i=None
            for rowd in ldtokens:
                mentioned=model.get_character_id(rowd.get(col_mentioned))
                if mentioned and mentioned != mentioned_last:
                    speaker=rowd.get(col_speaking)
                    mentioner=model.get_character_id(speaker) if speaker else narrator_id
                    yield dict(
                        source=mentioner,
                        rel='mentioned',
                        target=mentioned,
                        t=(rowd.get('text_id'), rowd.get('token_i')),
                        mention_diff=0,
                        **format_d(rowd)
                    )
                    mentioned_i=rowd.get('token_i')
                    if mentioned_last and mentioned_last_i is not None:
                        if mentioned_i-mentioned_last_i <= mentioned_near_window:
                            yield dict(
                            source=mentioned,
                            rel='mentioned_after',
                            target=mentioned_last,
                            mention_diff=mentioned_i-mentioned_last_i,
                            **format_d(rowd)
                        )
                    mentioned_last=mentioned
                    mentioned_last_i=mentioned_i

    


def get_booknlp(
        language=BOOKNLP_DEFAULT_LANGUAGE,
        pipeline=BOOKNLP_DEFAULT_PIPELINE,
        model=BOOKNLP_DEFAULT_MODEL, 
        cache=True,
        quiet=True,
        **kwargs):
    if quiet: 
        with Capturing() as o:
            return get_booknlp(
                language=language,
                pipeline=pipeline,
                model=model,
                cache=cache,
                quiet=False,
                **kwargs
            )
    
    global BOOKNLPD
    booknlpd=BOOKNLPD if cache else {}
    key=(language,model,pipeline)
    if not key in booknlpd:
        try:
            from booknlp.booknlp import BookNLP
            booknlpd[key]=BookNLP(
                language=language,
                model_params=dict(pipeline=pipeline,model=model)
            )
        except ModuleNotFoundError:
            log.error('BookNLP module not find')
            booknlpd[key]=None
    return booknlpd[key]




def parse_booknlp(
        path_txt,
        booknlp=None,
        language=BOOKNLP_DEFAULT_LANGUAGE,
        pipeline=BOOKNLP_DEFAULT_PIPELINE,
        model=BOOKNLP_DEFAULT_MODEL, 

        cache=True,
        quiet=True,
        force=False,
        
        **kwargs):
    ## quiet?
    if quiet:
        with Capturing() as output:
            return parse_booknlp(
                path_txt,
                booknlp=booknlp,
                language=language,
                pipeline=pipeline,
                model=model,
                cache=cache,
                quiet=False,
                force=force,
                **kwargs)
    
    
    ## path to parse even exist?
    if not os.path.exists(path_txt):
        if log.verbose>0: log(f'File does not exist: {path_txt}')
        return
    
    # get path
    
    odir = get_booknlp_text_path(path_txt,language=language,model=model)
    ensure_dir_exists(odir)
    num_files=len([x for x in os.listdir(odir) if x.startswith('text.')])
    if force or num_files<6:
        if booknlp is None:
            booknlp=get_booknlp(
                language=language,
                pipeline=pipeline,
                model=model,
                cache=cache,
                quiet=False,
                **kwargs
            )

        okwargs=dict(
            inputFile=path_txt,
            outputFolder=odir,
            idd='text'
        )
        # log(f'Parsing BookNLP with args: {okwargs}')
        try:
            return booknlp.process(**okwargs)
        except AttributeError:
            return


def get_booknlp_text_path(
        path_txt,
        language=BOOKNLP_DEFAULT_LANGUAGE,
        model=BOOKNLP_DEFAULT_MODEL,
        **kwargs):
    pathkey=['booknlp',f'{language}_{model}']
    odir=os.path.join(os.path.abspath(os.path.dirname(path_txt)),*pathkey)
    return odir






# # booknlp
# NARRATOR_ID='NARRATOR'

# def interactions(self,col_pref='char_id_',narrator_id=NARRATOR_ID,mentioned_near_window=100,**kwargs):
#     for model in self.models():
#         dftokens = model.tokens().reset_index()
#         dftokens['_t']=[f'{a}:{b:08}' for a,b in zip(dftokens.text_id, dftokens.token_i)]
#         dftokens=dftokens.reset_index().set_index('_t')
#         for text_id,text_df in dftokens.groupby('text_id'):
#             mentioned_last=None
#             mentioned_last_i=None
#             in_text=[]
#             for para_i,para_df in sorted(text_df.groupby('para_i')):
#                 in_para=[]
#                 for sent_i,sent_df in sorted(para_df.groupby('sent_i')):
#                     in_sent=[]
#                     for t,row in sent_df.iterrows():
#                         # mentioned
#                         mentioned=model.get_character_id(row.get('char_id_mentioned'))
#                         if mentioned:
#                             # in_sent.append(mentioned)
#                             in_para.append((mentioned,row.token_i))
#                             in_text.append((mentioned,row.token_i))
#                             mentioner=model.get_character_id(row.get('char_id_speaking')) if row.get('char_id_speaking') else narrator_id
#                             yield (mentioner,'mentions',mentioned,t)
                
#                     ## mentioned same para?
#                     near_edges=set()
#                     in_para.sort()
#                     for wtok1,wi1 in in_para:
#                         for wtok2,wi2 in in_para:
#                             if wtok1<wtok2:
#                                 near_edge=(wtok1,wtok2)
#                                 if near_edge not in near_edges:
#                                     near_edges|={near_edge}
#                                     yield (wtok1,'mentioned_within_same_para',wtok2,wi1)
#                                     yield (wtok2,'mentioned_within_same_para',wtok1,wi2)
                
#                 ## mentioned near?
#                 near_edges=set()
#                 in_text.sort()
#                 for wtok1,wi1 in in_text:
#                     for wtok2,wi2 in in_text:
#                         if wtok1>=wtok2: continue
#                         if abs(wi2-wi1)<=mentioned_near_window:
#                             near_edge=(wtok1,wtok2)
#                             if near_edge not in near_edges:
#                                 near_edges|={near_edge}
#                                 yield (wtok1,f'mentioned_within_{mentioned_near_window}_words',wtok2,wi1)
#                                 yield (wtok2,f'mentioned_within_{mentioned_near_window}_words',wtok1,wi2)


            


#                             # if mentioned_last and mentioned_last!=mentioned:
#                                 # yield (mentioned,'mentioned_after',mentioned_last,t)
#                             # mentioned_last=mentioned
#                     # done with sent
#                     # in_para+=in_sent
#                 # done with para
#                 # in_para=sorted(list(set(in_para)))
#                 # print(in_para)
#                 # if in_para and len(para_df):
#                 #     for ipx1 in in_para:
#                 #         for ipx2 in in_para:
#                 #             if ipx1!=ipx2:
#                 #                 otx=para_df.index[0]
#                 #                 yield (ipx1,'mentioned_near',ipx2,otx)




    # def df_renamed_by_character_id(self,mdf,model=None,pref_old='char_id_',pref_new='character_id_'):
    #     if model is None: model=self
    #     cols_to_change=[c for c in mdf.columns if c.startswith(pref_old)]
    #     cols_old=mdf.columns
    #     cols_new=[
    #         c if not c.startswith(pref_old) else pref_new+c[len(pref_old):]
    #         for c in cols_old
    #     ]
            
    #     for col in cols_to_change:
    #         vals_old=mdf[col]
    #         vals_new=vals_old.apply(model.get_character_id)
    #         mdf[col]=vals_new
        
    #     mdf.columns = cols_new
    #     if 'char_id' in set(mdf.columns):
    #         mdf['character_id']=[
    #             '; '.join(
    #                 model.get_character_id(cid)
    #                 for cid in val.split('; ')
    #             )
    #             for val in mdf['char_id']
    #         ]
    #     return mdf[[c for c in mdf.columns if c.startswith(pref_new[:-1])]]
