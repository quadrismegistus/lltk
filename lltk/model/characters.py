from lltk.imports import *
from lltk.model.networks import *
import networkx as nx




class Character:
    def __init__(self,id=None,_system=None,**kwargs):
        self.id=id if id else get_idx()
        self._system=_system
        self._meta=dict(kwargs.items())
    
    @property
    def system(self): return self._system
    

class CharacterSystem(BaseModel):
    EVENT_TYPE='interaction'
    CHARACTER_SYSTEM_ID='default'
    
    def __init__(self,source,id=None,_char_tokd=Counter(),**kwargs):
        self._source=source
        self.id=self.CHARACTER_SYSTEM_ID
        self._events={}
        self._df_feats=None
        self._df_names=None
        self._char_tokd=_char_tokd
        self._d_count=_char_tokd
        self._character_tok2id={}
        self._interactions=None
        self._charsys={
            self.CHARACTER_SYSTEM_ID:self
        }
        # self._dg=dnx.DynGraph(edge_removal=True)

    def add_system(self,character_system,**kwargs):
        if not hasattr(self,'_charsys'): self._charsys={}
        idx = character_system.CHARACTER_SYSTEM_ID
        if not idx in self._charsys:
            self._charsys[idx]=character_system
            setattr(self,zeropunc(idx),character_system)
            if self.CHARACTER_SYSTEM_ID in self._charsys:
                del self._charsys[self.CHARACTER_SYSTEM_ID]

    def iter_systems(self):
        if not hasattr(self,'_charsys') or not self._charsys:
            self._charsys={self.CHARACTER_SYSTEM_ID:self}
        yield from self._charsys.values()
    
    def systems(self,**kwargs):
        return list(self.iter_systems(**kwargs))
    
    @property
    def text_root(self): return self.source.text_root

    @property
    def source(self): return self._source
    @property
    def path_chars(self): return os.path.join(self.text_root.path, DIR_CHARS_NAME)
    @property
    def path_names_dir(self): return os.path.join(self.path_chars,'names_auto')
    @property
    def path_names(self): return os.path.join(self.path_chars,'names.csv')
    @property
    def path_names_anno(self): return get_anno_fn_if_exists(self.text_root.characters().path_names, return_fn_otherwise=False)
    @property
    def path_interactions(self):
        return os.path.join(self.path_chars,'interactions',f'{self.CHARACTER_SYSTEM_ID}.pkl')

    @property
    def path_names_auto(self): return os.path.join(self.path_names_dir,f'{self.CHARACTER_SYSTEM_ID}.json')
    @property
    def path_feats(self): return os.path.join(self.path_chars,'feats.csv')

    ## DF convenient
    @property
    def df_names(self): return self.get_df_names()
    @property
    def df_feats(self): return self.get_df_feats()

    def get_character_id(self,char_toks,force=False,bad=BAD_CHAR_IDS,return_tok=False,sep=';'):
        char_toks=str(char_toks)
        if not self._character_tok2id: self._character_tok2id=self.init_names_anno()
        default = '' if not return_tok else char_toks
        if not char_toks: return default
        ol=[]
        for char_tok in str(char_toks).split(sep):
            char_tok=char_tok.strip()
            o=str(self._character_tok2id.get(char_tok))
            if not o:
                character_tok=clean_character_tok(char_tok)
                o=self._character_tok2id.get(character_tok)
            if o: ol.append(o)
        out=(sep+' ' if sep==sep.strip() else sep).join(ol) if ol else default
        return out if out not in {'nan','None'} else '?'


        
    def names(self,force=False,merge=True,ignore_blank=True,overwrite=True):
        if force or self._df_names is None: self.init_names(force=force)
        odf=self._df_names
        
        if merge:
            odf=odf[odf.character_tok!=""]
            icols=['character_tok']
            qcols=['char_tok_count']
            odf=odf[icols+qcols].groupby(icols).sum().reset_index()
            odf['character_id']=odf.character_tok.fillna('').apply(self.get_character_id)
            # odf=odf[odf.char_tok!=""]
            odf=odf.sort_values(qcols,ascending=False)
            odf=odf[['character_tok','character_id','char_tok_count']]
            odf=odf.rename({'char_tok_count':'character_tok_count'},axis=1)

        if overwrite:
            save_df(odf, self.path_names, index=False)

        return odf if not ignore_blank else odf[odf.character_id!=""]


    def init_names(selff,force=False):
        od_l=[]
        odf_l=[]
        for self in selff.systems():
            if not force and self._df_names is None or self._character_tok2id is None:

                ## auto-generate my names if haven't
                name_counts = self.init_names_auto(force=force)
                
                ## get renamer
                name2id = self.init_names_anno()
                
                odf=pd.DataFrame(
                    dict(
                        character_tok=clean_character_tok(ctok),
                        char_tok=ctok,
                    )
                    for ctok in name_counts
                )
                odf['character_id']=[
                    name2id[a] if a in name2id else (name2id[b] if b in name2id else '?')
                    for a,b in zip(odf.character_tok,odf.char_tok)
                ]
                odf['character_id_auto']=odf['character_tok'].apply(zeropunc)
                odf['char_tok_count']=[name_counts[ctok] for ctok in odf.char_tok]

                odf=odf.sort_values('char_tok_count',ascending=False)
                #save_df(odf,self.path_names,index=False)
                self._df_names=odf
                self._character_tok2id = od = {}
                for col1 in ['character_tok','char_tok']:
                    for k,v in zip(odf[col1], odf.character_id):
                        if v and v not in BAD_CHAR_IDS:
                            od[k]=v
            od_l.extend(self._character_tok2id.items())
            odf_l.append(self._df_names.assign(charsys_id=self.CHARACTER_SYSTEM_ID))
        if selff is not self:
            selff._character_tok2id=dict(od_l)
            selff._df_names=pd.concat(odf_l) if odf_l else pd.DataFrame()
        return selff._df_names

    
    def init_names_anno(self,bad_ids=BAD_CHAR_IDS):
        fn=self.path_names_anno
        od={}
        if fn and os.path.exists(fn):
            odf=read_df(fn)
            cols=set(odf.columns)
            for col1 in ['character_tok','char_tok']:
                if col1 in cols:
                    for k,v in zip(odf[col1], odf.character_id):
                        if v and v not in bad_ids:
                            od[k]=v            
        return od


    def init_names_auto(self,char_toks=None,force=False):
        if force or not os.path.exists(self.path_names_auto):
            if not char_toks:
                char_toks=self.get_character_token_counts()
            ensure_dir_exists(self.path_names_auto)
            with open(self.path_names_auto,'w') as of:
                json.dump(dict(char_toks), of)
        else:
            with open(self.path_names_auto) as f:
                char_toks=Counter(json.load(f))
        return char_toks

    # def init_names_sections_auto(
    #         self,
    #         section_type=None,
    #         char_keys=['sender_tok','recip_tok','char_tok'],
    #         force=False):
    #     ofn=os.path.join(self.path_names_dir,f'{section_type if section_type else "sections"}.json')
    #     if force or not os.path.exists(ofn):
    #         counter=Counter()
    #         secs=self.sections(section_type)
    #         if secs is None: return counter
    #         sdf=secs.meta
    #         if sdf is None or not len(sdf): return counter
    #         for ckey in char_keys:
    #             if ckey in set(sdf.columns):
    #                 for cval in sdf[ckey]:
    #                     for cvalx in cval.strip().split('; '):
    #                         counter[cvalx.strip()]+=1
    #         with open(ofn,'w') as of:
    #             json.dump(dict(counter), of)
    #         return counter
    #     else:
    #         with open(ofn) as f:
    #             return Counter(json.load(f))
        

    def iter_interactions(self,**kwargs):
        for charsys in self.iter_systems():
            yield from charsys.iter_interactions(**kwargs)

    def interactions(self,force=False,ignore_blank=True,**kwargs):
        if not force and self._interactions is not None and len(self._interactions):
            odf=self._interactions
        elif not force and os.path.exists(self.path_interactions):
            odf=read_df(self.path_interactions)
        else:
            odf=self._interactions=pd.DataFrame(self.iter_interactions(**kwargs))
            save_df(odf, self.path_interactions, verbose=True)
        
        if ignore_blank: odf=odf[(odf.source!="") & (odf.target!="")]
        return odf.fillna('')

        

    def init_names_auto_all(self,**kwargs):
        ## get all auto's together
        nametoks=Counter()
        ensure_dir_exists(self.path_names_dir)
        for fn in os.listdir(self.path_names_dir):
            if fn.endswith('.json'):
                fnfn=os.path.join(self.path_names_dir, fn)
                with open(fnfn) as f: fnd=Counter(json.load(f))

                for name,count in fnd.items():
                    # nametoks[clean_character_tok(name)]+=count
                    nametoks[name]+=count

        return nametoks


    # ### Character data
    # def get_character_token_counts(self,event_type=None,force=False,**kwargs):
    #     if force or self._df_count is None:
    #         event_type=self.get_event_type(event_type)
    #         if event_type not in self._events: return Counter()
    #         xdf = self._events[event_type]
    #         o = Counter(list(xdf.u) + list(xdf.v))
    #         self._d_count = o
    #     return self._d_count
    
    def get_character_token_counts(self,force=False,**kwargs):
        if force or self._char_tokd is None:
            ## @REDO
            pass
        else:
            self._char_tokd
        return self._d_count
        
    
    def get_df_feats(self,force=False):
        if force or self._df_feats is None:
            odf_tok2id_anno = self.get_df_names(force=force)
            char_ids = Counter()
            for char_id,char_tok_count in zip(odf_tok2id_anno.char_id, odf_tok2id_anno.char_tok_count):
                try:
                    char_ids[char_id]+=float(char_tok_count)
                except ValueError:
                    pass
            # init?
            id2meta_l=[]
            for char_id,char_id_count in char_ids.most_common():
                char_dx={'char_id':char_id, 'char_id_count':char_id_count, **chardata_metakeys_initial}
                id2meta_l.append(char_dx)
            odf_id2meta = pd.DataFrame(id2meta_l).fillna('')
            #save_df(odf_id2meta, self.path_feats, index=False)
            self._df_feats=merge_read_dfs_anno(self.path_feats, on='char_id').fillna('')
        return self._df_feats
    
    
    
    ### NETWORK
    
    # def events_from_metadata(self,df=None,event_type=None,col_sender='sender_tok',col_recip='recip_tok',col_t='_t'):
    #     if df is None:
    #         sections = self.source.sections()
    #         if sections is not None:
    #             secmeta=sections.metadata(force=False)
    #             if secmeta is not None:
    #                 df=secmeta
    #     if df is None: return
    #     return self.events_from_dataframe(df,event_type=event_type,col_sender=col_sender,col_recip=col_recip,col_t=col_t)

    def init_events(self,
            df,
            event_type=None,
            col_sender='sender_tok',
            col_recip='recip_tok',
            col_t=None):
        if col_sender not in set(df.columns): return
        if col_recip not in set(df.columns): return
        if event_type is None: event_type=self.EVENT_TYPE

        def format_row_dict(row):
            return {
                k:fillna(pd.to_numeric(v,errors='ignore'), '')
                for k,v in row.items()
                # if k not in {col_sender,col_recip}
            }

        cols=set(df.columns)
        if not col_sender or col_sender not in cols: return
        if not col_recip or col_recip not in cols: return        
        
        xdf=pd.DataFrame()
        xdf['u'] = df[col_sender]
        xdf['v'] = df[col_recip]
        xdf['t'] = df[col_t] if col_t and col_t in cols else pd.Series(range(len(df)))+1
        xdf['d'] = df.to_dict(orient='records')
        self._events[event_type]=xdf
        
    def get_event_type(self,event_type=None):
        if event_type is None:
            keys=list(self._events.keys())
            if keys: event_type=keys[0]
        if event_type is None: event_type=self.EVENT_TYPE
        return event_type

    def events_df(self,event_type=None,return_df=False):
        default_events=[] if not return_df else pd.DataFrame()
        event_type=self.get_event_type(event_type)
        if not event_type in self._events: return default_events
        xdf = self._events[event_type].assign(
            u=self._events[event_type].u.apply(self.get_character_id),
            v=self._events[event_type].v.apply(self.get_character_id),
        )
        return xdf

    def events(self,event_type=None):
        xdf=self.events_df(event_type=event_type)
        return zip(xdf.u, xdf.v, xdf.t, xdf.d)

    def time_events(self,event_type=None,progress=True):
        xdf=self.events_df(event_type=event_type)
        iterr=sorted(xdf.groupby('t'))
        if progress: iterr=tqdm(iterr,desc='Iterating over event',total=xdf.t.nunique())
        for t,tdf in iterr:
            yield (t,[(u,v,d) for u,v,d in zip(tdf.u, tdf.v, tdf.d)])

    def time_to_events(self,event_type=None):
        xdf=self.events_df(event_type=event_type)
        events=defaultdict(list)
        for t,tdf in sorted(xdf.groupby('t')):
            events[t]=[(u,v,d) for u,v,d in zip(tdf.u, tdf.v, tdf.d)]
        return events
        

    def graph_iter(self,
            interactions=None,
            t1=None,
            t2=None,
            min_weight=None,
            remove_isolates=True,
            progress=True,
            bad_nodes=BAD_CHAR_IDS,
            nodesep=';',
            **kwargs):
        
        g=nx.DiGraph()
        if interactions is None: interactions=self.interactions()
        interactions=interactions[interactions.t!=""]
        
        oiterr=sorted(list(interactions.groupby('t')))
        if progress: oiterr=tqdm(oiterr, desc='Iterating over interactions')
        for time,timedf in sorted(oiterr):
            events = []
            for u,v,d in zip(timedf.source, timedf.target, timedf.to_dict('records')):
                for u2 in u.split(nodesep):
                    for v2 in v.split(nodesep):
                        u2x=u2.strip()
                        v2x=v2.strip()
                        if u2x in bad_nodes or v2x in bad_nodes: continue
                        events+=[(u2x, v2x, d)]
            
            if not events: continue
            
            for uvdi,(u,v,d) in enumerate(events):
                if not 'weight' in d: d['weight']=1
                if not g.has_edge(u,v):
                    g.add_edge(u,v,t_since=0,**d)
                else:
                    for dk,dv in d.items():
                        if dk not in g.edges[u,v]:
                            g.edges[u,v][dk]=v
                        else:
                            v_old = g.edges[u,v][dk]
                            vnum=pd.to_numeric(dv,errors='coerce')
                            vnum_old=pd.to_numeric(v_old,errors='coerce')
                            try:
                                if not np.isnan(vnum) and not np.isnan(vnum_old):
                                    g.edges[u,v][dk]=vnum_old + vnum
                                else:
                                    g.edges[u,v][dk]=v
                            except ValueError:
                                g.edges[u,v][dk]=v
                
                if not uvdi:
                    g.edges[u,v]['t_new']='red'
                else:
                    g.edges[u,v]['t_new']='blue'
                g.edges[u,v]['t_since']=0

            ## redo t-since
            uvset = {(u,v) for u,v,d in events}
            for xa,xb in list(g.edges(data=False)):
                if (xa,xb) not in uvset:
                    g.edges[(xa,xb)]['t_since']=g.edges[(xa,xb)]['t_since']+1
                    g.edges[(xa,xb)]['t_new']='black'
            
            # print([time,t1,t2])
            if t1 and time<t1: continue
            if t2 and time>t2: break
            g.t=time
            if len(g.edges()): yield g

            
    
    def graph(self, t=0, min_weight=None, remove_isolates=True, progress=False, **kwargs):
        gnow=nx.DiGraph()
        for gnow in self.graph_iter(t2=t, progress=progress, **kwargs): pass
        return filter_graph(gnow,min_weight=min_weight,remove_isolates=remove_isolates)

    ###

    def draw_graph(self,show=True,**kwargs):
        from lltk.model.charnet import draw_nx
        draw_nx(
            self.graph(),
            save_to=os.path.join(self.path_chars,'graphs','fig.static_graph.png'),
            show=show,
            **kwargs
        )

    def draw_graph_dynamic(self,fps=5,final_g=True):
        if final_g is True: final_g=self.graph()
        from lltk.model.charnet import draw_nx_dynamic
        fn,fig=draw_nx_dynamic(
            self.graph_iter(),
            ofn=os.path.join(self.path_chars,'graphs','fig.dynamic_graph.mp4'),
            odir_imgs=os.path.join(self.path_chars,'graphs','imgs'),
            final_g=final_g,
            fps=fps,
            color_by='t_new'
        )
        return fn
    
    def show_edges(self,**kwargs):
        g=self.static_graph(**kwargs)
        i=0
        for a,b,d in sorted(g.edges(data=True), key=lambda uvd: -uvd[-1]['weight']):
            printm(f'''* {a} -> {b} (weight={d["weight"]:.0f} | words={d["num_words"]})''')
            i+=1
            if i>=10: break
            
            


def clean_character_tok(ctok):
    _,tok,_ = gleanPunc2(ctok)
    toks=tok.strip().split()
    toks=[x for x in toks if x and x[0].isalpha() and x[0]==x[0].upper()]
    return ' '.join(toks)