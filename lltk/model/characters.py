from lltk.imports import *
import networkx as nx


chardata_metakeys_initial = dict(
    char_race='',
    char_gender='',
    char_class='',
    char_geo_birth='',
    char_geo_marriage='',
    char_geo_death='',
    char_geo_begin='',
    char_geo_middle='',
    char_geo_end='',
)




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
    
    def __init__(self,source,_char_tokd=Counter(),**kwargs):
        self._source=source
        self._events={}
        self._df_feats=None
        self._df_names=None
        self._char_tokd=_char_tokd
        self._d_count=_char_tokd
        self._character_tok2id={}
        # self._dg=dnx.DynGraph(edge_removal=True)

    
        
    @property
    def source(self): return self._source
    @property
    def path_chars(self): return os.path.join(self.source.text_root.path, DIR_CHARS_NAME)
    @property
    def path_names_dir(self): return os.path.join(self.path_chars,'names')
    @property
    def path_names(self): return os.path.join(self.path_chars,'names.csv')
    @property
    def path_names_anno(self): return get_anno_fn_if_exists(self.path_names, return_fn_otherwise=False)

    @property
    def path_names_auto(self): return os.path.join(self.path_names_dir,f'{self.CHARACTER_SYSTEM_ID}.json')
    @property
    def path_feats(self): return os.path.join(self.path_chars,'feats.csv')

    ## DF convenient
    @property
    def df_names(self): return self.get_df_names()
    @property
    def df_feats(self): return self.get_df_feats()

    def get_character_id(self,char_tok,force=False,bad={'?','?!','x'}):
        if not self._character_tok2id:
            self.init_names(force=force)
        o=self._character_tok2id.get(char_tok,'')
        if o in bad: o=''
        return o


    def names(self,force=False):
        if force or self._df_names is None:
            self.init_names(force=force)
        return self._df_names

    def init_names(self,force=False):
        if not force and self._df_names is not None and self._character_tok2id is not None: return
        ## auto-generate my names if haven't
        self.init_names_auto(force=force)
        self.init_names_sections_auto(force=force)
        ## get total auto freq
        name_counts = self.init_names_auto_all()
        ## get annotated renamer
        name2id = self.init_names_anno()
        odf=pd.DataFrame(
            dict(
                character_tok=ctok,
                character_id=name2id.get(ctok,'?'),
                character_id_auto=zeropunc(ctok),
                character_tok_count=name_counts[ctok],
            )
            for ctok in name_counts
        )
        odf=odf.sort_values('character_tok_count',ascending=False)
        save_df(odf,self.path_names,index=False)
        self._df_names=odf
        self._character_tok2id = dict((zip(odf.character_tok,odf.character_id)))

    
    def init_names_anno(self):
        fn=self.path_names_anno
        if fn and os.path.exists(fn):
            odf=read_df(fn)
            cols=set(odf.columns)
            if len(odf) and 'character_tok' in cols and 'character_id' in cols:
                od=dict(zip(odf.character_tok, odf.character_id))
                return od
        return {}


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

    def init_names_sections_auto(
            self,
            section_type=None,
            char_keys=['sender_tok','recip_tok','char_tok'],
            force=False):
        ofn=os.path.join(self.path_names_dir,f'{section_type if section_type else "sections"}.json')
        if force or not os.path.exists(ofn):
            counter=Counter()
            secs=self.sections(section_type)
            if secs is None: return counter
            sdf=secs.meta
            if sdf is None or not len(sdf): return counter
            for ckey in char_keys:
                if ckey in set(sdf.columns):
                    for cval in sdf[ckey]:
                        for cvalx in cval.strip().split('; '):
                            counter[cvalx.strip()]+=1
            with open(ofn,'w') as of:
                json.dump(dict(counter), of)
            return counter
        else:
            with open(ofn) as f:
                return Counter(json.load(f))
        
        

    def init_names_auto_all(self,**kwargs):
        ## get all auto's together
        nametoks=Counter()
        ensure_dir_exists(self.path_names_dir)
        for fn in os.listdir(self.path_names_dir):
            if fn.endswith('.json'):
                fnfn=os.path.join(self.path_names_dir, fn)
                with open(fnfn) as f: fnd=Counter(json.load(f))

                for name,count in fnd.items():
                    nametoks[clean_character_tok(name)]+=count

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
            event_type=None,
            t1=None,
            t2=None,
            min_weight=None,
            remove_isolates=True,
            progress=True,
            bad_nodes={'',None,'?'},
            **kwargs):
        
        g=nx.DiGraph()
        for time,events in self.time_events(event_type=event_type,progress=progress):
            #if t1 and time<t1: continue
            #if t2 and time>t2: continue

            for uvdi,(u,v,d) in enumerate(events):
                if u in bad_nodes or v in bad_nodes: continue
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
                            if not np.isnan(vnum) and not np.isnan(vnum_old):
                                g.edges[u,v][dk]=vnum_old + vnum
                            else:
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
            yield g

            
    
    def graph(self, t=0, min_weight=None, remove_isolates=True, progress=False, **kwargs):
        gnow=nx.DiGraph()
        for gnow in self.graph_iter(t2=t, progress=progress, **kwargs): pass
        return filter_graph(gnow,min_weight=min_weight,remove_isolates=remove_isolates)

    ###
    
    def show_edges(self,**kwargs):
        g=self.static_graph(**kwargs)
        i=0
        for a,b,d in sorted(g.edges(data=True), key=lambda uvd: -uvd[-1]['weight']):
            printm(f'''* {a} -> {b} (weight={d["weight"]:.0f} | words={d["num_words"]})''')
            i+=1
            if i>=10: break
            
            
def filter_graph(g,min_weight=None,remove_isolates=True):
    if min_weight:
        for a,b,d in list(g.edges(data=True)):
            if d['weight']<=min_weight:
                g.remove_edge(a,b)
    if remove_isolates:
        degree=dict(g.degree()).items()
        for node,deg in degree:
            if not deg:
                g.remove_node(node)
    return g



def clean_character_tok(ctok):
    _,tok,_ = gleanPunc2(ctok)
    toks=tok.strip().split()
    toks=[x for x in toks if x and x[0].isalpha() and x[0]==x[0].upper()]
    return ' '.join(toks)