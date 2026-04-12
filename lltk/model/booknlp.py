
from lltk.imports import *
from lltk.model.characters import CharacterSystem


class ModelBookNLP(CharacterSystem):
    CHARACTER_SYSTEM_ID='booknlp'

    def __init__(self,
            text=None,
            prefer_sections=False,
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
        self._chartokds=None
        self._df_tokens=None
        self._df_chars=None
        self._df_chardata=None
        self._interactions=None

    def __repr__(self): return f'[{self.__class__.__name__}]({self.text.addr})'

    def init(self,**kwargs):
        self.parse(**kwargs)

    @property
    def path(self):
        """Save BookNLP output to corpora/[corpus]/booknlp/[textid]/"""
        return os.path.join(
            self.text.corpus.path,
            'booknlp',
            f'{self.language}_{self.model}',
            self.text.id,
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

    def __getattr__(self, name):
        """Allow path_tokens, path_chardata etc. as shortcuts to paths dict."""
        if name.startswith('path_'):
            key = name[5:]
            paths = object.__getattribute__(self, 'paths')
            if key in paths:
                return paths[key]
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

    def iter_models(self,section_type=None,progress=True):
        if not self.prefer_sections or self.text is None or issubclass(self.text.__class__, TextSection):
            yield self
            return
        sections = self.text.sections(section_type)
        if sections is not None:
            section_texts = list(sections.texts())
            if section_texts:
                for tx in get_tqdm(section_texts) if progress else section_texts:
                    yield ModelBookNLP(tx)
                return
        # Fallback: no sections or empty sections — use the text itself
        yield self

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
        output_dirs = [model.path for model in self.models()]
        pmap(
            parse_booknlp,
            list(zip(paths_to_parse, output_dirs)),
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
                ctoks=[(xd['n'],xd['c']) for xd in chardat['mentions']['proper']]
                if not ctoks:
                    ctok=''
                else:
                    ctok=ctoks[0][0]
                    if char_tok_only_capitalized:
                        ctok=' '.join(xw for xw in ctok.split() if xw and xw[0]==xw[0].upper())

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
    def text_root(self): return self.text.text_root if hasattr(self.text, 'text_root') else self.text

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

            mdf=pd.read_csv(model.path_tokens,sep='\t',quoting=3,on_bad_lines='skip')
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


    ### CHARACTERS
    def get_character_token_counts(self,allow_empty_chars=False,**kwargs):
        chardf=self.chardata(allow_empty=allow_empty_chars,**kwargs)
        od=Counter()
        for ctoks in chardf.char_toks:
            for ctok,ccount in ctoks:
                od[ctok]+=ccount
        return od


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
                and (_k.endswith('_id') or _k.endswith('_i'))
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



    # ── Resolved character data ─────────────────────────────────────

    @property
    def _resolution_path(self):
        return os.path.join(self.path, 'characters_resolved.json')

    @property
    def _intros_path(self):
        return os.path.join(self.path, 'llm_anno.char_intros.json')

    def resolved_characters(self):
        """Load LLM-resolved character list from characters_resolved.json.

        Returns:
            list[dict]: Each with name, ids, type, gender, notes.
            None if not yet resolved.
        """
        if not os.path.exists(self._resolution_path):
            return None
        with open(self._resolution_path) as f:
            return json.load(f).get('characters', [])

    def character_intros(self):
        """Load LLM character introduction annotations from llm_anno.char_intros.json.

        Returns:
            list[dict]: Each with character_name, intro_mode, social_class, etc.
            None if not yet annotated.
        """
        if not os.path.exists(self._intros_path):
            return None
        with open(self._intros_path) as f:
            return json.load(f).get('characters', [])

    @property
    def is_resolved(self):
        return os.path.exists(self._resolution_path)

    @property
    def has_intros(self):
        return os.path.exists(self._intros_path)

    # ── LLM wrappers (require largeliterarymodels) ───────────────

    def resolve_characters(self, model=None, max_chars=30, force=False):
        """Run LLM character resolution. Requires largeliterarymodels.

        Args:
            model: LLM model to use (default: GEMINI_FLASH).
            max_chars: Maximum clusters to send to LLM.
            force: Re-resolve even if characters_resolved.json exists.

        Returns:
            list[dict]: Resolved characters.
        """
        if not force and self.is_resolved:
            return self.resolved_characters()
        from largeliterarymodels.tasks import CharacterTask, format_character_roster
        from largeliterarymodels.llm import GEMINI_FLASH
        task = CharacterTask()
        prompt = format_character_roster(self.text, max_chars=max_chars)
        results = task.run(prompt, model=model or GEMINI_FLASH,
                           metadata={'_id': self.text.addr}, force=force)
        result_data = {
            '_id': self.text.addr,
            'characters': [r.model_dump() for r in results],
        }
        os.makedirs(self.path, exist_ok=True)
        with open(self._resolution_path, 'w') as f:
            json.dump(result_data, f, indent=2)
        return result_data['characters']

    def annotate_intros(self, model=None, force=False):
        """Run LLM character introduction annotation. Requires largeliterarymodels.

        Args:
            model: LLM model to use (default: GEMINI_FLASH).
            force: Re-annotate even if llm_anno.char_intros.json exists.

        Returns:
            list[dict]: Character introduction annotations.
        """
        if not force and self.has_intros:
            return self.character_intros()
        if not self.is_resolved:
            self.resolve_characters(model=model)
        from largeliterarymodels.tasks import CharacterIntroTask, format_all_character_intros
        from largeliterarymodels.llm import GEMINI_FLASH
        task = CharacterIntroTask()
        prompts_and_meta = format_all_character_intros(self.text)
        results = []
        for prompt, meta in prompts_and_meta:
            result = task.run(prompt, model=model or GEMINI_FLASH,
                              metadata=meta, force=force)
            results.append(result.model_dump())
        result_data = {
            '_id': self.text.addr,
            'characters': results,
        }
        os.makedirs(self.path, exist_ok=True)
        with open(self._intros_path, 'w') as f:
            json.dump(result_data, f, indent=2)
        return results

    # ── Co-mention network ───────────────────────────────────────

    def comention_network(self, window=200, min_edge_count=5, exclude_generic=True):
        """Build a character co-mention network from BookNLP entities + resolved characters.

        Args:
            window: Token window for co-mention (200 ≈ 1 paragraph).
            min_edge_count: Minimum co-mentions to include an edge.
            exclude_generic: Drop unresolved generic clusters ('the king', 'his son', etc.).

        Returns:
            networkx.Graph with character nodes and weighted co-mention edges.
        """
        import networkx as nx

        resolution_path = os.path.join(self.path, 'characters_resolved.json')
        if not os.path.exists(resolution_path):
            raise FileNotFoundError(
                f"No characters_resolved.json for {self.text.addr}. "
                "Run CharacterTask from largeliterarymodels first."
            )

        with open(resolution_path) as f:
            resolved = json.load(f)

        # Build cluster ID -> canonical name mapping
        id_to_name = {}
        char_meta = {}  # name -> {type, gender, ids, notes}
        for char in resolved['characters']:
            if char['type'] in ('noise', 'abstraction'):
                continue
            name = char['name']
            if exclude_generic:
                if char['type'] == 'collective':
                    continue
                nl = name.lower()
                if (nl.startswith(('generic', 'unknown', 'unnamed', 'a man',
                                   'a woman', 'the one', 'noise',
                                   'unspecified', 'most of', 'one '))
                    or nl.startswith(('the ', 'a ', 'his ', 'her ', 'my ',
                                     'your ', 'some ', 'every '))
                    or nl in ('men', 'people', 'everyone', 'some', 'one')
                    or '(generic' in nl or 'unnamed' in nl
                ):
                    continue
            char_meta[name] = char
            for cid in char['ids']:
                id_to_name[int(cid.lstrip('C'))] = name

        # Load entities and map to resolved names
        entities = pd.read_csv(
            self.path_entities, sep='\t', quoting=3, on_bad_lines='skip'
        )
        entities['name'] = entities['COREF'].map(id_to_name)
        entities = entities.dropna(subset=['name'])

        mentions = entities[['start_token', 'name']].sort_values('start_token').values.tolist()

        # Count co-mentions within window
        from collections import Counter
        edges = Counter()
        for i, (pos_i, name_i) in enumerate(mentions):
            for j in range(i + 1, len(mentions)):
                pos_j, name_j = mentions[j]
                if pos_j - pos_i > window:
                    break
                if name_i != name_j:
                    edge = tuple(sorted([name_i, name_j]))
                    edges[edge] += 1

        # Build graph
        G = nx.Graph()
        # Add all resolved characters as nodes (even if no edges)
        for name, meta in char_meta.items():
            total_mentions = sum(
                len(entities[entities.name == name])
                for _ in [1]  # just count once
            )
            G.add_node(name, type=meta['type'], gender=meta.get('gender', ''),
                       mentions=int((entities.name == name).sum()))

        for (a, b), count in edges.items():
            if count >= min_edge_count:
                G.add_edge(a, b, weight=count)

        # Remove isolated nodes
        G.remove_nodes_from(list(nx.isolates(G)))

        return G

    def plot_network(self, save_path=None, window=200, min_edge_count=5,
                     exclude_generic=True, figsize=(12, 10), max_nodes=25,
                     title=None, **kwargs):
        """Plot the character co-mention network.

        Args:
            save_path: Path to save the figure (PNG/PDF/SVG). If None, shows interactively.
            window: Token window for co-mention.
            min_edge_count: Minimum co-mentions to include an edge.
            exclude_generic: Drop unresolved generic clusters.
            figsize: Figure size tuple.
            max_nodes: Maximum number of nodes to display (by mention count).
            title: Plot title. Defaults to text title + year.

        Returns:
            networkx.Graph (the underlying graph).
        """
        import networkx as nx
        import matplotlib
        if save_path:
            matplotlib.use('Agg')
        import matplotlib.pyplot as plt

        G = self.comention_network(
            window=window, min_edge_count=min_edge_count,
            exclude_generic=exclude_generic
        )

        if len(G) == 0:
            log.warning(f'Empty network for {self.text.addr}')
            return G

        # Trim to top N nodes by mention count
        if len(G) > max_nodes:
            mention_counts = nx.get_node_attributes(G, 'mentions')
            top_nodes = sorted(mention_counts, key=mention_counts.get, reverse=True)[:max_nodes]
            G = G.subgraph(top_nodes).copy()

        # Layout
        weights = [G[u][v]['weight'] for u, v in G.edges()]
        max_w = max(weights) if weights else 1

        pos = nx.spring_layout(G, k=2.0, iterations=80, seed=42,
                               weight='weight')

        fig, ax = plt.subplots(1, 1, figsize=figsize)

        # Edge widths scaled by weight
        edge_widths = [0.5 + 4.0 * (w / max_w) for w in weights]
        edge_alphas = [0.2 + 0.6 * (w / max_w) for w in weights]

        # Node sizes scaled by mention count
        mentions = [G.nodes[n].get('mentions', 10) for n in G.nodes()]
        max_m = max(mentions) if mentions else 1
        node_sizes = [200 + 2000 * (m / max_m) for m in mentions]

        # Color by type
        type_colors = {
            'character': '#4A90D9',
            'collective': '#82B366',
            'place': '#D4A574',
        }
        node_colors = [type_colors.get(G.nodes[n].get('type', 'character'), '#4A90D9')
                       for n in G.nodes()]

        # Draw edges
        for (u, v), w, alpha in zip(G.edges(), edge_widths, edge_alphas):
            nx.draw_networkx_edges(G, pos, edgelist=[(u, v)], width=w,
                                   alpha=alpha, edge_color='#555555', ax=ax)

        # Draw nodes
        nx.draw_networkx_nodes(G, pos, node_size=node_sizes, node_color=node_colors,
                               edgecolors='#333333', linewidths=0.8, alpha=0.9, ax=ax)

        # Labels
        nx.draw_networkx_labels(G, pos, font_size=9, font_weight='bold', ax=ax)

        # Title
        if title is None:
            text_title = getattr(self.text, 'title', '')
            if len(text_title) > 60:
                text_title = text_title[:57] + '...'
            year = getattr(self.text, 'year', '')
            title = f'{text_title} ({year})'
        ax.set_title(title, fontsize=14, pad=20)
        ax.axis('off')
        plt.tight_layout()

        if save_path:
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            fig.savefig(save_path, dpi=150, bbox_inches='tight',
                        facecolor='white', edgecolor='none')
            plt.close(fig)
        else:
            plt.show()

        return G


BOOKNLPD = {}

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
            log.error('BookNLP module not found')
            booknlpd[key]=None
    return booknlpd[key]




def parse_booknlp(
        path_txt_and_odir,
        booknlp=None,
        language=BOOKNLP_DEFAULT_LANGUAGE,
        pipeline=BOOKNLP_DEFAULT_PIPELINE,
        model=BOOKNLP_DEFAULT_MODEL,

        cache=True,
        quiet=True,
        force=False,

        **kwargs):
    # Unpack args — can be (path_txt, odir) tuple or just path_txt
    if isinstance(path_txt_and_odir, (list, tuple)):
        path_txt, odir = path_txt_and_odir
    else:
        path_txt = path_txt_and_odir
        odir = None

    ## quiet?
    if quiet:
        with Capturing() as output:
            return parse_booknlp(
                (path_txt, odir),
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
        if log>0: log(f'File does not exist: {path_txt}')
        return

    # get output path
    if odir is None:
        odir = get_booknlp_text_path(path_txt,language=language,model=model)
    os.makedirs(odir, exist_ok=True)
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
        try:
            return booknlp.process(**okwargs)
        except AttributeError:
            return


def get_booknlp_text_path(
        path_txt,
        language=BOOKNLP_DEFAULT_LANGUAGE,
        model=BOOKNLP_DEFAULT_MODEL,
        **kwargs):
    """Legacy path computation — used as fallback when no corpus context available."""
    pathkey=['booknlp',f'{language}_{model}']
    odir=os.path.join(os.path.abspath(os.path.dirname(path_txt)),*pathkey)
    return odir
