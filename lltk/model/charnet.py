import os,sys,json,pickle,random
import networkx as nx
import pandas as pd
from pprint import pprint
from tqdm import tqdm
import numpy as np

###
TITLES = {
    "Dr", "Dr.",
    "Mr", "Mr.",
    "Ms", "Ms.",
    "Miss",
    "Master", "Mistress",
    "Sir","Lady",
    "Duke","Baron","Count",
    "Prince","Princess","King","Queen",
    "Bishop","Father","Mother"
}


## Dead simple person NER?

def ner_parse_flair(txt):
    from collections import defaultdict
    from flair.models import SequenceTagger
    from flair.data import Sentence
    tagger = SequenceTagger.load('ner')
    from tqdm import tqdm
    from flair.models import MultiTagger
    from flair.tokenization import SegtokSentenceSplitter
    # initialize sentence splitter
    splitter = SegtokSentenceSplitter()

    # load tagger for POS and NER 
    # tagger = MultiTagger.load(['pos', 'ner'])

    senti=0
    tokeni=0
    res=defaultdict(list)
    incl_sent=set()
    for pi,paragraph in enumerate(tqdm(txt.split('\n\n'))):
        for si,sentence in enumerate(splitter.split(paragraph)):
            senti+=1
            tagger.predict(sentence)
            sentdat=sentence.to_dict()
            for datatype in sentdat:
                print(datatype,sentdat[datatype])
                for datad in sentdat[datatype]:
                    if type(datad)==str: continue
                    sentdx={
                        **datad,
                        **{
                            'para':pi+1,
                            'sent':senti,
                            'sent_txt':datad['text'] if senti not in incl_sent else ''
                        }
                    }
                    print(sentdx)
                    res[datatype]+=[sentdx]
                    incl_sent|={senti} 
    return res

def ner_parse_stanza(txt,nlp=None):
    ### Return df of named entities and types
    import stanza
    from tqdm import tqdm
    import pandas as pd

    if nlp is None: nlp = stanza.Pipeline(lang='en', processors='tokenize,ner')
    
    #doc = nlp("Chris Manning teaches at Stanford University. He lives in the Bay Area.")
    #print(*[f'entity: {ent.text}\ttype: {ent.type}' for ent in doc.ents], sep='\n')

    from collections import Counter
    countd=Counter()

    # clean
    # txt = txt.replace('--','—')
    ent_ld=[]
    para_txts = txt.split('\n\n')
    senti=0
    incl_sent=set()
    char_so_far=0
    # for pi,para in enumerate(tqdm(para_txts,desc="Parsing paragraphs")):
    for pi,para in enumerate(para_txts):
        try:
            pdoc = nlp(para)
        except Exception as e:
            print('!!',e)
            continue

        for si,sentdoc in enumerate(pdoc.sentences):
            senti+=1
            for ent in sentdoc.ents:
                # print(dir(ent))
                # print(dir(ent.words[0]))
                ids=[tok.id[0] for tok in ent.tokens]
                idstr=f'{ids[0]}-{ids[-1]}' if len(ids)>1 else str(ids[0])
                entd = {
                    'num_para':pi+1,
                    'num_sent':senti,
                    'text':ent.text,
                    'start_word_in_sent':ids[0],
                    'end_word_in_sent':ids[-1],
                    'start_char_in_para':ent.tokens[0].start_char,
                    'end_char_in_para':ent.tokens[0].end_char,
                    'type':ent.type,
                    'sent_text':sentdoc.text if senti not in incl_sent else '',
                }
                incl_sent|={senti}
                ent_ld.append(entd)
        char_so_far+=len(para)
    return ent_ld

def ner_parse(txt,*x,**y):
    return ner_parse_stanza(txt,*x,**y)


def ner_txt2names(txt,incl_labels={}):
    import stanza
    from tqdm import tqdm
    
    nlp = stanza.Pipeline(lang='en', processors='tokenize,ner')
    
    
    #doc = nlp("Chris Manning teaches at Stanford University. He lives in the Bay Area.")
    #print(*[f'entity: {ent.text}\ttype: {ent.type}' for ent in doc.ents], sep='\n')

    from collections import Counter
    countd=Counter()

    # clean
    # txt = txt.replace('--','—')
    para_txts = txt.split('\n\n')
    for para in tqdm(para_txts,desc="Parsing paragraphs"):
        pdoc = nlp(para)
        for entd in pdoc.ents:
            countd[entd.text]+=1
    return countd

def ner_txt2names_spacy(txt,incl_labels={}): # 'PERSON'
    import spacy
    from tqdm import tqdm


    nlp = spacy.load("en_core_web_lg")

    from spacy.tokens import Span
    import dateparser,time

    def expand_person_entities(doc):
        new_ents = []
        doc.ents = [e for e in doc.ents if e.label_=='PERSON']
        for ent in doc.ents:
            if ent.label_ == "PERSON" and ent.start != 0:
                prev_token = doc[ent.start - 1]
                if prev_token.text in TITLES:
                    new_ent = Span(doc, ent.start - 1, ent.end, label=ent.label)
                    new_ents.append(new_ent)
            else:
                new_ents.append(ent)
        doc.ents = new_ents
        return doc

    # Add the component after the named entity recognizer
    nlp.add_pipe(expand_person_entities, after='ner')

    from collections import Counter
    countd=Counter()

    
    # clean
    txt = txt.replace('--','—')

    # doc = nlp(txt)
    # now=time.time()
    # for entd in doc.ents:
    #     if entd.label_=='PERSON':
    #         countd[entd.text]+=1
    # print(f'done in {int(time.time()-now)} seconds')

    para_txts = txt.split('\n\n')
    for para in tqdm(para_txts,desc="Parsing paragraphs"):
        pdoc = nlp(para)
        for entd in pdoc.ents:
            countd[entd.text]+=1
    
    return countd


class CharacterNetwork:

    def __init__(self, texts=[]):
        self.texts = texts
        self.paths_txt = [(t if type(t)==str else t.path_txt) for t in texts]
        self.paths_nlp = [(os.path.splitext(t.replace('/txt/','/corenlp/'))[0]+'.json' if type(t)==str else t.path_nlp) for t in texts]
        

    def parse_corenlp(self):
        from lltk.model import corenlp
        corenlp.annotate_paths(self.paths_txt)

    def entities(self):
        from collections import Counter
        from lltk.model.corenlp import CoreDoc

        speakers=Counter()
        for path in self.paths_nlp:
            if not os.path.exists(path): continue
            with open(path) as f: pdat=json.load(f)
            
            doc = CoreDoc(pdat)
            for parse in doc.parses:
                for quote in parse.quotes:
                    speakers[quote.data['speaker']]+=1
        
        return speakers





class Hairball():

    
    def from_nx(self,g):
        self.g=g
        # make tables
        self.nx2df()
    
        
    def get_url_or_path(self,url_or_path,tmpfn='/tmp/hairball.download_url.data'):
        # download
        path=None
        print(f'Downloading URL ({url_or_path[:10]}...{url_or_path[-10:]})')
        if url_or_path.startswith('http'):
            import urllib
            urllib.request.urlretrieve(url_or_path, tmpfn)
            path = tmpfn
        else:
            path = url_or_path
        return path
    
    def from_url(self,url_or_path):
        if 'output=ods' in url_or_path or url_or_path.endswith('.ods'):
            return self.from_ods(url_or_path)
        if 'output=csv' in url_or_path or url_or_path.endswith('.csv'):
            return self.from_csv(url_or_path)
    
    def from_csv(self,url_or_path,col_id='id',col_edges='rels'):
        ### Format
        path = self.get_url_or_path(url_or_path)
        
        self.df_nodes = df = pd.read_csv(path).set_index(col_id)
        
        # make edges
        eld = []
        for idx,row in df.iterrows():
            edgestr = str(row[col_edges])
            
            # @TODO can only handle initial snapshot
            edgestr = edgestr.split('-->')[-1].strip()
            
            # loop over each one
            for e in edgestr.split(';'):
                if not ')' in e: continue
                reltype,etrgt=e.replace('(','').strip().split(')',1)
                etrgtmeta=''
                if '[' in etrgt:
                    etrgt,etrgtmeta=etrgt.split('[',1)
                    etrgtmeta.replace(']','').strip()
                etrgt=etrgt.strip()
                for etrgtx in etrgt.split(','):
                    edx = {'source':idx, 'target':etrgtx.strip(), 'reltype':reltype.strip(), 'meta':etrgtmeta}
                    eld.append(edx)
        self.df_edges = pd.DataFrame(eld)
        # from df
        self.df2nx()
        
    
    def from_ods(self,url_or_path):
        from pandas_ods_reader import read_ods
        path = self.get_url_or_path(url_or_path)

        # nodes are in first sheet, edges next
        self.df_nodes = read_ods(path, 1).set_index('id')
        self.df_edges = read_ods(path, 2)
        
        self.df2nx()
        
    def nx2df(self):
        # make node table
        self.df_nodes = pd.DataFrame([
            {
                **{'id':n},
                **d
            }
            for n,d in tqdm(self.g.nodes(data=True),desc='Building df_nodes from g')
        ]).set_index('id')
        
        # make edge table
        self.df_edges = pd.DataFrame([
            {
                **{'source':a, 'target':b},
                **d
            }
            for a,b,d in tqdm(self.g.edges(data=True), desc='Building df_edges from g')
        ])
        
    def df2nx(self):
        if self.df_nodes is None or self.df_edges is None: return
        
        # add nodes
        g=self.g=nx.Graph() if not self.is_directed else nx.DiGraph()
        for row in tqdm(self.df_nodes.reset_index().to_dict(orient="records"),desc='Generating graph nodes from df_nodes'):
            idx=row['id']
            g.add_node(idx, **row)
        
        # add edges
        for row in tqdm(self.df_edges.reset_index().to_dict(orient="records"),desc='Generating graph edges from df_edges'):
            idx1=row['source']
            idx2=row['target']
            g.add_edge(idx1, idx2, **row)
        
        print(f'Generated graph: {g.order()} nodes, {g.size()} edges')
        
        
    def gen_stats(self,stats = ['degree', 'betweenness_centrality']):
        for st in stats:
            func=getattr(nx,st)
            res=func(self.g)
            self.df_nodes[st]=[res[idx] for idx in self.df_nodes.index]
        self.df2nx()
            
    
    @property
    def g_bokeh(self):
        from bokeh.plotting import from_networkx
        return from_networkx(self.g, nx.spring_layout, scale=10, center=(0, 0))
    
    def draw_bokeh(self,
        title='Networkx Graph', 
        save_to=None,
        color_by=None,
        size_by=None,
        default_color='skyblue',
        default_size=15,
        min_size=5,
        max_size=30,
    ):
        from bokeh.io import output_notebook, show, save
        from bokeh.models import Range1d, Circle, ColumnDataSource, MultiLine, EdgesAndLinkedNodes, NodesAndLinkedEdges, LabelSet
        from bokeh.plotting import figure
        from bokeh.plotting import from_networkx
        from bokeh.palettes import Blues8, Reds8, Purples8, Oranges8, Viridis8, Spectral8
        from bokeh.transform import linear_cmap
        from networkx.algorithms import community

        #Establish which categories will appear when hovering over each node
        HOVER_TOOLTIPS = [("ID", "@index")]#, ("Relations")]

        #Create a plot — set dimensions, toolbar, and title
        # possible tools are pan, xpan, ypan, xwheel_pan, ywheel_pan, wheel_zoom, xwheel_zoom, ywheel_zoom, zoom_in, xzoom_in, yzoom_in, zoom_out, xzoom_out, yzoom_out, click, tap, crosshair, box_select, xbox_select, ybox_select, poly_select, lasso_select, box_zoom, xbox_zoom, ybox_zoom, save, undo, redo, reset, help, box_edit, line_edit, point_draw, poly_draw, poly_edit or hover
        plot = figure(
            tooltips = HOVER_TOOLTIPS,
            tools="pan,wheel_zoom,save,reset,point_draw",
#             active_scroll='wheel_zoom',
#             tools="",
            x_range=Range1d(-10.1, 10.1),
            y_range=Range1d(-10.1, 10.1),
            title=title
        )

        #Create a network graph object with spring layout
        # https://networkx.github.io/documentation/networkx-1.9/reference/generated/networkx.drawing.layout.spring_layout.html

        #Set node size and color
        
        # size?
        size_opt = default_size
        if size_by is not None:
            size_opt = '_size'
            data_l = X = np.array([d.get(size_by,0) for n,d in self.g.nodes(data=True)])
            data_l_norm = (X - X.min(axis=0)) / (X.max(axis=0) - X.min(axis=0))
            data_scaled = [(min_size + (max_size * x)) for x in data_l_norm]
            for x,n in zip(data_scaled, self.g.nodes()):
                self.g.nodes[n]['_size']=x
                

        # get network
        network_graph = self.g_bokeh
        
        
        # render nodes
        network_graph.node_renderer.glyph = Circle(
            size=size_opt, 
            fill_color=color_by if color_by is not None else default_color
        )

        #Set edge opacity and width
        network_graph.edge_renderer.glyph = MultiLine(line_alpha=0.5, line_width=1)

        #Add network graph to the plot
        plot.renderers.append(network_graph)

        #Add Labels
        x, y = zip(*network_graph.layout_provider.graph_layout.values())
        node_labels = list(self.g.nodes())
        source = ColumnDataSource({'x': x, 'y': y, 'name': [node_labels[i] for i in range(len(x))]})
        labels = LabelSet(x='x', y='y', text='name', source=source, background_fill_color='white', text_font_size='10px', background_fill_alpha=.7)
        plot.renderers.append(labels)

        show(plot)
        if save_to: save(plot, filename=save_to)





def use_notebook():
    from bokeh.io import output_notebook
    output_notebook()

def is_url(x): return type(x)==str and x.strip().startswith('http')
def is_path(x): 
    return type(x) == str and os.path.exists(x)
def is_graph(x): return type(x) in {nx.Graph, nx.DiGraph}

def tupper(x): return x[0].upper()+x[1:]


def condense_booknlp_output(df=None,url=None):
    if df is None and url: df=pd.read_csv(url)
    if df is None: return None
    
    nameld=[]
    gby='name_real'
    other_cols='gender	race	class	other	notes'.split()
    for name,namedf in df.groupby(gby):
        #names=  ', '.join(tupper(x) for x in namedf.names)
        names = {tupper(nm.strip())
                 for nms in namedf.names
                 for nm in nms.split(',')
                 if nm.strip()
                }
        
        
        namedx={}
        namedx['ID']=tupper(name)
        namedx['Label']=tupper(name)
        namedx['Names'] = ', '.join(names)
        namedx['Num'] = sum(namedf.num)
        for oc in other_cols: namedx[oc]=namedf[oc].iloc[0]
        nameld.append(namedx)
    newdf=pd.DataFrame(nameld).sort_values('Num',ascending=False).fillna('')
    newdf['Rank']=[i+1 for i,x in enumerate(newdf.index)]
    return newdf






def test():
    import lltk
    C = lltk.load('Chadwyck')
    texts = [t for t in C.texts() if 'Austen' in t.author and 'Emma' in t.title]
    charnet = CharacterNetwork(texts)
    
    speakers = charnet.entities()
    print(speakers.most_common(100))
    #  dict_keys(['index', 'paragraph', 'parse', 'basicDependencies', 'enhancedDependencies', 'enhancedPlusPlusDependencies', 'entitymentions', 'tokens'])
    # print([os.path.exists(x) for x in charnet.paths_nlp])



