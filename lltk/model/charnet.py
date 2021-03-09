from lltk.imports import *

from lltk.imports import *


class CharacterNetwork:

    def __init__(self, url_or_path_or_g = None, is_directed=True):
        # init
        self.df_nodes = None
        self.df_edges = None
        self.is_directed = is_directed
        self.g = None
        self.gl = []
    
    def from_nx(self,g):
        self.g=g
        # make tables
        self.nx2df()
    
        





    ##############
    # Dynamic networks
    ##############

    def parse_dynamic(self, path_or_url, id_col = 'id', rel_col='rels', setting_col='setting', bad_nodes={}):
        # For now must be csv
        df=pd.read_table(path_or_url,sep=None)
        
        # build data
        ld_edges = []
        for i,row in df.sort_values(id_col).iterrows():
            idx=row.get(id_col)
            rels=row.get(rel_col,'')
            attrs = dict((k,v) for k,v in dict(row).items() if not k in {id_col,rel_col})
            
            ## edges
            for edge in str(rels).split(';'):
                edge=edge.strip()
                try:
                    c1,c2 = edge.replace('>','|').replace('<','|').replace('||','|').replace('| |','|').split('|',1)
                except ValueError:
                    continue
                c1l = [x.strip() for x in c1.split(',')]
                c2l = [x.strip() for x in c2.split(',')]
                # print(edge,'\n',c1l,c2l)

                # # type of edge?
                if '<>' in edge:
                    # xl=yl=c1l+c2l
                    xl,yl=c1l,c2l
                elif '>' in edge:
                    xl,yl=c1l,c2l
                elif '<' in edge:
                    xl,yl=c2l,c1l
                else:
                    continue
                # xl=yl=c1l+c2l

                # add
                for x in xl:
                    x=x.split('[')[0].strip()
                    for y in yl:
                        y=y.split('[')[0].strip()
                        if x in bad_nodes or y in bad_nodes: continue
                        if x==y: continue
                        # print('?',x,y)
                        # if x>=y: continue
                        ld_edges += [{
                            'i':len(ld_edges),
                            't':idx,
                            'source':x,
                            'target':y,
                            **attrs
                        }]

                        # ???
                        ld_edges += [{
                            'i':len(ld_edges),
                            't':idx,
                            'source':y,
                            'target':x,
                            **attrs
                        }]

        # set as df
        self.df_edges =dfe= pd.DataFrame(ld_edges)
        if 'lat' in dfe.columns and 'lon' in dfe.columns:
            dfe['merc_x'],dfe['merc_y']=zip(*[merc(lat,lon) for lat,lon in zip(dfe['lat'],dfe['lon'])])


        ### allow node table too @TODO
        self.df_nodes = pd.DataFrame([
            {'id':charname, 'name':charname}
            for charname in set(list(self.df_edges.source) + list(self.df_edges.target)) 
        ])

        for df in [self.df_nodes, self.df_edges]:
            if setting_col in df.columns:
                sd=self.settings_latlon()
                df['lat'],df['lon']=zip(*[
                    list([(0,0)]+[sd.get(x.strip()) for x in stg.split(';') if x.strip() in sd])[-1]
                    for stg in df[setting_col]
                ])
                df['merc_x'],df['merc_y']=zip(*[merc(lat,lon) for lat,lon in zip(df['lat'],df['lon'])])
                merc_x_avg=df['merc_x'].mean()
                merc_y_avg=df['merc_y'].mean()
                df['merc_x']=[x if not np.isnan(x) else merc_x_avg for x in df['merc_x']]
                df['merc_y']=[x if not np.isnan(x) else merc_y_avg for x in df['merc_y']]

    ### Settings?
    def settings_latlon(self,in_edges=True,in_nodes=True,setting_col='setting'):
        if hasattr(self,'_settings_latlon') and self._settings_latlon: return self._settings_latlon
        # import
        from geopy.geocoders import Nominatim
        geolocator = Nominatim(user_agent='lltk_charnet')

        # cache?
        if os.path.exists(LATLONG_CACHE_FN):
            with open(LATLONG_CACHE_FN) as f:
                cacheloc=json.load(f)
        else:
            cacheloc={}
        # build
        self._settings_latlon=settingd={}
        dfs=[]
        if in_nodes and setting_col in self.df_nodes.columns: dfs+=[self.df_nodes]
        if in_edges and setting_col in self.df_edges.columns : dfs+=[self.df_edges]
        settings = {
            x.strip()
            for df in dfs
            for stg in df[setting_col]
            for x in stg.split(';')
        }
        # for setting in tqdm(list(settings),desc='Geolocating settings'):
        for setting in settings:#),desc='Geolocating settings'):
            if setting in cacheloc:
                settingd[setting]=cacheloc[setting]
            else:
                loc=geolocator.geocode(setting)
                if loc is not None:
                    cacheloc[setting]=settingd[setting]=(loc.latitude, loc.longitude)
                    
        with open(LATLONG_CACHE_FN,'w') as of: json.dump(cacheloc,of)
        return self._settings_latlon
        
                



















    ##############
    # Static networks
    ##############

    def static_from_url(self,url_or_path):
        if 'output=ods' in url_or_path or url_or_path.endswith('.ods'):
            return self.static_from_ods(url_or_path)
        if 'output=csv' in url_or_path or url_or_path.endswith('.csv'):
            return self.static_from_csv(url_or_path)
    
    def static_from_csv(self,url_or_path,col_id='id',col_edges='rels'):
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
        
    
    def static_from_ods(self,url_or_path):
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
        
    def df2nx(self,t_max=None,t_min=None,i_min=None,i_max=None):
        if self.df_nodes is None or self.df_edges is None: return
        
        # add nodes
        g=self.g=nx.Graph()# if not self.is_directed else nx.DiGraph()
        # for row in self.df_nodes.reset_index().to_dict(orient="records"):
        #     idx=row['id']
        #     g.add_node(idx, last_node=False, **row)
        
        # add edges
        ld_edges=self.df_edges.reset_index().to_dict(orient="records")
        for i,row in enumerate(ld_edges):
            idx1=row['source']
            idx2=row['target']
            if t_max is not None and row.get('t',0)>t_max: continue
            if t_min is not None and row.get('t',0)<t_min: continue
            if i_min is not None and i<i_min: continue
            if i_max is not None and i>i_max: continue
            if not g.has_edge(idx1,idx2):
                g.add_edge(idx1, idx2, weight=1, **row)
            else:
                g[idx1][idx2]['weight']+=1
                for k,v in row.items():g[idx1][idx2][k]=v

            # last edge?
            last_edge = i==i_max if i_max is not None else ((i+1)==len(ld_edges))
            g[idx1][idx2]['last_edge']=last_edge
            g.nodes[idx1]['last_node']=last_edge
            g.nodes[idx2]['last_node']=False

            # propagate latest edge attrs?
            for k,v in sorted(row.items()):
                if v is not None:
                    if k in {'name','id','rels'}: continue
                    if k in {'lat','lon','merc_x','merc_y'}:
                        #print(k,v,idx1,idx2,g.nodes[idx1].get(k),g.nodes[idx2].get(k),g.nodes[idx1].get(k)!=v,g.nodes[idx2].get(k)!=v)
                        # v=round(v,8)
                        # g.nodes[idx1][k+'_changed']=ch1=k in g.nodes[idx1] and g.nodes[idx1][k]!=v
                        # g.nodes[idx2][k+'_changed']=ch2=k in g.nodes[idx2] and g.nodes[idx2][k]!=v
                        g.nodes[idx1][k+'_changed']=ch1=g.nodes[idx1].get(k)!=v
                        g.nodes[idx2][k+'_changed']=ch2=g.nodes[idx2].get(k)!=v
                        # if ch1: print(k,v,idx1,g.nodes[idx1].get(k),v)
                        # if ch2: print(k,v,idx2,g.nodes[idx2].get(k),v)
                        g.nodes[idx1][k]=v
                        g.nodes[idx2][k]=v

            # setting?
        # print(f'Generated graph: {g.order()} nodes, {g.size()} edges')
        self.g=g
        self.gen_stats()
        # from copy import deepcopy
        # return deepcopy(g)
        return self.g
        
        
    def gen_stats(self,stats = ['degree', 'betweenness_centrality']):
        for st in stats:
            func=getattr(nx,st)
            res=func(self.g)
            for n,v in dict(res).items(): self.g.nodes[n][st]=v
            self.df_nodes[st]=[dict(res).get(idx) for idx in self.df_nodes.id]
        # self.df2nx()
        
            
    
   





import math,numpy as np
def merc(lat, lon):
    try:
        r_major = 6378137.000
        x = r_major * math.radians(lon)
        scale = x/lon
        y = 180.0/math.pi * math.log(math.tan(math.pi/4.0 + lat * (math.pi/180.0)/2.0)) * scale
        return (x, y)
    except Exception:
        return (np.nan,np.nan)



def layout_graph_force(g,pos=None,iterations=10000,**attrs):
    from fa2 import ForceAtlas2
    defaults=dict(
        # Behavior alternatives
        outboundAttractionDistribution=False,  # Dissuade hubs
        linLogMode=False,  # NOT IMPLEMENTED
        adjustSizes=False,  # Prevent overlap (NOT IMPLEMENTED)
        edgeWeightInfluence=1.0,

        # Performance
        jitterTolerance=1.0,  # Tolerance
        barnesHutOptimize=True,
        barnesHutTheta=1.2,
        multiThreaded=False,  # NOT IMPLEMENTED

        # Tuning
        scalingRatio=1.0,
        strongGravityMode=False,
        gravity=1.0,

        # Log
        verbose=False
    )
    newattrs={
        **defaults,
        **dict((k,v) for k,v in attrs.items() if k in defaults)
    }
    # if pos is None: pos={}
    # print(pos.keys())
    # print(pos,'!!')
    # print(g.nodes())
    forceatlas2=ForceAtlas2(**newattrs)
    # print('inp',pos,'?!?!?')
    pos=dict(forceatlas2.forceatlas2_networkx_layout(g,pos=pos,iterations=iterations).items())
    # print('\nres',pos,'\n')
    return pos

def layout_graph_geo(
        g,
        pos={},
        iterations=0,
        jitter=True,
        fudge_fac=0,
        range_x=0,
        range_y=0,
        range_fac=0,
        **attrs):
    if pos is None:pos={}
    if pos:
        # fudge_x,fudge_y = list(pd.DataFrame(dict((k,v) for k,v in pos.items() if g.has_node(k))).T.std() * fudge_fac)
        fudge_x,fudge_y = list(pd.DataFrame(pos).T.std() * fudge_fac)
        # if range_x: fudge_x/=range_x
        # if range_y: fudge_y/=range_y
    else:
        fudge_x,fudge_y = 0,0
    # print(fudge_fac,fudge_x,fudge_y,'fudge')

    worked=[]
    for node,dat in g.nodes(data=True):
        # if dat.get('lat_changed'): print(node)
        # if not node in pos: print(node,'??')
        
        # print(node,dat.get('lat_changed'),pos.get(node))
        if node in pos and not dat.get('merc_x_changed'):
            pass
        else:
            # print('regen pos for',node)
            # print(pos.get(node))
            # print('moving',node,dat.get('lat_changed'),dat.get('long_changed'))
            # latlon=dat.get('lat',0), dat.get('lon',0)
            # # print(latlon)

            # try:
            #     posx=merc(*latlon)
            #     # print(posx)
            #     worked+=[posx]
            # except ZeroDivisionError:
            #     if not worked:
            #         posx=pos.get(node,(0,0))
            #     else:
            #         posx=worked[-1]
            # print(dat)
            xj = range_x * (range_fac/2)
            yj = range_y * (range_fac/2)
            #print(xj,yj,range_x,range_y,range_fac)
            
            posx=(dat.get('merc_x'), dat.get('merc_y'))
            #posvals=set(pos.values())
            #if posx in posvals:
            posx=(
                posx[0] + random.normalvariate(-fudge_x,fudge_x), #random.normalvariate(-xj,xj),
                posx[1] + random.normalvariate(-fudge_y,fudge_y), #random.normalvariate(-xj,xj),
            )
            pos[node]=posx
        # print(node,dat.get('lat_changed'),pos.get(node))
        # print()


    if jitter and iterations:
        pos=dict(layout_graph_force(g,pos=pos,iterations=iterations,**attrs).items())
    # if jitter:
        # pos=nx.spring_layout(g,pos=pos)#,scale=1.1)
        # pos=layout_graph_force(g,pos=pos,iterations=iterations,**attrs)


    return pos

def layout_graph(g,pos=None,pos_by='force',iterations=10000,**attrs):
    if pos_by=='geo':
        return layout_graph_geo(g,pos=pos,iterations=iterations,**attrs)
    return layout_graph_force(g,pos=pos,iterations=iterations,**attrs)
    

### Draw quadratic bezier paths
def bezier(start, end, control, steps):
    return [(1-s)**2*start + 2*(1-s)*s*control + s**2*end for s in steps]




#### DRAWING



def to_bokeh(g,pos=None,pos_by='force',iterations=2000,**attrs):
    from bokeh.plotting import from_networkx
    # gu=g.to_undirected()
    gu=g
    if not pos or pos_by=='geo':
        pos = layout_graph(gu,pos=pos,pos_by=pos_by,iterations=iterations,**attrs)
    
    posnow = dict((n,x) for n,x in pos.items() if gu.has_node(n))
    
    # # jitter?
    # xj = range_x * (range_fac/2)
    # yj = range_y * (range_fac/2)
    # for node in posnow:
    #     x,y = posnow[node]
    #     x2=x + random.normalvariate(-xj,xj)
    #     y2=y + random.normalvariate(-yj,yj)
    #     posnow[node]=(x2,y2)
    
    return from_networkx(gu,posnow),posnow


def draw_bokeh(g,
    title='Networkx Graph', 
    save_to=None,
    color_by=None,
    color_line_by=None,
    size_by=None,
    size_max_val=None,
    weight_max_val=None,
    default_color='skyblue',
    default_size=15,
    pos_by='force',
    min_size=5,
    max_size=30,
    min_weight=1,
    max_weight=3,
    min_degree=0,
    pos=None,
    show_plot=True,
    nodeval2color={True:'red',False:'blue',None:'blue'},
    edgeval2color={True:'red',False:'black',None:'black'},
    min_x=-10.1,
    min_y=-10.1,
    max_x=10.1,
    max_y=10.1,
    range_fac=0.5,
    geo_default_range=1000000,
    line_alpha=0.5,
    webdriver=None,
    **attrs
):
    from bokeh.io import output_notebook, show, save
    from bokeh.models import Range1d, Circle, ColumnDataSource, MultiLine, EdgesAndLinkedNodes, NodesAndLinkedEdges, LabelSet
    from bokeh.plotting import figure
    from bokeh.plotting import from_networkx
    from bokeh.palettes import Blues8, Reds8, Purples8, Oranges8, Viridis8, Spectral8
    from bokeh.transform import linear_cmap
    from networkx.algorithms import community
    from bokeh.io import export_png
    from bokeh.models import Ellipse, GraphRenderer, StaticLayoutProvider
    import networkx as nx


    # get network

    #Establish which categories will appear when hovering over each node
    HOVER_TOOLTIPS = [("ID", "@index")]#, ("Relations")]

    #Create a plot — set dimensions, toolbar, and title
    # possible tools are pan, xpan, ypan, xwheel_pan, ywheel_pan, wheel_zoom, xwheel_zoom, ywheel_zoom, zoom_in, xzoom_in, yzoom_in, zoom_out, xzoom_out, yzoom_out, click, tap, crosshair, box_select, xbox_select, ybox_select, poly_select, lasso_select, box_zoom, xbox_zoom, ybox_zoom, save, undo, redo, reset, help, box_edit, line_edit, point_draw, poly_draw, poly_edit or hover
    figopts=dict(
        tooltips = HOVER_TOOLTIPS,
        tools="pan,wheel_zoom,save,reset,point_draw",
            active_scroll='wheel_zoom',
#             tools="",
        
        title=title,
        plot_width=999,
        plot_height=999
    )

    if pos_by=='geo':
        figopts['x_axis_type']='mercator'
        figopts['y_axis_type']='mercator'
    #     figopts['x_range']=Range1d(min_x, 6000000)
    #     figopts['y_range']=Range1d(-1000000, 7000000)
        
    # else:
    # expand range?
    range_x = abs(max_x - min_x)
    range_y = abs(max_y - min_y)
    if pos_by=='geo' and not range_x: range_x=geo_default_range
    if pos_by=='geo' and not range_y: range_y=geo_default_range
    # print('!?!?',range_x,range_y,max_x,min_x,max_y,min_y)
    # if pos_by=='geo':
    figopts['x_range']=Range1d(min_x - (range_x*range_fac),max_x + (range_x*range_fac))
    figopts['y_range']=Range1d(min_y - (range_y*range_fac),max_y + (range_y*range_fac))
    # else:
    # figopts['x_range']=Range1d(min_x,max_x)
    # figopts['y_range']=Range1d(min_x,max_y)

    # start fig
    plot = figure(**figopts)
    plot.title.text_font_size = '18px'

    if pos_by=='geo':
        from bokeh.tile_providers import CARTODBPOSITRON, get_provider
        tile_provider = get_provider(CARTODBPOSITRON)
        plot.add_tile(tile_provider)

    # size?
    
    
    if size_by is not None:
        size_opt = '_size'
        data_l = X = np.array([d.get(size_by,0) for n,d in g.nodes(data=True)])
        maxval=size_max_val if size_max_val else X.max(axis=0)
        data_l_norm = (X - X.min(axis=0)) / (size_max_val - X.min(axis=0))
        data_scaled = [(min_size + (max_size * x)) for x in data_l_norm]
        for x,n in zip(data_scaled, g.nodes()):
            g.nodes[n]['_size']=x
    
    # color?
    for n,dat in g.nodes(data=True):
        # print(n,dat['last_node'])
        g.nodes[n]['_color']=nodeval2color.get(dat.get(color_by),default_color)


    # filter graph
    if min_degree:
        g=g
        # from copy import deepcopy
        # g=deepcopy(g)
        # g=g.copy()
        for n,d in list(g.nodes(data=True)):
            if d.get('degree')<min_degree:
                g.remove_node(n)

    network_graph,pos = to_bokeh(g,pos=pos,range_x=range_x,range_y=range_y,range_fac=range_fac,**attrs)

    
    # render nodes
    network_graph.node_renderer.glyph = Circle(
        size=size_opt, 
        fill_color='_color',#color_by if color_by is not None else default_color
    )

    #Set edge opacity and width
    # network_graph.edge_renderer.glyph = MultiLine(line_alpha=0.5, line_width=1)
    network_graph.edge_renderer.data_source.data["line_width"] = [
        ((g.get_edge_data(a,b).get('weight',1) / weight_max_val) * max_weight) + min_weight
        for a, b in g.edges()
    ]
    network_graph.edge_renderer.glyph.line_width = {'field': 'line_width'}

    if color_line_by:
        vals=[g.get_edge_data(a,b).get(color_line_by,'black') for a, b in g.edges()]
        valtypes=set(vals)
        network_graph.edge_renderer.data_source.data["line_color"] = [
            edgeval2color.get(vx) for vx in vals
        ]
        network_graph.edge_renderer.glyph.line_color = {'field': 'line_color'}
    network_graph.edge_renderer.glyph.line_alpha=line_alpha


    # ### Curve edges?
    # graph_layout = pos
    # graph = network_graph
    # graph.layout_provider = StaticLayoutProvider(graph_layout=graph_layout)
    # ### Draw quadratic bezier paths
    # def bezier(start, end, control, steps):
    #     return [(1-s)**2*start + 2*(1-s)*s*control + s**2*end for s in steps]
    # xs, ys = [], []
    # sx, sy = graph_layout[0]
    # steps = [i/100. for i in range(100)]
    # for node_index in node_indices:
    #     ex, ey = graph_layout[node_index]
    #     xs.append(bezier(sx, ex, 0, steps))
    #     ys.append(bezier(sy, ey, 0, steps))
    # graph.edge_renderer.data_source.data['xs'] = xs
    # graph.edge_renderer.data_source.data['ys'] = ys
    # #######

    #Add network graph to the plot
    plot.renderers.append(network_graph)

    #Add Labels
    x, y = zip(*network_graph.layout_provider.graph_layout.values())
    node_labels = list(g.nodes())
    # source = ColumnDataSource({'x': x, 'y': y, 'name': [node_labels[i] for i in range(len(x))]})
    source = ColumnDataSource({'x':x,'y':y,'name':node_labels})
    labels = LabelSet(x='x', y='y', text='name', source=source, background_fill_color='white', text_font_size='16px', background_fill_alpha=.7)
    plot.renderers.append(labels)

    
    if save_to: export_png(plot, filename=save_to, webdriver=webdriver)#, width=999, height=999)
    if show_plot: show(plot)




def bokeh_uses_notebook():
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






#### Graph Code ######


def make_vid_from_folder(folder,ofn=None,fps=5):
    import moviepy.video.io.ImageSequenceClip
    from base64 import b64encode
    from IPython.display import HTML

    
    # get img files
    if not ofn:
        if folder.endswith(os.path.sep): folder=folder[:-1]
        ofn=folder+'.mp4'
        if os.path.exists(ofn): os.remove(ofn)
    
    image_files = [os.path.join(folder,img) for img in sorted(os.listdir(folder)) if img.endswith(".png")]
    clip = moviepy.video.io.ImageSequenceClip.ImageSequenceClip(image_files, fps=fps)
    try:
        clip.write_videofile(ofn,verbose=False,logger=None) #progress_bar=False)
    except Exception:
        clip.write_videofile(ofn,verbose=False,progress_bar=False)

    
    
    with open(ofn,'rb') as f: mp4=f.read()
    data_url = "data:video/mp4;base64," + b64encode(mp4).decode()
    htm=HTML("""
    <video width=666 controls>
        <source src="%s" type="video/mp4">
    </video>
    """ % data_url)
    return ofn,htm

def make_gif_from_folder(folder,ofn=None,fps=5):
    import imageio
    from IPython.display import HTML

    images = []
    # for fn in tqdm(sorted(os.listdir(folder)),desc='Building gif from images'):
    for fn in sorted(os.listdir(folder)):#,desc='Building gif from images'):
        if fn.endswith('.png'):
            with open(os.path.join(folder,fn),'rb') as f:
                images.append(imageio.imread(f))

    if not ofn:
        if folder.endswith(os.path.sep): folder=folder[:-1]
        ofn=folder+'.gif'
        if os.path.exists(ofn): os.remove(ofn)
    imageio.mimsave(ofn, images, duration = 1/fps)
    return ofn, HTML(f'''<img src="{ofn}" />''')

def save_combo(idir_left, idir_right, odir, fps=5):
    # combine images
    combo_imgs(
        idir_left,
        idir_right,
        odir
    )
    # make gif
    #return make_gif_from_folder(odir,fps=fps)
    return make_vid_from_folder(odir,fps=fps)


def make_both(url,vnum,name,attrs_force={},attrs_geo={},fps=5):
    default_attrs_force = dict(
        pos_by='force',
        odir=f'figures/charnet/{name}/force/{vnum}',
        repos=False,
        fudge_fac=0,
        iterations=10000,
        num_proc=DEFAULT_NUM_PROC,
#         i_max=10,
        rescale=False,
        save_gif=False,
        name=name
    )
    
    default_attrs_geo = dict(
        pos_by='geo',
        odir=f'figures/charnet/{name}/geo/{vnum}',
        repos=True,
#         i_max=10,
        fudge_fac=0.1,
        iterations=0,#1000,
        num_proc=DEFAULT_NUM_PROC,
        rescale=False,
        rescale_memory=45,
        range_fac=0,
        save_gif=False,
        name=name
    )
    
    newattrs_force = {**default_attrs_force, **attrs_force}
    newattrs_geo = {**default_attrs_geo, **attrs_geo}
    save_nets(url,**newattrs_force)
    save_nets(url,**newattrs_geo)

    combo_odir=newattrs_force['odir'].replace('/force/','/combo/')
    return save_combo(
        newattrs_geo['odir'],
        newattrs_force['odir'],
        combo_odir,
        fps=fps
    )
    
    
    
def do_save_nets(obj,*args,**kwargs):
    g,obj_kwds=obj
    kwds = {**kwargs, **obj_kwds}
#     print(kwds.get('min_x'),kwds.get('max_x'))
#     print(kwds.get('min_y'),kwds.get('max_y'))
#     print()
    draw_bokeh(
        g,
        **kwds
    )

def save_nets(
        url,
        bad_nodes=[],
        odir='graph_imgs',
        size_by='degree',
        fps=5,
        num_proc=DEFAULT_NUM_PROC,
        pos_by='force',
        repos=False,
        i_max=None,
        fudge_fac=0,
        rescale=False,
        rescale_memory=50,
        range_fac=0.5,
        save_gif=True,
        name='',
        **y):
    import shutil
    from copy import deepcopy
    if os.path.exists(odir): shutil.rmtree(odir)
    if not os.path.exists(odir): os.makedirs(odir)

    # overall pos
    cnet = CharacterNetwork(is_directed=False)
    cnet.parse_dynamic(url,bad_nodes=set(bad_nodes))
    g=cnet.df2nx(t_min=None,t_max=None)#,i_max=i_max)
#     print(g.nodes(),'!!!!!')

    pos=dict(layout_graph(g,pos=None,pos_by=pos_by,**y).items())
#     pos={}
#     print(pos.keys(),'??')

    if pos_by=='geo':
        min_x,max_x = cnet.df_edges.merc_x.min(),cnet.df_edges.merc_x.max()
        min_y,max_y = cnet.df_edges.merc_y.min(),cnet.df_edges.merc_y.max()
        std_x,std_y=list(pd.DataFrame(pos).T.std())
        fudge_x,fudge_y = std_x*fudge_fac, std_y*fudge_fac
    else:
        min_x=min(xy[0] for xy in pos.values())
        max_x=max(xy[0] for xy in pos.values())
        min_y=min(xy[1] for xy in pos.values())
        max_y=max(xy[1] for xy in pos.values())
        std_x,std_y,fudge_x,fudge_y=0,0,0,0
#     min_x-=abs(fudge_x)
#     max_x+=abs(fudge_x)
#     min_y-=abs(fudge_y)
#     max_y+=abs(fudge_y)
    # return pos
    
#     print(min_x,max_x,min_y,max_y)
    
    timesteps=sorted(list(set(cnet.df_edges.i)))

    size_max_val = cnet.df_nodes[size_by].max() if size_by else None
    weight_max_val = max(d.get('weight',1) for a,b,d in g.edges(data=True))

    # for t in tqdm(timesteps,f'Saving graph images to {odir}'):
    objs = []

    last_kwds={}
    for i,row in tqdm(cnet.df_edges.sort_values('i').iterrows(),total=len(cnet.df_edges),desc=f'Generating graphs over time'):
#     for i,row in cnet.df_edges.iterrows():#,total=len(cnet.df_edges),desc=f'Generating graphs over time'):
        # title and fn
        title=f'Interaction #{str(i+1).zfill(4)}: {row.source} to {row.target}'
        if name: title=name.upper()+': '+title
        if row.get('name'): title+=f' ({row["name"]})'
        if row.get('setting'): title+=f', in {row.setting}'
        if row.get('id'): title+=f'[t={row.id}]'
        ofn=os.path.join(odir,f'graph.t_{str(i).zfill(4)}.png')
        
        # get graph        
        g=deepcopy(cnet.df2nx(i_max=i))#.copy()
        if repos:
            #print('1',pos)
            posnow=dict(layout_graph(g,pos=pos,pos_by=pos_by,fudge_fac=fudge_fac,**y).items())
            pos=dict(pos.items())
            for k,v in posnow.items(): pos[k]=v
            #print('2',pos)
            
        obj_kwds = dict(
            save_to=ofn,
            title=title,
            pos=dict(pos.items())
        )
            
        if rescale and pos_by=='geo':
            dfr=cnet.df_edges.iloc[:i+1] if i<rescale_memory else cnet.df_edges.iloc[i-rescale_memory:i+1]
            obj_kwds['min_x'],obj_kwds['max_x'] = dfr.merc_x.min(),dfr.merc_x.max()
            obj_kwds['min_y'],obj_kwds['max_y'] = dfr.merc_y.min(),dfr.merc_y.max()
#             print(i,len(dfr))
#             print(obj_kwds['min_x'],obj_kwds['max_x'])
#             print(obj_kwds['min_y'],obj_kwds['max_y'])

            if any([np.isnan(x) for k,x in obj_kwds.items() if k.startswith('min_') or k.startswith('max_')]):
#                 print('reset')
                obj_kwds['min_x'],obj_kwds['max_x']=min_x,max_x
                obj_kwds['min_y'],obj_kwds['max_y']=min_y,max_y

            
            if obj_kwds['min_x']==obj_kwds['max_x']:
#                 xj=std_x/2 if not fudge_x else (fudge_x*range_fac)
                xj=std_x*range_fac
                obj_kwds['min_x']-=xj
                obj_kwds['max_x']+=xj
            if obj_kwds['min_y']==obj_kwds['max_y']:
                yj=std_y*range_fac
                obj_kwds['min_y']-=yj
                obj_kwds['max_y']+=yj
            

                #                 obj_kwds['min_x']=last_kwds

                
#             print(obj_kwds['min_x'],obj_kwds['max_x'])
#             print(obj_kwds['min_y'],obj_kwds['max_y'])
#             print()            
    
        objs += [(g,obj_kwds)]

    pmap(
        do_save_nets,
        #random.sample(objs,i_max) if i_max else objs,
        objs[:i_max],
        kwargs=dict(
            show_plot=False,
            pos_by=pos_by,
            size_by=size_by,
            size_max_val=size_max_val,
            weight_max_val=weight_max_val,
            color_line_by='last_edge',
            color_by='last_node',
            min_x=min_x,
            min_y=min_y,
            max_x=max_x,
            max_y=max_y,
            range_fac=range_fac,
            **y
        ),
        desc=f'Saving graph images to {odir}',
        num_proc=num_proc,
        progress=True
    )

    # make gif?
    if save_gif:
        # return make_gif_from_folder(odir,fps=fps)
        return make_vid_from_folder(odir,fps=fps)






def combo_imgs(folder1, folder2, ofolder):
    from PIL import Image

    # fns
    fns1=sorted([x for x in os.listdir(folder1) if x.endswith('.png')])
    fns2=sorted([x for x in os.listdir(folder2) if x.endswith('.png')])
    
    for fn1,fn2 in tqdm(list(zip(fns1,fns2)),desc='Combining images'):
        images = []
        images+=[Image.open(os.path.join(folder1,fn1))]
        images+=[Image.open(os.path.join(folder2,fn2))]
        
        widths, heights = zip(*(i.size for i in images))

        total_width = sum(widths)
        max_height = max(heights)

        new_im = Image.new('RGB', (total_width, max_height))

        x_offset = 0
        for im in images:
            new_im.paste(im, (x_offset,0))
            x_offset += im.size[0]

        if not os.path.exists(ofolder): os.makedirs(ofolder)
        new_im.save(os.path.join(ofolder,fn1))


















##############























def test():
    import lltk
    C = lltk.load('Chadwyck')
    texts = [t for t in C.texts() if 'Austen' in t.author and 'Emma' in t.title]
    charnet = CharacterNetwork(texts)
    
    speakers = charnet.entities()
    print(speakers.most_common(100))
    #  dict_keys(['index', 'paragraph', 'parse', 'basicDependencies', 'enhancedDependencies', 'enhancedPlusPlusDependencies', 'entitymentions', 'tokens'])
    # print([os.path.exists(x) for x in charnet.paths_nlp])



