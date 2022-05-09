from lltk.imports import *

def filter_graph(g,min_weight=None,remove_isolates=True,min_degree=2,**kwargs):
    if min_weight:
        for a,b,d in list(g.edges(data=True)):
            if d['weight']<=min_weight:
                g.remove_edge(a,b)
    if min_degree:
        degree=dict(g.degree()).items()
        for node,deg in degree:
            if deg<min_degree:
                g.remove_node(node)
    if remove_isolates:
        degree=dict(g.degree()).items()
        for node,deg in degree:
            if not deg:
                g.remove_node(node)
    return g






netstat_keys=['degree','degree_centrality','betweenness_centrality']

def netstat_nx(g, stats=netstat_keys):
    for stat_name in stats:
        stat_node2val=getattr(nx,stat_name)(g)
        for node,val in dict(stat_node2val).items():
            g.nodes[node][stat_name]=val
    return g

def rescale_weights(weights, min_size=1, max_size=7, max_val=None):
    max_val = weights.max() if not max_val else max_val
    weights = weights / max_val * max_size
    weights = weights.apply(lambda x: x if x>min_size else min_size)
    return weights

def draw_nx(
        g,
        pos=None,
        final_g=None,
        
        size_by='degree',
        weight_by='weight',
        color_by='color',
        color_default='black',
        
        min_weight_size=1,
        max_weight_size=5,
        max_weight=None,
        
        plot_width=20,
        plot_height=10,
        save_to='',
        show=True,
        **layout_opts,
        ):
    if not pos: pos = layout_graph_force(g if not final_g else final_g, **layout_opts)

    import matplotlib
    import matplotlib.pyplot as plt
    matplotlib.rc('figure', figsize=(plot_width, plot_height))
    
    
    # get stats
    if size_by in set(netstat_keys): netstat_nx(g)
    sizes = pd.Series([d.get(size_by,0) for a,d in g.nodes(data=True)])
    weights = pd.Series([d.get(weight_by,0) for a,b,d in g.edges(data=True)])
    edge_colors = [d.get(color_by,color_default) for a,b,d in g.edges(data=True)]
    

    fig, ax = plt.subplots()
    if not show: plt.close()

    nxfig=nx.draw_networkx(
        g,
        ax=ax,
        pos=pos,
        node_size=rescale_weights(sizes,min_size=1,max_size=1000),
        width=rescale_weights(weights,min_size=min_weight_size,max_size=max_weight_size,max_val=max_weight),
        edge_color=edge_colors
    )
    if save_to: fig.savefig(save_to)

    if show:
        plt.show()
        plt.close()



def draw_nx_dynamic(
        iter_g,
        pos=None,
        final_g=None,
        max_weight=None,
        fps=10,
        ofn='fig.dynamic_graph.mp4',
        odir_imgs=None,
        force=False,
        **kwargs):
    import moviepy.video.io.ImageSequenceClip
    from base64 import b64encode
    from IPython.display import HTML
    import matplotlib.pyplot as plt
    plt.ioff()
    if final_g is not None:
        if pos is None: pos = layout_graph_force(final_g)
        if max_weight is None: max_weight = max(d.get('weight',1) for a,b,d in final_g.edges(data=True))
    
    
    if not odir_imgs:
        tdir=tempfile.TemporaryDirectory()
        odir_imgs=tdir.name
    else:
        tdir=None
        if not os.path.exists(odir_imgs): os.makedirs(odir_imgs)

    # with tempfile.TemporaryDirectory() as odir:    
    ofn_l=[]
    posnow=None
    for gi,g in enumerate(iter_g):
        gi=g.t if hasattr(g,'t') and g.t else gi
        if type(gi) in {tuple,list}: gi='_'.join(str(gix) for gix in gi)
        ofn_png = os.path.join(odir_imgs,f'graph{gi}.png')
        posnow = pos if pos else layout_graph_force(
            g,
            pos=posnow,
            iterations=1
        )

        if force or not os.path.exists(ofn_png):
            draw_nx(
                g,
                pos=posnow,
                max_weight=max_weight,
                save_to=ofn_png,
                show=False,
                **kwargs)
        ofn_l.append(ofn_png)

    print(f'Making movie ({ofn}) [{len(ofn_l)})]...')
    
    clip = moviepy.video.io.ImageSequenceClip.ImageSequenceClip(ofn_l, fps=fps)
    clip.write_videofile(ofn,verbose=False,logger=None) #progress_bar=False)


    with open(ofn,'rb') as f: mp4=f.read()
    data_url = "data:video/mp4;base64," + b64encode(mp4).decode()
    htm=HTML("""
    <video width=666 controls>
        <source src="%s" type="video/mp4">
    </video>
    """ % data_url)

    if tdir is not None: tdir.cleanup()
    return ofn,htm









def draw_graph_pyvis(networkx_graph,notebook=True,output_filename='graph.html',show_buttons=True,only_physics_buttons=False):
    """
    This function accepts a networkx graph object,
    converts it to a pyvis network object preserving its node and edge attributes,
    and both returns and saves a dynamic network visualization.
    
    Valid node attributes include:
        "size", "value", "title", "x", "y", "label", "color".
        
        (For more info: https://pyvis.readthedocs.io/en/latest/documentation.html#pyvis.network.Network.add_node)
        
    Valid edge attributes include:
        "arrowStrikethrough", "hidden", "physics", "title", "value", "width"
        
        (For more info: https://pyvis.readthedocs.io/en/latest/documentation.html#pyvis.network.Network.add_edge)
        
    
    Args:
        networkx_graph: The graph to convert and display
        notebook: Display in Jupyter?
        output_filename: Where to save the converted network
        show_buttons: Show buttons in saved version of network?
        only_physics_buttons: Show only buttons controlling physics of network?
    """
    
    # import
    from pyvis import network as net
    
    # make a pyvis network
    pyvis_graph = net.Network(notebook=notebook)
    
    # for each node and its attributes in the networkx graph
    for node,node_attrs in networkx_graph.nodes(data=True):
        pyvis_graph.add_node(str(node),**node_attrs)
        
    # for each edge and its attributes in the networkx graph
    for source,target,edge_attrs in networkx_graph.edges(data=True):
        # if value/width not specified directly, and weight is specified, set 'value' to 'weight'
        if not 'value' in edge_attrs and not 'width' in edge_attrs and 'weight' in edge_attrs:
            # place at key 'value' the weight of the edge
            edge_attrs['value']=edge_attrs['weight']
        # add the edge
        pyvis_graph.add_edge(str(source),str(target),**edge_attrs)
        
    # turn buttons on
    if show_buttons:
        if only_physics_buttons:
            pyvis_graph.show_buttons(filter_=['physics'])
        else:
            pyvis_graph.show_buttons()
    
    # return and also save
    return pyvis_graph.show(output_filename)













import math,numpy as np
def merc(lat, lon):
    try:
        r_major = 6378137.000
        x = r_major * math.radians(lon)
        scale = x/lon
        y = 180.0/math.pi * math.log(math.tan(math.pi/4.0 + lat * (math.pi/180.0)/2.0)) * scale
        return (x, y)
    except AssertionError:
        return (np.nan,np.nan)



def layout_graph_force(g,pos=None,iterations=10000,**attrs):

    try:
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
        #print(type(pos), pos.get('ClarisasHarlowe') if type(pos)==dict else None)
        pos=dict(
            forceatlas2.forceatlas2_networkx_layout(
                g,
                pos=pos,
                iterations=iterations
            ).items()
        )
        # print('\nres',pos,'\n')
        return pos
    
    except (ImportError,ModuleNotFoundError,AttributeError) as e:
        try:
            return nx.nx_agraph.graphviz_layout(g,prog='neato')
        except (ImportError,ModuleNotFoundError) as e2:
            return nx.spring_layout(g,k=0.15, iterations=20)



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
    
    size_opt = min_size
    if size_by is not None:
        size_opt = '_size'
        data_l = X = np.array([d.get(size_by,0) for n,d in g.nodes(data=True)])
        maxval=size_max_val if size_max_val else X.max(axis=0)
        data_l_norm = (X - X.min(axis=0)) / (maxval - X.min(axis=0) + 1)
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
    if not weight_max_val: weight_max_val = max(d.get('weight',1) for a,b,d in g.edges(data=True))
    widths = [
        ((g.get_edge_data(a,b).get('weight',1) / weight_max_val) * max_weight) + min_weight
        for a, b in g.edges()
    ]
    network_graph.edge_renderer.data_source.data["line_width"] = widths
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
