from __future__ import absolute_import
from __future__ import print_function
from lltk.model import Model,NullModel
import networkx as nx
import numpy as np
from lltk import tools
import six
from six.moves import zip

# Semantic Networks

SEMANTIC_NETWORK_GOOG_URLS={}
SEMANTIC_NETWORK_GOOG_URLS['ECCO']='https://docs.google.com/spreadsheets/d/1jGMgfUfXP22mwhmIqznYhcVN5xWLr-yq-M6QJW0EhCA/pubhtml'
SEMANTIC_NETWORK_GOOG_URLS['ECCO.by_period.10years']='https://docs.google.com/spreadsheets/d/e/2PACX-1vSONgEG9ITNpYDG54bckoe4Vc8DhMHNqjXDSI9Rmw3hwgb5XZvhhM4RYK83mmlwTbdt1Dxq-YO0Z4rK/pubhtml'

class SemanticNetwork(Model):
	def __init__(self, model=None):
		self.model=model if model is not None else NullModel
		self.g=None

	@property
	def fn(self):
		name=['semantic_network',self.model.name,'graphml']
		return '.'.join([x for x in name if x])

	def load(self,giant_component=True):
		self.g=nx.read_graphml(self.fn)

	def to_giant_component(self):
		self.g=get_giant_component(self.g)

	def gen(self,words=[],words_ld=[],cosine_cut=None,min_num_connected_factor=None,allow_other_words=False,num_nearest_neighbors=False,save=False,k_core=None,giant_component=True, num_edges=None, num_edge_factor=2,wordkey='word'):
		"""
		- Words selected using self.model.mfw() [opts set by self.model.mfw_d] unless manually supplied (words)
		- Draw a network between all words meeting the conditions:
		-- minimum cosine similarity between words A and B (cosine_cut=0.7)
		-- whether to allow other words (allow_other_words=True) besides those in the MFW
		-- if num_edge_factor=N then draw edges for [num_nodes]*N
		"""

		#words = set(self.model.mfw()) if not words else set(words)
		words_ld=self.model.mfw(return_ld=True) if not words_ld else words_ld
		words = set(d[wordkey] for d in words_ld) if not words else set(words)
		word2d=tools.ld2dd(words_ld,wordkey)

		"""
		# UHHH WHAT WAS THIS ??

		if allow_other_words:
			syn_ld=self.model.synonyms(words,save=False,cosine_cut=cosine_cut)
			for dx in syn_ld:
				for syn in dx['synonyms']:
					if not syn in words:
						words|={syn}
		"""



		g=nx.Graph()
		num_words=len(words)
		print('>> generating graph for',num_words,'words...')
		for w in words: g.add_node(w)

		if num_nearest_neighbors:
			print('>> generating by using num_nearest_neighbors =',num_nearest_neighbors)
			for word,neighbors in self.model.nearest_neighbors(words,n=num_nearest_neighbors,allow_other_words=allow_other_words):
				for neighbor,val in neighbors:
					g.add_edge(word,neighbor,weight=float(val))
		else:
			if num_edge_factor:
				print('>> generating by using num_edge_factor =',num_edge_factor)
				num_edges=num_words * num_edge_factor

			print('>> gathering top connections...')
			gen=self.model.top_connections(words,n=num_edges if num_edges else None)
			print('>> done.')
			for i,(a,b,w) in enumerate(gen):
				if cosine_cut:
					if w<cosine_cut:
						break
				g.add_edge(a,b,weight=float(w))

				if min_num_connected_factor:
					num_in_biggest_component = len(sorted(nx.connected_components(g), key = len, reverse=True)[0])
					print('>>',i,num_in_biggest_component,a,b,w)
					if num_in_biggest_component >= num_words * min_num_connected_factor: break


		if k_core:
			g=nx.k_core(g,k_core)

		if giant_component:
			print('>> reducing to giant component...')
			g=sorted(nx.connected_component_subgraphs(g), key = len, reverse=True)[0]
		#	g=nx.connected_component_subgraphs(g)[0]

		## add attrs
		for word in g.nodes():
			for k,v in list(word2d.get(word,{}).items()):
				if type(v) in [int,str,float,six.text_type]:
					g.node[word][k]=v
			if 'pos' in g.node[word]:
				g.node[word]['pos_simple']=g.node[word]['pos'][0]

		self.g=g
		if save: self.save()

	def gen_from_dist(self,dist,dist_words,num_edge_factor=2,cosine_cut=None,save=False,k_core=None,giant_component=True):
		"""
		- Words selected using self.model.mfw() [opts set by self.model.mfw_d] unless manually supplied (words)
		- Draw a network between all words meeting the conditions:
		-- minimum cosine similarity between words A and B (cosine_cut=0.7)
		-- whether to allow other words (allow_other_words=True) besides those in the MFW
		-- if num_edge_factor=N then draw edges for [num_nodes]*N
		"""

		def flatten(dist,dist_words):
			l=[]
			for i1,w1 in enumerate(dist_words):
				for i2,w2 in enumerate(dist_words):
					if i1>=i2: continue
					this_dist = dist[i1][i2]
					if not this_dist or np.isnan(this_dist): continue
					l+=[this_dist]
			return l

		# Find value to cut at
		N = len(dist_words)
		if num_edge_factor:
			Ne = N * num_edge_factor
			vals = flatten(dist,dist_words)
			vals.sort()
			dist_cut = vals[Ne]
			print(N,Ne,dist_cut)
		elif cosine_cut:
			dist_cut = 1-cosine_cut
		else:
			dist_cut = None

		# Make graph
		g=nx.Graph()
		g.add_nodes_from(dist_words)
		for i1,w1 in enumerate(dist_words):
			for i2,w2 in enumerate(dist_words):
				if i1>=i2: continue
				this_dist = dist[i1][i2]
				if not dist_cut or this_dist < dist_cut:
					g.add_edge(w1,w2,weight=float(1-this_dist))

		# Postprocess and save
		if k_core: g=nx.k_core(g,k_core)
		if giant_component: g=sorted(nx.connected_component_subgraphs(g), key = len, reverse=True)[0]
		self.g=g
		if save: self.save()

	def save(self,store_attrs=False):
		import networkx as nx
		g=self.g
		print('>> saving graph of',g.order(),'nodes and',g.size(),'edges ...')
		if store_attrs:
			for node,noded in list(self.node2d.items()):
				if g.has_node(node):
					for k,v in list(noded.items()):
						g.node[node][k]=v
		nx.write_graphml(g,self.fn)

	def gen_edge_betweenness(self):
		for (a,b),v in list(nx.edge_betweenness_centrality(self.g,weight='weight').items()):
			self.g.edge[a][b]['edge_betweenness_centrality']=v

	def analyze(self,save=True,wordkey='word'):
		net=self.g
		print('>> analyzing graph of',net.order(),'nodes and',net.size(),'edges ...')
		statd = {}
		statd['centrality_degree']=nx.degree_centrality(net)
		statd['centrality_information']=nx.current_flow_closeness_centrality(net)
		statd['centrality_closeness']=nx.closeness_centrality(net)
		statd['centrality_betweenness']=nx.betweenness_centrality(net)
		statd['centrality_betweenness_weighted']=nx.betweenness_centrality(net,weight='weight')
		statd['centrality_eigenvector']=nx.eigenvector_centrality(net)
		statd['centrality_degree']=nx.degree_centrality(net)
		statd['clustering_coefficient']=nx.clustering(net)
		statd['eccentricity']=nx.eccentricity(net)
		print('>> done with analysis.')

		old=[]
		for node in net.nodes():
			dx={'model':self.model.name, wordkey:node, 'neighbors':', '.join(sorted(list(nx.all_neighbors(net,node))))}
			#for k,v in self.node2d.get(node,{}).items(): dx[k]=v
			for statname,node2stat in list(statd.items()):
				dx[statname]=node2stat[node]
			old+=[dx]

		if save: pytxt.write2(self.fn.replace('.graphml','.analysis2.txt'), old)
		return old

	def modularity(self,save=True,num_words_to_display=5, stats=['centrality_betweenness_weighted']):
		#stats=['centrality_betweenness','centrality_degree','clustering_coefficient']
		nw=num_words_to_display
		import community,pystats
		#g=get_giant_component(self.g)
		g=self.g
		partition = community.best_partition(g)

		ld=self.analyze(save=False)
		for k in stats:
			vals = [d[k] for d in ld]
			z = pystats.zfy(vals)
			for i,d in enumerate(ld):
				d[k+'_z']=z[i]

		zstat2node2val=dict((stat,{}) for stat in stats)
		stat2node2val=dict((stat,{}) for stat in stats)
		for d in ld:
			node=d['word']
			d['partition_id']=partition[node]
			for k in stats:
				stat2node2val[k][node]=d[k]
				zstat2node2val[k][node]=d[k+'_z']

		old=[]
		for partition_i,(partition_id,pld) in enumerate(sorted(list(pytxt.ld2dld(ld,'partition_id').items()),key=lambda _tup: len(_tup[1]), reverse=True)):


			odx={}
			odx['partition_rank']=partition_i+1
			odx['partition_id']=partition_id
			odx['partition_size']=len(pld)


			nodes_within_part = [d['word'] for d in pld]
			nodes_within_part_set = set(nodes_within_part)
			degree_within_part = [g.degree(node) for node in nodes_within_part]
			num_edges_within_part=[len([x for x in list(self.g.edge[node].keys()) if x in nodes_within_part_set]) for node in nodes_within_part]
			num_edges_without_part=[len([x for x in list(self.g.edge[node].keys()) if not x in nodes_within_part_set]) for node in nodes_within_part]
			num_edges_diff = [x-y for x,y in zip(num_edges_within_part,num_edges_without_part)]
			#odx['words_by_degree_within_cluster'] = ', '.join([w+' ('+str(e)+')' for e,w in sorted(zip(num_edges_within_part,nodes_within_part),reverse=True)[:nw]])

			odx['words_by_hubness'] = ', '.join([w+' ('+str(e)+')' for e,w in sorted(zip(num_edges_diff,nodes_within_part),reverse=True)[:nw]])
			#odx['words_by_degree_without_cluster'] = ', '.join([w+' ('+str(e)+')' for e,w in sorted(zip(num_edges_without_part,nodes_within_part),reverse=True)[:nw]])
			odx['words'] = ', '.join([w for e,w in sorted(zip(degree_within_part,nodes_within_part),reverse=True)])

			for k in stats:
				odx['words_by_'+k]=', '.join([d['word']+' ('+str(round(stat2node2val[k][d['word']]*100,1))+'%)' for d in sorted(pld,key=lambda _d: -_d[k])[:nw]])
			old+=[odx]

		if save:
			pytxt.write2(self.fn.replace('.graphml','.modularity3.txt'), old)
			pytxt.write2(self.fn.replace('.graphml','.analysis-with-modularity3.txt'), ld)
		return old

	def blockmodel(self,save=True):
		goog_url=SEMANTIC_NETWORK_GOOG_URLS[self.model.name]
		cluster_ld = pytxt.tsv2ld(goog_url)
		cluster_id2d=pytxt.ld2dd(cluster_ld,'ID')
		node_ld = pytxt.tsv2ld(self.fn.replace('.graphml','.analysis-with-modularity.txt'))
		id2ld =pytxt.ld2dld(node_ld,'partition_id')
		part_list=[]
		for p_id,pld in sorted(id2ld.items()):
			nodes = [d['word'] for d in pld]
			part_list+=[nodes]

		M=nx.blockmodel(self.g,part_list)
		M2=nx.Graph()
		node2name={}
		for node in M.nodes():
			cluster = cluster_id2d[str(node)]
			name = cluster['NAME']
			node2name[node]=name
			M2.add_node(name,**cluster)

		for a,b in M.edges():
			M2.add_edge(node2name[a], node2name[b])



		if save: nx.write_graphml(M2, self.fn.replace('.graphml','.blockmodel.graphml'))
		return M2

	def blockmodel_betweenness(self,n_between=3,save=True):
		M = self.blockmodel(save=False)
		node_ld = pytxt.tsv2ld(self.fn.replace('.graphml','.analysis-with-modularity.txt'),keymap={'partition_id':str})
		id2ld =pytxt.ld2dld(node_ld,'partition_id')
		word2d=pytxt.ld2dd(node_ld,'word')

		M2 = nx.Graph()

		g_nodes = self.g.nodes()

		for cluster in M.nodes():
			M2.add_node(cluster,**M.node[cluster])
			M2.node[cluster]['node_type']='Cluster'
			M2.node[cluster]['Label']=cluster

		num_cedges = M.size()
		word2bridges={}
		edge2bridges={}
		cedge2numpaths={}
		for i,(cluster1,cluster2) in enumerate(sorted(M.edges())):
			print('>>',i+1,num_cedges,cluster1,cluster2,'...')
			cid1,cid2=M.node[cluster1]['ID'],M.node[cluster2]['ID']
			words_cluster1 = [d['word'] for d in id2ld[cid1]]
			words_cluster2 = [d['word'] for d in id2ld[cid2]]
			if not (cluster1,cluster2) in cedge2numpaths: cedge2numpaths[(cluster1,cluster2)]=0.0

			betweens=[]
			betweens_edges=[]
			path_lens=[]
			for w1,w2 in pystats.product(words_cluster1,words_cluster2):
				cedge2numpaths[(cluster1,cluster2)]+=1.0
				#print w1,w2
				path = nx.dijkstra_path(self.g,w1,w2)
				path_lens+=[len(path)]
				#print path
				#continue
				betweens+=path[1:-1]
				betweens_edges+=pytxt.bigrams(path)
				#print

			for w in betweens:
				if not w in word2bridges: word2bridges[w]=[]
				word2bridges[w]+=[(cluster1,cluster2)]

			for e in betweens_edges:
				if not e in edge2bridges: edge2bridges[e]=[]
				edge2bridges[e]+=[(cluster1,cluster2)]

			#return

			betweend=pytxt.toks2freq(betweens)
			betweend_edges=pytxt.toks2freq(betweens_edges)
			top_between = [k for k,v in sorted(list(betweend.items()),key=lambda _tup: -_tup[1])[:n_between]]
			top_between_edges = [k[0]+'-'+k[1] for k,v in sorted(list(betweend_edges.items()),key=lambda _tup: -_tup[1])[:n_between]]
			print(cluster1,cluster2,top_between)

			"""
			for word in top_between:
				print '>>',word

				#if not M2.has_node(word):
				#	M2.add_node(word,**word2d[word])
				#	M2.node[word]['node_type']='Word'
				#	M2.node[word]['Label']=word

				wordnode = '{0} [{1}-{2}]'.format(word,cid1,cid2)
				M2.add_node(wordnode,**word2d[word])
				M2.node[wordnode]['node_type']='Word'
				M2.node[wordnode]['Label']=word
				M2.add_edge(cluster1,wordnode)
				M2.add_edge(wordnode,cluster2)
			"""

			avg_path_length = pystats.mean(path_lens)
			M2.add_edge(cluster1,cluster2,weight=1/avg_path_length,Label=', '.join(top_between), LabelEdges=', '.join(top_between_edges))

			if i>3: break
			#break


		## Collect global edge/node stats
		ld_words,ld_edges=[],[]
		word_numbridges = float(sum([len(v) for v in list(word2bridges.values())]))
		edge_numbridges = float(sum([len(v) for v in list(edge2bridges.values())]))
		for i,word in enumerate(sorted(word2bridges,key=lambda _k: len(word2bridges[_k]),reverse=True)):
			odx={'word':word, 'rank':i+1}
			odx['num_paths']=len(word2bridges[word])
			odx['num_paths_norm']=len(word2bridges[word]) / word_numbridges

			bridged = pytxt.toks2freq(word2bridges[word])
			for cedge in bridged:
				bridged[cedge] = bridged[cedge] / cedge2numpaths[cedge]

			odx['bridges']=''
			for bridge,count in sorted(list(bridged.items()),key=lambda _tup: -_tup[1]):
				odx['bridges']+='{0}-{1}: {2}%\n'.format(bridge[0],bridge[1],round(count*100,1))
			ld_words+=[odx]

		for i,edge in enumerate(sorted(edge2bridges,key=lambda _k: len(edge2bridges[_k]),reverse=True)):
			odx={'edge':'-'.join(edge), 'rank':i+1}
			odx['num_paths']=len(edge2bridges[edge])
			odx['num_paths_norm']=len(edge2bridges[edge]) / edge_numbridges

			bridged = pytxt.toks2freq(edge2bridges[edge])
			odx['bridges']=''
			for bridge,count in sorted(list(bridged.items()),key=lambda _tup: -_tup[1]):
				odx['bridges']+='{0}-{1}: {2}\n'.format(bridge[0],bridge[1],count)
			ld_edges+=[odx]



		if save:
			pytxt.write2(self.fn.replace('.graphml','.blockmodel_betweenness.bridges_words.xls'), ld_words)
			pytxt.write2(self.fn.replace('.graphml','.blockmodel_betweenness.bridges_edges.xls'), ld_edges)
			nx.write_graphml(M2, self.fn.replace('.graphml','.blockmodel_betweenness.graphml'))
		return M2

	def remove_clusters_from_graph(self,cluster_ids=['17','9','21']):
		print('>> removing nodes from graph belonging to clusters:',', '.join(cluster_ids))
		i=0
		for d in self.node_ld:
			if d['partition_id'] in cluster_ids:
				if self.g.has_node(d['word']):
					self.g.remove_node(d['word'])
					i+=1
		self.g=sorted(nx.connected_component_subgraphs(self.g), key = len, reverse=True)[0]
		print('>> removed',i,'nodes from graph')

	def betweenness(self,return_paths=False):
		now=time.time()
		print('>> starting to find all shortest paths...')
		paths=nx.all_pairs_dijkstra_path(self.g)
		if return_paths: return paths
		nownow=time.time()
		print('>> finished finding shortest paths in',round(nownow-now,1),'seconds')

		node2d=pytxt.ld2dd(self.node_ld,'word')
		cluster2d=pytxt.ld2dd(self.cluster_ld,'ID')

		cedge2paths={}
		for source in paths:
			cluster_source=node2d[source]['partition_id']
			for target in paths[source]:
				if len(paths[source][target])<3: continue
				cluster_target=node2d[target]['partition_id']
				#
				cluster_edge = (cluster_source, cluster_target)
				if not cluster_edge in cedge2paths: cedge2paths[cluster_edge]=[]
				cedge2paths[cluster_edge]+=[paths[source][target]]

		word2graph={}
		def writegen():
			for i,((c1,c2),paths) in enumerate(sorted(cedge2paths.items())):
				cluster_edge = ' - '.join([cluster2d[c1]['NAME'],cluster2d[c2]['NAME']])
				dx={'cluster1':cluster2d[c1]['NAME'], 'cluster2':cluster2d[c2]['NAME'], 'cluster_edge':cluster_edge}
				x2count={'words':{}, 'edges':{}}
				for path in paths:
					for word in path[1:-1]:
						if not word in x2count['words']: x2count['words'][word]=0
						if not word in word2graph: word2graph[word]=nx.Graph()
						G=word2graph[word]
						x2count['words'][word]+=1
						for a,b in pytxt.bigrams(path):
							if G.has_edge(a,b):
								G[a][b]['weight']+=1
							else:
								G.add_edge(a,b,weight=1)
					for edge in pytxt.bigrams(path):
						edgestr=' - '.join(edge)
						if not edgestr in x2count['edges']: x2count['edges'][edgestr]=0
						x2count['edges'][edgestr]+=1

				for x in x2count:
					xsum = float(sum(x2count[x].values()))
					for k in x2count[x]:
						v=x2count[x].get(k,0)
						odx=dict(list(dx.items()))
						odx['unit_type']=x
						odx['unit']=k
						odx['count']=v
						odx['perc']=round(v/xsum*100,2)
						yield odx

		pytxt.writegen('semnet.betweenness.txt', writegen)
		for word,graph in list(word2graph.items()):
			for node in graph.nodes():
				for k,v in list(node2d[node].items()):
					graph.node[node][k]=v
				for k,v in list(cluster2d[node2d[node]['partition_id']].items()):
					graph.node[node][k]=v
			nx.write_graphml(graph, 'semnet_by_betweenness_word/{1}.{0}.graphml'.format(str(graph.order()).zfill(4), word))




	@property
	def cluster_ld(self):
		if not hasattr(self,'_cluster_ld'):
			self._cluster_ld = [d for d in pytxt.tsv2ld(SEMANTIC_NETWORK_GOOG_URLS[self.model.name]) if d['ID']]
			for d in self._cluster_ld: d['ID']=str(int(d['ID']))
		return self._cluster_ld

	@property
	def node_ld(self,bad_keys={'words'}):
		if not hasattr(self,'_node_ld'):
			node_ld = pytxt.tsv2ld(self.fn.replace('.graphml','.analysis-with-modularity.txt'),keymap={'partition_id':str})
			cluster_ld = self.cluster_ld
			cluster_id2d=pytxt.ld2dd(cluster_ld,'ID')
			for d in node_ld:
				idx=str(int(d['partition_id']))
				for k,v in list(cluster_id2d[idx].items()):
					if k in bad_keys: continue
					d[k]=v
			self._node_ld=node_ld
		return self._node_ld

	@property
	def node2d(self):
		if not hasattr(self,'_node2d'):
			self._node2d=pytxt.ld2dd(self.node_ld,'word')
		return self._node2d



	def model_words(self,words=[],vectord={},cluster_vectors=[]):
		node_ld = self.node_ld
		node2d=pytxt.ld2dd(node_ld,'word')
		if not words: words = [d['word'] for d in node_ld]



		if cluster_vectors:
			id2cluster=pytxt.ld2dd(self.cluster_ld,'ID')
			cluster2nodeld=pytxt.ld2dld(node_ld,'partition_id')
			print(list(cluster2nodeld.keys()))
			for id1,id2 in cluster_vectors:
				vname = id2cluster[str(id2)]['NAME']+' <> '+id2cluster[str(id1)]['NAME']
				words1 = [d['word'] for d in cluster2nodeld[str(id1)]]
				words2 = [d['word'] for d in cluster2nodeld[str(id2)]]
				v = self.model.centroid(words1)-self.model.centroid(words2)
				vectord[vname]=v

		ld = self.model.model_words(words=words,save=False,vectord=vectord)
		for d in ld:
			w=d['word']
			if w in node2d:
				for k,v in list(node2d[w].items()):
					d[k]=v

		ofn=self.model.fn.replace('word2vec','data.word2vec.words').replace('.txt.gz','.txt')
		pytxt.write2(ofn, ld)





def do_semantic_network(model,k_core,giant_component=True):
	import networkx as nx
	print('>> generating network:',model.name,'...')

	net=model.semantic_network(save=True, k_core=k_core, giant_component=giant_component)
	print('>> analyzing graph of',net.order(),'nodes and',net.size(),'edges ...')
	statd = {}
	statd['betweenness_centrality']=nx.betweenness_centrality(net)
	#statd['eigenvector_centrality']=nx.eigenvector_centrality(net)
	statd['degree_centrality']=nx.degree_centrality(net)

	old=[]
	for node in net.nodes():
		dx={'model':model.name, 'word':node, 'neighbors':', '.join(sorted(list(nx.all_neighbors(net,node))))}
		for statname,node2stat in list(statd.items()):
			dx[statname]=node2stat[node]
		old+=[dx]

	model.unload()
	return old

def get_giant_component(g):
	return sorted(nx.connected_component_subgraphs(g), key = len, reverse=True)[0]
