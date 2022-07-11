SERVERS = [
    '128.232.229.63', # '10.42.0.1',
    '185.143.45.10',
    '74.208.251.91'
]

from lltk.imports import *
from lltk.model import *

import uuid
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.cqlengine import columns
from cassandra.cqlengine import connection
from datetime import datetime
from cassandra.cqlengine.management import sync_table,drop_table
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.connection import register_connection, set_default_connection

DEFAULT_KEYSPACE='dataspace7' #os.environ.get('ASTRA_DB_KEYSPACE','dataspace_2020_05_09')

_CDB_=None
def CDB(force=False):
    global _CDB_
    if force or _CDB_ is None:
        _CDB_ = Cassandra()
    return _CDB_



TEXTDOCKEYS = {'id','corpus','author','title','year','data'}
#first, define a model
class TextDoc(Model):
    __table_name__ = 'texts'
    __keyspace__ = DEFAULT_KEYSPACE

    id = columns.Text(primary_key=True, required=True)
    corpus = columns.Text(required=True, index=True)
    author = columns.Text(custom_index=True, max_length=255)
    title = columns.Text(custom_index=True, max_length=255)
    year = columns.Integer(index=True)
    data = columns.Map(columns.Text, columns.Text)

    @property
    def t(self): return self.to_text()

    def to_text(self):
        data=deserialize_map(self.data)
        ometa=dict(
            id=self.id,
            _cache=False,
            **just_meta_no_id(data))
        return Text(ometa)
    
    def from_text(text):
        return CDB().set(text.addr, text._meta)



# class TextRel(Model):
#     __table_name__ = 'textrel'
#     __keyspace__ = DEFAULT_KEYSPACE

#     # id = columns.UUID(primary_key=True, default=uuid.uuid4)
#     id = columns.Text(primary_key=True, required=True)
#     rel = columns.Text(required = True, index=True)
#     rel_type = columns.Text(index = True)
#     addr_u = columns.Text(required = True, index=True)
#     addr_v = columns.Text(required = True, index=True)
    
class TextRels(Model):
    __table_name__ = 'textrels'
    __keyspace__ = DEFAULT_KEYSPACE
    id = columns.Text(primary_key=True, required=True)
    rels = columns.Map(columns.Text, columns.Text)

class MinHashDoc(Model):
    __table_name__ = 'textrels'
    __keyspace__ = DEFAULT_KEYSPACE
    id = columns.Text(primary_key=True, required=True)
    data = columns.Bytes()

class QueryCache(Model):
    __table_name__ = 'qcache'
    __keyspace__ = DEFAULT_KEYSPACE
    id = columns.Text(primary_key=True, required=True)
    data = columns.Bytes()


DEFAULT_DOCTYPE=TextDoc

class Cassandra(LLDBBase):
    def __init__(self,doctype=DEFAULT_DOCTYPE,keyspace=DEFAULT_KEYSPACE,*x,**y):
        self._fn=None
        self._db=None
        self._G=None
        self._cachedb=None
        self._conn=None
        self._getfunc=None
        self._getfuncs=None
        self.cluster = None
        self.session = None
        self._doctype = doctype
        self.keyspace = keyspace
        self.id = doctype.__name__
        self._hellos=0
        self.open()

    def __getattr__(self,key): return getattribute(self,key)
    def __exit__(self,*x): return

    @property
    def G(self): 
        if self._G is None: self.open_graph()
        return self._G

    def open(self):
        if self._db is None: 
            try:
                self._db = self.connect()
                self.open_graph()
            except AssertionError as e:
                if log: log.debug(f'CANNOT CONNECT: {e}')
                if self._hellos<2: log.info('cannot connect. staying offline')
                self._db = None
        
        return self._db

    def ego_graph(self,node,radius=2):
        import networkx as nx
        nodes=set(self.G.nodes())
        if node not in nodes: return nx.Graph()
        return nx.ego_graph(self.G, node, radius=radius)

    def open_graph(self,force=False):
        if force or self._G is None:
            now=time.time()

            key=f'all_rels__{getweeknum()}'
            odb=self.cachedb('graph')
            res=odb.get(key) if not force else None
            if log: log(res)
            if res:
                g=self._G=res
                if log: log(f'loading match {g} in {round(time.time()-now,2)}s from local cache [{getweeknum()}]')
                return self._G
            else:        
                def __callback(rows):
                    g=nx.Graph()
                    for d in rows:
                        # print(d)
                        id1=d['id']
                        if IDSEP_START+TMP_CORPUS_ID+IDSEP in id1: continue
                        for id2,d2str in dict(d['rels']).items():
                            d2=ujson.loads(d2str)
                            # print([id1,id2,d2])
                            if IDSEP_START+TMP_CORPUS_ID+IDSEP in id2: continue
                            g.add_edge(id1,id2,**d2)
                        # stopx
                    if log: log(f'generated match {g} in {round(time.time()-now,2)}s')
                    self._G=g

                    # now save locally
                    log.info(f'caching locally to "{key}": {g}')
                    res=odb.set(key,g)
                    log.info(f'done = {res} ?')
                
                func=self.getfuncs.get('all_rels')
                promise = self.execute_async(func,[],callback=__callback)
                return promise


    def get_all_ids(self,force=False):
        if force or self._ids is None:
            now=time.time()

            key=f'all_ids__{getweeknum()}'
            odb=self.cachedb('graph')
            res=odb.get(key) if not force else None
            if res:
                self._ids=set(res)
                if log: log(f'loading {len(res)} from local cache [{getweeknum()}] in {round(time.time()-now,2)}s')
                return self._ids
            else:
                # check if i have ?
                try:
                    res = list(self.execute(self.getfunc_qcache, [key]))
                    log.info(len(res))
                except Exception as e:
                    log.error(e)
                    res = None

                if res:
                    log.info('got half way cache')
                    self._ids={d['id'] for d in res}
                else:
                    log.info('generating')
                    ids=set()
                    for rowd in get_tqdm(self.execute(self.getfunc_all_ids,[])):
                        if rowd.get('id'):
                            ids|={rowd['id']}
                    self._ids=ids

                    # cache for network?
                    now=time.time()                    
                    def callback(res): log.info(f'finished uploading cache to net in {round(time.time()-now,2)}s')
                    id = key
                    data = serialize(self._ids)
                    promise = self.execute_async(self.setfunc_qcache, [id,data], callback=callback)
                odb.set(key,self._ids)
            if log: log(f'generated id list (n={len(self._ids)}) in {round(time.time()-now,2)}s')
            return self._ids


    @property
    def getfuncs(self):
        if self._getfuncs is None:
            self._getfuncs={}
            self._getfuncs['rels_from_id']=self.session.prepare("SELECT * FROM textrels WHERE id=?")
            self._getfuncs['text_from_id']=self.session.prepare("SELECT * FROM texts WHERE id=?")
            self._getfuncs['text_ids_from_corpus']=self.session.prepare("SELECT id FROM texts WHERE corpus=?")
            self._getfuncs['all_rels']=self.session.prepare("SELECT * FROM textrels")
            # self._getfuncs['all_ids']=self.session.prepare("SELECT id FROM texts LIMIT 1000000 ALLOW FILTERING")
            # self._getfuncs['matches_from_id']=self.session.prepare("SELECT * FROM textrels WHERE id IN (?);")
        return self._getfuncs

    @property
    def getfunc_all_ids(self):
        if self._getfunc_all_ids is None:
            self._getfunc_all_ids=self.session.prepare("SELECT id FROM texts LIMIT 1000000 ALLOW FILTERING")
        return self._getfunc_all_ids

    @property
    def setfunc_match(self):
        if self._setfunc_match is None:
            self._setfunc_match=self.session.prepare("INSERT INTO textrels (id, rels) VALUES (?, ?)")
        return self._setfunc_match

    @property
    def setfunc_qcache(self):
        if self._setfunc_qcache is None:
            self._setfunc_qcache=self.session.prepare("INSERT INTO qcache (id, data) VALUES (?, ?)")
        return self._setfunc_qcache
    @property
    def getfunc_qcache(self):
        if self._getfunc_qcache is None:
            self._getfunc_qcache=self.session.prepare("SELECT * FROM qcache WHERE id=?")
        return self._getfunc_qcache
    
    @property
    def setfunc_text(self):
        if self._setfunc_text is None:
            self._setfunc_text=self.session.prepare("INSERT INTO texts (id,corpus,author,title,year,data) VALUES (?,?,?,?,?,?)")
        return self._setfunc_text


    def connect(self):
        # if log: log('connecting to cassandra db')
        if not self._hellos:
            log('connecting to cassandra db')
        self._hellos+=1
        self.cluster = Cluster(SERVERS)
        self.session = self.cluster.connect()
        register_connection(str(self.session), session=self.session)
        set_default_connection(str(self.session))
        self.session.execute('''CREATE KEYSPACE IF NOT EXISTS %s
            WITH REPLICATION = { 
            'class' : 'NetworkTopologyStrategy', 
            'replication_factor' : 3 
            };
        ''' % self.keyspace)
        self.session.set_keyspace(self.keyspace)

        # sync tables
        sync_table(TextDoc)
        sync_table(TextRels)
        sync_table(QueryCache)

        self.session.execute('''
            CREATE CUSTOM INDEX IF NOT EXISTS author_like ON texts(author) 
            USING 'org.apache.cassandra.index.sasi.SASIIndex' 
            WITH OPTIONS = {
                'mode': 'CONTAINS', 
                'analyzer_class': 'org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer', 
                'case_sensitive': 'false'
            };
        ''')
        self.session.execute('''
            CREATE CUSTOM INDEX IF NOT EXISTS title_like ON texts(title) 
            USING 'org.apache.cassandra.index.sasi.SASIIndex' 
            WITH OPTIONS = {
                'mode': 'CONTAINS', 
                'analyzer_class': 'org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer', 
                'case_sensitive': 'false'
            };
        ''')
        self.getfuncs # prime
        if log: log(self.getfuncs)
        # if log: log('connected to cassandra db')
        if log: log('connected')
        return self.session


    # @property
    # def getfunc(self):
    #     if self._getfunc is None:
    #         self._getfunc = self.session.prepare(
    #             "SELECT * FROM texts WHERE id=?"# % self.id
    #         )
    #     return self._getfunc



    def to_dict(self,obj):
        try:
            d=dict(obj)
        except Exception as e:
            try:
                d={k:getattr(obj,k) for k in obj._fields}
            except Exception as e:
                log.error(e)
                return {}
        
        if 'data' in d and d['data']: d['data']=dict(d['data'])
        d['rels']=dict(d['rels']) if 'rels' in d and d['rels'] else {}
        return d
    
            
    def get(self,id=None,default=None,**meta):
        if id:
            o=self._doctype.objects.filter(id=id).first()
            return o if o else default
        
        o=self.search(_keys_like={},**just_meta_no_id(meta))
        return o if o else default

    def execute(self,qstr,qvals=[],timeit=True,return_future=False):
        if not self.db: return
        now=time.time()
        
        if log: log(f'>>> Q: {qstr} ...')
        if not return_future:
            if qvals:
                res=self.db.execute(qstr,qvals if type(qvals)==list else [qvals])
            else:
                res=self.db.execute(qstr)
            if log:
                log(f'<<<< A: {type(res)} returned in {round(time.time()-now,2)}s')
        return list(res)

    def execute_async(self,qstr,qvals=None,timeit=True,callback=None,callback_error=None):
        if not self.db: return
        now=time.time()
        callback_success=callback
        
        if log: log(f'>>> Q: {qstr} ...')
        if qvals is not None:
            future=self.db.execute_async(qstr,qvals if type(qvals)==list else [qvals])
        else:
            future=self.db.execute_async(qstr)

        def _callback_success(rows):
            if log: log(f'<- sucess: {rows}')
        def _callback_error(exception):
            if log: log.error(exception)
        if callback_success or callback_error:
            future.add_callbacks(
                callback_success if callback_success else _callback_success,
                callback_error if callback_error else _callback_error,
            )
        if log: log(f'returning future in {round(time.time()-now,2)}s')
        return future
        
    
    where = get
    def search(self,au=None,ti=None,_keys=TEXTDOCKEYS,_keys_like={'author','title'},operator='AND',table='texts',return_promise=False,**meta):
        if au: meta['author']=au
        if ti: meta['title']=ti
        if log: log(f'? {meta}')
        conds = []
        extra_conds=OrderedSetDict()
        for k,v in meta.items():
            if k in _keys_like:
                words=[zeropunc(x) for x in v.split()]
                if len(words)>1:
                    for word in words[:-1]:
                        extra_conds[k]=('LIKE',word)
                
                conds.append(f"{k} LIKE '%{words[-1]}%'")
            else:
                condvalstr = f"'{v}'" if type(v)==str else f"{v}"
                conds.append(f"{k} = {condvalstr} ")
        condstr=f' {operator} '.join(conds)
        keystr=','.join(['id'] + list(extra_conds.keys()))
        keystr='*'
        
        qstr=f"SELECT {keystr} FROM {table} WHERE {condstr} ALLOW FILTERING;"
        if log: log(f'? {qstr}')


        future=self.execute_async(qstr) if return_promise else self.execute(qstr)
        # if log: log(f'execution res = {future}')
        return (future,extra_conds)




    def search_obj(self,au=None,ti=None,_keys=TEXTDOCKEYS,_keys_like={'author','title'},**meta):
        if au: meta['author']=au
        if ti: meta['title']=ti
        conds = OrderedSetDict()
        extra_conds=OrderedDict()
        for k,v in meta.items():
            if k in _keys_like:
                for word in v.split():
                    word=zeropunc(word)
                    conds[k+'__like']=f'%{word}%'
            elif k in _keys:
                conds[k]=v
        q=None
        if conds:
            for cond,opts in conds.items():
                if len(opts)>1:
                    for opt in opts[:-1]:
                        extra_conds[cond]=opt
                opt = opts[-1]
                if q is None:
                    q=self._doctype.objects.filter(**{cond:opt})
                else:
                    q=q.filter(**{cond:opt})
            o=list(q)
        else:
            o=list(self._doctype.objects.all())
        # print(f'got = {o}')

        if extra_conds:
            for cond,opts in conds.items():
                if cond.endswith('__like'):
                    key=cond.split('__like')[0]
                    for opt in opts:
                        opt=opt[1:-1]
                        if opt:
                            o = [obj for obj in o if opt.lower() in getattr(obj,key).lower()]
                else:
                    o = [obj for obj in o if getattr(obj,cond)==opt]
        
        O=[self.to_dict(x) for x in o]
        # if log: log(f'-> {O}')
        return O



        
    def set(self,id,_d={},**meta):
        # serialize whole lltk input
        ometa=safebool({**_d, **meta})

        # decide db input from lltk input
        dbd={}
        au=str(get_prop_ish(ometa,'author',default=''))
        ti=str(get_prop_ish(ometa,'title',default=''))
        rels=ometa.get('_rels',{})
        minhash=ometa.get('_minhash')
        dbd['id']=id
        dbd['corpus']=to_corpus_and_id(id)[0]
        dbd['author']=au[:100] if au else ''
        dbd['title']=ti[:100] if ti else ''
        dbd['year']=int(get_year(ometa))
        if rels and isinstance(rels,dict):
            dbd['rels']=ometa.pop('_rels')
        dbd['data']=serialize_map(just_meta_no_id(ometa))
        return self._doctype.create(**dbd)
        

    def insert(self,*lt,col_id=COL_ID,batch_size=10):
        for id,d in get_tqdm(lt,desc='.... Syncing database'): self.set(id,d)
        self.log('done')

    def clear(self):
        drop_table(TextDoc)
        self._db = None
        self.open()