from lltk.imports import *
from lltk.model import *

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.cqlengine import columns
from cassandra.cqlengine import connection
from datetime import datetime
from cassandra.cqlengine.management import sync_table,drop_table
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.connection import register_connection, set_default_connection

DEFAULT_KEYSPACE='dataspace' #os.environ.get('ASTRA_DB_KEYSPACE','dataspace_2020_05_09')
SERVERS = [
    '10.42.0.1',
    # '128.232.229.63',
    '185.143.45.10',
    '127.0.0.1',
]



def get_remote_conn(local=['127.0.0.1']):
    un = os.environ.get('ASTRA_CLIENT_ID')
    pw = os.environ.get('ASTRA_CLIENT_SECRET')
    path = PATH_SECURE_BUNDLE
    if un and pw and os.path.exists(path):
        print('creating remote cassandra session!')
        """Create and get a Cassandra session"""
        cloud_config= {
                'secure_connect_bundle': path,
        }
        auth_provider = PlainTextAuthProvider(un, pw)
        cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
        return cluster


_CDB_=None
def CDB(force=False):
    global _CDB_
    if force or _CDB_ is None:
        _CDB_ = Cassandra()
    return _CDB_


TEXTDOCKEYS = {'id','corpus','author','title','year'}
#first, define a model
class TextDoc(Model):
    __table_name__ = 'texts'
    __keyspace__ = DEFAULT_KEYSPACE

    id = columns.Text(primary_key=True, required=True)
    corpus = columns.Text(required=True, index=True)
    author = columns.Text(custom_index=True, max_length=255)
    title = columns.Text(custom_index=True, max_length=255)
    year = columns.Integer(index=True)
    rels = columns.Map(columns.Text, columns.Text)
    data = columns.Bytes()

    @property
    def t(self): return self.to_text()

    def to_text(self):
        data=deserialize(self.data)
        ometa=dict(
            id=self.id,
            _cache=False,
            **just_meta_no_id(data))
        return Text(ometa)
    
    def from_text(text):
        return CDB().set(text.addr, text._meta)

    

DEFAULT_DOCTYPE=TextDoc

class Cassandra(LLDBBase):
    def __init__(self,doctype=DEFAULT_DOCTYPE,keyspace=DEFAULT_KEYSPACE,*x,**y):
        self._fn=None
        self._db=None
        self._cachedb=None
        self._conn=None
        self._getfunc=None
        self.cluster = None
        self.session = None
        self.doctype = doctype
        self.keyspace = keyspace
        self.id = doctype.__name__
        self.open()

    def open(self):
        if self._db is None: self._db = self.connect()
        return self._db


    def connect(self):
        
        # self.cluster = get_remote_conn()
        self.cluster = Cluster(SERVERS)

        self.session = self.cluster.connect()
        register_connection(str(self.session), session=self.session)
        set_default_connection(str(self.session))
        self.session.execute('''CREATE KEYSPACE IF NOT EXISTS %s
            WITH REPLICATION = { 
            'class' : 'SimpleStrategy', 
            'replication_factor' : 1 
            };
        ''' % self.keyspace)
        # self.session.execute('USE %s' % self.keyspace)
        self.session.set_keyspace(self.keyspace)
        # connection.setup(SERVERS, self.keyspace, protocol_version=3)
        
        # @TODO REMOVE
        # drop_table(TextDoc)
        #

        
        sync_table(TextDoc)
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
        self.session.execute('''CREATE CUSTOM INDEX IF NOT EXISTS title_like ON texts(title) USING 'org.apache.cassandra.index.sasi.SASIIndex';''')
        return self.session


    @property
    def getfunc(self):
        if self._getfunc is None:
            self._getfunc = self.session.prepare(
                "SELECT * FROM texts WHERE id=?"# % self.id
            )
        return self._getfunc

    def to_dict(self,obj):
        try:
            d=dict(obj)
        except Exception as e:
            try:
                d={k:getattr(obj,k) for k in obj._fields}
            except Exception as e:
                log.error(e)
                return {}
        if not d.get('data'): 
            log.error(d)
            return {}
        bdata_enc = d.get('data')
        C=Corpus(d.get('corpus'))
        try:
            bdata = C.decrypt(bdata_enc)
            return deserialize(bdata)
        except Exception as e:
            log.error(f'!! {e} !!')
            return {}
            
    def get(self,id=None,**meta):
        if id:
            for x in self.session.execute(self.getfunc, [id]):
                return self.to_dict(x)
            return
        return [self.to_dict(x) for x in self.doctype.objects.filter(**meta).all()]
    where = get
    
    def search_x(self,_keys=TEXTDOCKEYS,_keys_like={'author','title'},**meta):
        o = {}
        for k,v in meta.items():
            if k in _keys:
                o[k]=v
        return [self.to_dict(x) for x in self.doctype.objects.filter(**o)]


    def search(self,_keys=TEXTDOCKEYS,_keys_like={'author','title'},**meta):
        # o = {}
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
                    q=self.doctype.objects.filter(**{cond:opt})
                else:
                    q=q.filter(**{cond:opt})
            o=q
        else:
            o=self.doctype.objects.all()

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
        return [self.to_dict(x) for x in o]



        
    def set(self,id,_d={},**meta):
        # serialize whole lltk input
        ometa=safebool({**_d, **meta})
        bdata=serialize(ometa)

        # decide db input from lltk input
        dbd={}
        au=str(get_prop_ish(ometa,'author',default=''))
        ti=str(get_prop_ish(ometa,'title',default=''))
        rels=ometa.get('_rels',{})
        dbd['id']=id
        dbd['corpus']=to_corpus_and_id(id)[0]
        dbd['author']=au[:100] if au else ''
        dbd['title']=ti[:100] if ti else ''
        dbd['year']=int(get_year(ometa))
        if rels: dbd['rels']=rels

        # encrypt!
        C = Corpus(dbd['corpus'])
        try:
            bdata_enc = C.encrypt(bdata)
            dbd['data']=bdata_enc
            otype=self.doctype
            return otype.create(**dbd)
        except Exception as e:
            log.error(f'!! {e} !!')
            return None
        

    def insert(self,*lt,col_id=COL_ID,batch_size=10):
        for id,d in get_tqdm(lt,desc='.... Syncing database'): self.set(id,d)
        self.log('done')

    def clear(self):
        drop_table(TextDoc)
        self._db = None
        self.open()