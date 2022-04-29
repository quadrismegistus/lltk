from __future__ import absolute_import
from __future__ import print_function
TABLE_NOW='texts'
TABLE_NEXT='texts2'
ADDR_SEP='|'


GLOBAL_DB=None
def get_global_db():
	global GLOBAL_DB
	if GLOBAL_DB is None:
		






def make_metadata_db(dbname='lltk',tablename=TABLE_NEXT, buffer_size=1000):
	import pymongo
	from pymongo import MongoClient

	client = MongoClient()
	db = client[dbname]
	table = db[tablename]
	print('>> removing')
	table.drop()

	from .. import BaseCorpus
	from corpus import corpora

	print('>> creating index')
	table.create_index([('corpus', pymongo.ASCENDING)])
	table.create_index([('corpus', pymongo.ASCENDING), ('id', pymongo.ASCENDING)],unique=True)

	for corpus_name,corpus in corpora():
		print('>>',corpus,'...')
		#if corpus_name!='ChadwyckDrama': continue
		ild=[]
		for ti,text in enumerate(corpus.texts()):
			if not text.id: continue
			#odx={'corpus_textid':(corpus_name, text.id), 'corpus':corpus_name, 'id':text.id}
			odx={'corpus':corpus_name, 'id':text.id}
			for k,v in list(text.meta.items()):
				if not k: continue
				k=k.replace('.','_')
				if k=='_id': continue
				odx[k]=v
			#odx=dict(odx.items())
			if '_id' in odx: del odx['_id']

			#print corpus_name,text.id,ti,table.insert_one(dict(odx.items())).inserted_id
			# try:
			# 	table.insert_one(odx)
			# except pymongo.errors.DuplicateKeyError as e:
			# 	print "!!",e
			# 	print "!!",odx


			ild+=[odx]

			if len(ild)>=buffer_size:
				try:
					print(odx['corpus'],odx['id'],len(ild),ti,table.insert_many(ild).inserted_ids[:2],'...')
				except pymongo.errors.BulkWriteError as e:
					print("!!",e)
				ild=[]

		if ild:
			try:
				print(odx['corpus'],odx['id'],len(ild),ti,table.insert_many(ild).inserted_ids[:2],'...')
			except pymongo.errors.BulkWriteError as e:
				print("!!",e)


def get_text_meta(corpus,text_id,dbname='lltk',tablename=TABLE_NOW):
	from pymongo import MongoClient
	client = MongoClient()
	db = client[dbname]
	table = db[tablename]

	dx=table.find_one({'corpus':corpus, 'id':text_id})
	if dx and '_id' in dx: del dx['_id']
	return dx

def get_corpus_meta(corpus,dbname='lltk',tablename=TABLE_NOW):
	from pymongo import MongoClient
	client = MongoClient()
	db = client[dbname]
	table = db[tablename]

	for dx in table.find({'corpus':corpus}):
		if dx and '_id' in dx: del dx['_id']
		yield dx
	#for dx in ld: del dx['_id']
	#return ld


def get_table(dbname='lltk',tablename=TABLE_NOW):
	from pymongo import MongoClient
	client = MongoClient()
	db = client[dbname]
	table = db[tablename]
	return table


def make_mini_db(keys=['author','title','year','genre','medium'],extra_keys=[]): #keys=['corpus','id','author','title','year']):
	from lltk import tools
	from tqdm import tqdm #tqdm_notebook as tqdm

	dbtable=get_table()
	total=dbtable.count()

	def _writegen():
		for dx in get_tqdm(dbtable.find(),total=total,desc='>> saving tsv from mongo'):
			minidx=dict( [ (k,dx.get(k,'')) for k in keys+extra_keys ] )
			minidx['_addr']=str(dx.get('corpus','Corpus')) + ADDR_SEP + str(dx.get('id','ID'))
			yield minidx

	tools.writegen('data.lltk_mini_db.txt.gz', _writegen)

#def get_mini_db():




### ZODB

class ZDB(object):
    ### CLASS ATTRS
    conn = None
    root = None
    key_broken = '__Broken_state__'
    
    ### ATTR METHODS
    def __init__(self,path=None,open=True,**kwargs):
        if path: self.path = path
        ensure_dir_exists(self.path,fn=True)
        self.db = ZODB.DB(self.path)
        # self.conn = self.db.open()
        # self.root = self.conn.root()

    ## Convenience methods
    def __getitem__(self, key): return self.get(key)
    def __setitem__(self, key, value): self.set(key,value)

    # def __iter__(self): 
    #     with self.db.transaction() as conn: return iter(conn.root().items())
    # def __len__(self):
    #     with self.db.transaction() as conn: return len(conn.root())
    
    
    def get(self,k): 
        with self.db.transaction() as conn: return conn.root().get(k)
        # with ZConnection(self.db) as (conn,root): return root.get(k)
        # return self.root.get(k)

    def set(self,key,value):
        if key is None or value is None: return
        key=str(key)
        with self.db.transaction() as conn:
            root=conn.root()
            root[key]=value
            if log.verbose>0: log(f'DB["{key}"] = {value}')
        # with ZConnection(self.db) as (conn,root):
            oldvalue = root.get(key)
            #if value != oldvalue:
            # transaction.commit()
        
        
    @property
    def path_lock(self): return self.path+'.lock'
    @property
    def path(self):
        from lltk.imports import PATH_LLTK_ZODB
        return PATH_LLTK_ZODB




ZODB_OBJ=None
def get_zodb(force=False,**kwargs):
    global ZODB_OBJ
    if force or ZODB_OBJ is None:
        if ZODB_OBJ is not None: ZODB_OBJ.close()
        ZODB_OBJ = ZDB(**kwargs)
        # list(ZODB_OBJ.root.items())
        # list(ZODB_OBJ.root.items())
        # ZODB_OBJ.root
    return ZODB_OBJ





class ZDatabase():
    """ Provides a ZODB database context manager """

    def __init__(self, uri, **kwargs):
        self.storage = create_storage(uri)
        self.db = DB(self.storage, **kwargs)

    def __enter__(self):
        return self.db

    def __exit__(self, exc_type, exc_value, traceback):
        self.db.close()
        return False


class ZConnection():
    """ Provides a ZODB connection with auto-abort (default).
    Provides a tuple of connection and root object:
        with ZConnection(db) as (cx, root):
            root.one = "ok"
    ZConnection implements a connection context manager.
    Transaction context managers in contrast do auto-commit:
        a) with db.transaction() as connection, or
        b) with cx.transaction_manager as transaction, or
        c) with transaction.manager as transaction  (for the thread-local transaction manager)
    See also http://www.zodb.org/en/latest/guide/transactions-and-threading.html
    """
    def __init__(self, db, auto_commit=False, transaction_manager=None):
        self.db = db
        self.auto_commit = auto_commit
        self.transaction_manager = transaction_manager
        self.cx = None

    def __enter__(self):
        if self.transaction_manager:
            self.cx = self.db.open(self.transaction_manager)
        else:
            self.cx = self.db.open()
        return self.cx, self.cx.root()

    def __exit__(self, exc_type, exc_value, traceback):
        if self.auto_commit:
            self.cx.transaction_manager.commit()
        self.cx.close()
        return False


def create_storage(uri):
    """ supported URIs
    file://e:/workspaces/zeo/bots.fs
    zeo://localhost:8001
    e:/workspaces/zeo/bots.fs
    @see https://en.wikipedia.org/wiki/Uniform_Resource_Identifier
    """
    if uri.startswith("file://"):
        storage = FileStorage(uri[7:])
    elif uri.startswith("zeo://"):
        addr, port = uri[6:].split(":")
        # addr_ = addr.encode("ASCII")
        storage = ClientStorage((addr, int(port)))
    else:
        storage = FileStorage(uri)
    return storage


def database(uri):
    """ convenience function for single thread, return one connection from the pool """
    storage = create_storage(uri)
    db = DB(storage)
    return db


def connection(db):
    """ Convenience function for multi thread, returns
    connection, transaction manager and root
    """
    cx = db.open()
    return cx, cx.root()