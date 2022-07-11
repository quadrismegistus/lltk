from lltk.imports import *
from lltk.text.textlist import hellofunc

def run_match_by_title(*x,**y):
    print(f'run_match_by_title({x},{y})')
    # from lltk.model.orm import CDB
    # db=CDB(force=True)
    bag = match_by_title(*x,**y)#db=db,
    return bag
    #return [promise.result() for promise in bag]

def match_by_title(df,
        yn='',
        rel=MATCHRELNAME,
        rel_type='title',
        db=None,
        full=False,
        compare_by=DEFAULT_COMPAREBY,
        method_string='levenshtein',
        **kwargs):
    # get df
    # set up index
    import recordlinkage as rl
    indexer = rl.Index()
    indexer.block(left_on='author', right_on='author') if not full else indexer.full()
    # get candidates
    candidates = indexer.index(df, df)
    # set up comparison model
    c = rl.Compare()
    for k,v in compare_by.items():
        c.string(k,k,threshold=v,method=method_string) if v<1.0 else c.exact(k,k)
    res = c.compute(candidates, df, df)

    res.columns = [f'match_{k}' for k in compare_by]
    res['match_sum'] = res.sum(axis=1)
    res['match_rel'] = res['match_sum'] / len(compare_by)
    res['match'] = res['match_rel'] == 1
    res=res.reset_index()
    res = res[res.id_1 != res.id_2]
    res = res[res.match==True]

    # add matches
    osd=OrderedSetDict()
    for id1,id2 in zip(res.id_1,res.id_2):
        osd[id1]=(id2,yn,rel,rel_type)
    # if log: log(pf(osd.to_dict()))
    # display(osd.to_dict())
    # matchdf.to_csv(time.time()+'.csv')

    return match_multiple(osd,db=db,**kwargs)
    

def match_multiple(others_osd,yn='',rel=MATCHRELNAME,rel_type='',db=None,db_force=False,**kwargs):
    log(pf(others_osd.to_dict()))
    from lltk.model.orm import CDB
    db = CDB(force=db_force) if db is None else db
    o=[]
    alltexts=set()
    for id1 in others_osd:
        for id2,yn,rel,rel_rype in others_osd[id1]:
            if log:
                log(f'{id1} --{rel}-> {id2}')
                log(f'{id2} --{rel}-> {id1}')
            
            t1,t2=Text(id1),Text(id2)
            relmeta=dict(yn=yn,rel=rel,rel_type=rel_type,**just_meta_no_id(kwargs))
            t1._rels[t2.addr]=relmeta
            t2._rels[t1.addr]=relmeta
            alltexts|={t1,t2}
    
    bag=[]
    for t in alltexts:
        promise = db.session.execute_async(
            db.setfunc_match, [t.addr,serialize_map(t.rels)],
            #callback=
        )
        bag.append(promise)
    return bag






def find_matches_by_hash(self, texts_iter=None, lsh=None, threshold=0.95, progress=True):
    # Approximate
    if texts_iter is None: texts_iter = self.iter_texts_each()
    texts = []
    if lsh is None:
        from datasketch import MinHashLSH
        lsh = MinHashLSH(threshold=threshold, num_perm=128*2)
        
        for t in texts_iter:
            try:
                minhash = t.minhash()
                if minhash:
                    lsh.insert(t.addr, minhash)
                    texts.append(t)
            except Exception as e:
                self.log.error(e)
    
    # for t in LLTK(author='Gibson'):
    o=[]
    if texts:
        iterr = texts
        if progress and len(texts)>=0: iterr=get_tqdm(texts,desc='[LLTK] Matching texts')
        for t in iterr:
            for t2addr in lsh.query(t.minhash()):
                if t2addr!=t.addr:
                    t2=Text(t2addr)
                    t.add_source(t2,rel_type='minhash')
                    o.append((t.addr,t2.addr))
                    # iterr.set_description(f'[LLTK] Matching: {t.addr} -> {t2.addr}')
    return o


