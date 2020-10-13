from __future__ import absolute_import
from __future__ import print_function
from six.moves import range
def model_ranks_lm(self,fn=None):
    import pystats
    if not fn: fn='data.word2vec.consolidated.ranks.{0}.txt'.format(self.name)
    print('>> loading:',fn)
    ld=pytxt.tsv2ld(fn)
    print('>> done.')
    for d in ld: d['W1_W2']=(d['word1'],d['word2'])

    #"""
    dld=pytxt.ld2dld(ld,'W1_W2')
    max_periods=len(set([d['model_1'] for d in ld]))
    for (word1,word2),wld in sorted(dld.items()):
        num_periods=len(set([d['model_1'] for d in wld]))
        for dx in wld:
            dx['num_periods']=num_periods
            dx['has_max_periods']=num_periods==max_periods
        print(word1,word2)
        if num_periods!=max_periods: continue
        wld.sort(key=lambda _d: _d['model_1'])
        Y = [d['closeness_rank'] for d in wld]
        X = [int(d['model_1'].split('-')[0]) for d in wld]
        a,b,RR = pystats.linreg(X,Y)
        pr,pp=pearsonr(X,Y)
        for dx in wld:
            dx['linreg_RR']=RR
            dx['pearson_R']=pr
            dx['pearson_P']=pp
    #"""

    for word1,wld in sorted(pytxt.ld2dld(ld,'word1').items()):
        print(word1,'...')
        dld_wld=pytxt.ld2dld(wld,'model_1')
        period2sets={}
        for period,pld in sorted(dld_wld.items()):
            if not period in period2sets: period2sets[period]=[]
            for run,rld in sorted(pytxt.ld2dld(pld,'model_2').items()):
                word2s=[dx['word2'] for dx in rld]
                period2sets[period]+=[set(word2s)]

        setlist = [v for k,v in sorted(period2sets.items())]
        periods = [k for k,v in sorted(period2sets.items())]
        period2jacs = dict((k,[]) for k in periods)
        for set_combo in pystats.product(*setlist):
            i=0
            for set1,set2 in pytxt.bigrams(set_combo):
                i+=1
                jacc = float(len(set1&set2)) / len(set2) if len(set2) else 0
                period=periods[i]
                period2jacs[period]+=[jacc]

        jacc_scores = []
        for period in sorted(period2jacs):
            #print word1, period, pystats.median(period2jacs[period]) if period2jacs[period] else 0
            jacc_score = pystats.median(period2jacs[period]) if period2jacs[period] else 0
            jacc_scores+=[jacc_score]
            for dx in dld_wld[period]:
                dx['word1_jaccard_prev_period']=jacc_score

        X=list(range(len(jacc_scores)))

        la,lb,lrr = pystats.linreg(X, jacc_scores)
        for dx in wld:
            dx['linreg_jacc']=lrr

    ## Compress by median-ing across runs

    def writegen():
        for ww,wwld in list(dld.items()):
            dld2=pytxt.ld2dld(wwld,'model_1')
            old=[]
            for pww,pwwld in list(dld2.items()):
                newd={}
                keys=set()
                for d in pwwld: keys|=set(d.keys())
                for k in keys:
                    if type(pwwld[0][k]) in [float,int]:
                        newd[k]=pystats.median([d[k] for d in pwwld])
                    else:
                        newd[k]=pwwld[0][k]

                old+=[newd]

            dld2=pytxt.ld2dld(old,'model_1')
            for pww,pwwld in list(dld2.items()):
                for dx in pwwld:
                    #print sorted(d.keys())
                    if not dx['has_max_periods']: continue
                    for k in ['pearson_R','linreg_RR']:
                        dx[k+'_across_periods']=pystats.median([d[k] for d in pwwld])
                    dx['closeness_rank_stdev_across_periods']=pystats.stdev([float(d['closeness_rank']) for d in pwwld])
                    yield dx

    pytxt.writegen(fn.replace('.txt','.with-linreg-results.txt'), writegen)
