"""
A set of classes and functions for doing logistic regression and classification
"""
from lltk.imports import *

# imports
from sklearn.linear_model import LogisticRegression
from sklearn.neural_network import MLPClassifier
from sklearn.model_selection import cross_val_predict,cross_val_score
from sklearn.metrics import classification_report
from sklearn.model_selection import LeaveOneOut
from sklearn.model_selection import KFold
import numpy as np
from lltk.model import Model,NullModel
import pandas as pd
import os


def resample_df(df,key,numsample=None,replace=False):
    # print(df[key].value_counts())
    groups=df.groupby(key)
    lens=[len(_gdf.index) for _gname,_gdf in groups]
    minlen=min(lens)
    # print(lens)
    # print(minlen)
    ns=numsample if numsample else minlen
    # print('-->',ns)
    dfs_sample=[_gdf.sample(ns,replace=replace) for _gname,_gdf in groups if len(_gdf)>=ns]
    # print([len(x) for x in dfs_sample])
    ndf=pd.concat(dfs_sample)
    ndf=ndf.sample(frac=1)
    return ndf


class Classifier(Model):
    def __init__(self, dataframe, model_type='logistic',name='Classifier'):
        self.df=dataframe
        self.dfq=dataframe.select_dtypes('number')
        self.name=name
        self.model_type=model_type



    def get_clf(self,type=None,C=0.01):
        if not type: type=self.model_type
        if type=='neural':
            from sklearn.neural_network import MLPClassifier
            clf = MLPClassifier()
        else:
            clf = LogisticRegression(C=C,solver='lbfgs')
        return clf

    def resample(self,y_col_name): #,standardize=True,zscore_axis=0):
        Xdf=resample_df(self.df, y_col_name)
        return Xdf

    

    # basic classifier
    def classify(self,
                 y_col_name,
                 standardize=True,
                 leave_one_out=False,
                 n_splits=5,
                 zscore_axis=0,
                 C=None,
                 resample=True,
                 fillna=0):
        loo=LeaveOneOut()
        kf = KFold(n_splits=n_splits,shuffle=True,random_state=11)
        all_predictions=[]
        all_probs=[]
        ind2prob={}
        ind2pred={}
        if resample:
            groups=self.df.groupby(y_col_name)
            minsize=min(len(g) for i,g in groups)
            Xdf=groups.sample(n=minsize)
        else:
            Xdf=self.df

        y=np.array(Xdf[y_col_name].apply(str)) # fix?
        Xdfq=Xdf.select_dtypes('number').dropna(axis=1) #1)
        

        if standardize:
            for col in Xdfq: Xdfq[col]=(Xdfq[col] - Xdfq[col].mean()) / Xdfq[col].std()

        X=Xdfq.values
#         print(self.df.shape,'-->',Xdf.shape,'-->',Xdfq.shape)
        Xdf=Xdf.loc[Xdfq.index]
#         print(Xdfq.shape)
        #display(Xdfq)
        #display(Xdfq.iloc[0])

        self.Xdf=Xdf
        self.Xdfq=Xdfq
        self.X=X
        self.y=y

        self.cols=cols=Xdf.columns
        all_coeffs=defaultdict(list)
        splitter = loo.split(X) if leave_one_out else kf.split(X)

        for train_index,test_index in splitter:
            clf=self.get_clf()
            y_train, y_test = y[train_index], y[test_index]
            Xdf_train, Xdf_test = Xdfq.iloc[train_index], Xdfq.iloc[test_index]
            X_train,X_test = Xdf_train.values.astype('float'), Xdf_test.values.astype('float')
            try:
                clf.fit(X_train,y_train)
            except Exception as e:
                print('!!',e)
                continue
            probs=clf.predict_proba(X_test)
            predictions=clf.predict(X_test)
            if leave_one_out:
                # predict probs
                prob=probs[0][1]
                all_probs+=[prob]
                # predict vals
                prediction=predictions[0]
                all_predictions+=[prediction]
                # get feature coefficients
            else:
                this_predictions=list(predictions)
                this_probs=[prob[1] for prob in probs]
                for i,index in enumerate(Xdf_test.index):
                    ind2pred[index]=this_predictions[i]
                    ind2prob[index]=this_probs[i]

            try:
                for col,coef in zip(cols,clf.coef_[0]): all_coeffs[col]+=[coef]
            except AttributeError:
                pass

        # reorder if KF
        if not leave_one_out:
            all_predictions=[ind2pred[ind] for ind in Xdfq.index]
            all_probs=[ind2prob[ind] for ind in Xdfq.index]

        # avg feature coefficients
        for cf in all_coeffs: all_coeffs[cf]=np.mean(all_coeffs[cf])

        # return all this data
        self.dfr=pd.DataFrame(index=Xdfq.index)
        self.dfr['pred']=all_predictions
        self.dfr['prob']=all_probs
        self.dfr['true']=self.dfr.join(Xdf[y_col_name])[y_col_name]
        self.dfr['correct']=[int(xx==yy) for xx,yy in zip(self.dfr.pred, self.dfr.true)]
        self.dfr['support']=len(Xdfq)
        self.coeffs=all_coeffs
        self.dfc=pd.DataFrame([{'feat':c, 'coeff':f} for c,f in list(all_coeffs.items())])
        self.clf=clf

        # sort
        self.dfc.sort_values('coeff',inplace=True)
        self.dfr.sort_values('prob',inplace=True)
        #return (all_predictions,all_probs,all_coeffs,clf)

    def report(self):
        from sklearn.metrics import classification_report
        print('## Report for Model (%s)' % self.name)
        print(classification_report(self.dfr['true'], self.dfr['pred']))


    def save_model(self,odir='saved_clf_model_data'):
        from joblib import dump
        if not os.path.exists(odir): os.makedirs(odir)
        fnfn_results=os.path.join(odir,'results.txt')
        fnfn_coeffs=os.path.join(odir,'coeffs.txt')
        fnfn_sample=os.path.join(odir,'sample.txt')
        fnfn_cols=os.path.join(odir,'cols.txt')
        fnfn_clf=os.path.join(odir,'clf.pickle')

        self.dfr.to_csv(fnfn_results,sep='\t',encoding='utf-8')
        self.dfc.to_csv(fnfn_coeffs,sep='\t',encoding='utf-8')
        #self.Xdf.to_csv(fnfn_sample,sep='\t',encoding='utf-8')
        dump(self.clf, fnfn_clf)

        with open(fnfn_cols,'w') as of:
            for col in self.cols:
                of.write(col+'\n')


    def plot_prcurve(self,label='Model',color='b',pos_label='Human'):
        import matplotlib.pyplot as plt
        from sklearn.metrics import precision_recall_curve

        y_true=self.dfr.true
        y_scores=self.dfr.prob

        def prcurve(y_true,y_scores,pos_label='Human'):
            return precision_recall_curve(list(y_true), y_scores, pos_label=pos_label)

        plt.rcParams['figure.figsize'] = [7, 7]
        #from sklearn.utils.fixes import signature
        precision, recall, _ = prcurve(y_true,y_scores,pos_label=pos_label)
        from sklearn.metrics import average_precision_score
        average_precision = average_precision_score([int(yx==pos_label) for yx in y_true], y_scores)
        label = label.replace('_prob_sonnet','') + ' (AP=%s)' % round(average_precision,2)
        #step_kwargs = ({'step': 'post'} if 'step' in signature(plt.fill_between).parameters else {})
        plt.step(recall, precision, color=color, alpha=1.0, where='post',label=label)
        plt.xlabel('Recall')
        plt.ylabel('Precision')
        plt.ylim([0, 1.05])
        plt.xlim([0, 1.0])
        plt.title('2-class Precision-Recall curve')