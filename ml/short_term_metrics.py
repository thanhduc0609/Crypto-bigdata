"""Unrounded classification/calibration scores and paired UTC-day uncertainty."""
import numpy as np
import pandas as pd
from sklearn.metrics import (accuracy_score,balanced_accuracy_score,precision_recall_fscore_support,
                            f1_score,roc_auc_score,log_loss,brier_score_loss,confusion_matrix)


def classification_metrics(y,pred,prob=None):
    y,pred=np.asarray(y,dtype=int),np.asarray(pred,dtype=int)
    if len(y)==0 or len(y)!=len(pred) or not np.isin(y,[0,1]).all() or not np.isin(pred,[0,1]).all():
        raise ValueError("Invalid binary evaluation cohort")
    precision,recall,f1,_=precision_recall_fscore_support(y,pred,labels=[0,1],zero_division=0)
    result=dict(samples=len(y),accuracy=float(accuracy_score(y,pred)),balanced_accuracy=float(balanced_accuracy_score(y,pred)),
        precision_up=float(precision[1]),recall_up=float(recall[1]),f1_up=float(f1[1]),
        precision_not_up=float(precision[0]),recall_not_up=float(recall[0]),f1_not_up=float(f1[0]),
        macro_f1=float(f1_score(y,pred,average="macro",zero_division=0)),confusion_matrix=confusion_matrix(y,pred,labels=[0,1]).tolist(),
        actual_up=int(y.sum()),predicted_up=int(pred.sum()),roc_auc=None,log_loss=None,brier=None,ece_10=None)
    if prob is not None:
        prob=np.asarray(prob,dtype=float)
        if len(prob)!=len(y) or not np.isfinite(prob).all() or ((prob<0)|(prob>1)).any():
            raise ValueError("Probabilities must be finite and between zero and one")
        result.update(roc_auc=float(roc_auc_score(y,prob)) if len(np.unique(y))==2 else None,
                      log_loss=float(log_loss(y,np.c_[1-prob,prob],labels=[0,1])),brier=float(brier_score_loss(y,prob)))
        bins=np.minimum((prob*10).astype(int),9)
        result["ece_10"]=float(sum((bins==b).mean()*abs(prob[bins==b].mean()-y[bins==b].mean()) for b in range(10) if (bins==b).any()))
    return result


def baselines(train_y,evaluation, names):
    train_y=np.asarray(train_y,dtype=int)
    freq=float(train_y.mean())
    majority=int(np.bincount(train_y,minlength=2).argmax())
    result={}
    for name in names:
        if name=="train_majority":
            result[name]=(np.full(len(evaluation),majority),np.full(len(evaluation),freq))
        else:
            lag=1 if name.endswith("1m") else 5
            signal=evaluation["log_return" if lag==1 else "price_momentum_5m"].to_numpy()
            pred=(signal>0).astype(int)
            if name.startswith("mean_reversion"):
                pred=1-pred
            result[name]=(pred,None)
    return result


def paired_day_bootstrap(y,first,second,times,repetitions=2000,seed=42):
    y,first,second=map(np.asarray,(y,first,second))
    dates=pd.to_datetime(times,utc=True).dt.strftime("%Y-%m-%d").to_numpy()
    days=np.unique(dates)
    if len(days)<3:
        raise ValueError("At least three UTC day clusters required")
    def counts(pred):
        return np.array([[((y==1)&(pred==1)&(dates==d)).sum(),((y==1)&(dates==d)).sum(),
                          ((y==0)&(pred==0)&(dates==d)).sum(),((y==0)&(dates==d)).sum()] for d in days],dtype=float)
    a,b=counts(first),counts(second)
    rng=np.random.default_rng(seed)
    samples=rng.integers(0,len(days),size=(repetitions,len(days)))
    def scores(table):
        total=table[samples].sum(axis=1)
        return .5*(total[:,0]/total[:,1]+total[:,2]/total[:,3])
    delta=scores(a)-scores(b)
    lower,upper=np.quantile(delta,[.025,.975])
    point=float(balanced_accuracy_score(y,first)-balanced_accuracy_score(y,second))
    return dict(metric="balanced_accuracy_difference",point=point,lower_95=float(lower),upper_95=float(upper),
                utc_day_clusters=len(days),bootstrap_repetitions=repetitions,
                interpretation="Approximate paired day-block interval; labels overlap and days may remain dependent")
