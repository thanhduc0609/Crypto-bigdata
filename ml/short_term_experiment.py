"""Versioned offline experiments. Selection precedes holdout access; no live writes."""
import hashlib
import json
import platform
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import sklearn
import xgboost
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from threadpoolctl import threadpool_limits
from xgboost import XGBClassifier

from ml.locked_dataset import load_locked_candles
from ml.short_term_features import build_research_dataset, feature_views
from ml.short_term_metrics import classification_metrics, baselines, paired_day_bootstrap
from processing.features.feature_engineering import COMMON_FEATURE_COLUMNS, compute_features

ROOT=Path(__file__).resolve().parents[1]
PROTOCOL=ROOT/"configs/short_term_experiment.json"
SOURCES=["configs/short_term_experiment.json","ml/short_term_experiment.py","ml/short_term_features.py",
         "ml/short_term_metrics.py","ml/locked_dataset.py","processing/features/feature_engineering.py"]


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def write_json(path,value):
    Path(path).write_text(json.dumps(value,indent=2,ensure_ascii=False,allow_nan=False),encoding="utf-8")


def fingerprints():
    return {p:sha(ROOT/p) for p in SOURCES}


def protected_fingerprints():
    files=list((ROOT/"ml/artifacts").glob("*"))
    files+=[ROOT/"configs/canonical_snapshot.json",ROOT/"data/long_term/current.json"]
    return {p.relative_to(ROOT).as_posix():sha(p) for p in files if p.is_file()}


def cohort(frame,start=None,end=None):
    mask=np.ones(len(frame),dtype=bool)
    if start is not None:mask &= frame.window_start.ge(pd.Timestamp(start)).to_numpy()
    if end is not None:mask &= frame.label_available_at.lt(pd.Timestamp(end)).to_numpy()
    return np.flatnonzero(mask)


def describe(frame,ids):
    part=frame.iloc[ids]
    if part.empty:raise ValueError("Empty experiment partition")
    return dict(rows=len(part),start=part.window_start.min().isoformat(),end=part.window_start.max().isoformat(),
                max_label_available_at=part.label_available_at.max().isoformat(),up=int(part.target_direction.sum()))


def estimator(spec,seed):
    if spec["kind"]=="logistic":
        return make_pipeline(StandardScaler(),LogisticRegression(C=spec["C"],max_iter=2000,random_state=seed))
    if spec["kind"]=="hist":
        return HistGradientBoostingClassifier(max_iter=spec["trees"],learning_rate=spec["rate"],
            max_leaf_nodes=spec["leaves"],l2_regularization=spec["lambda"],early_stopping=False,random_state=seed)
    if spec["kind"]!="xgb":raise ValueError("Unknown candidate family")
    return XGBClassifier(n_estimators=spec["trees"],max_depth=spec["depth"],learning_rate=spec["rate"],
        min_child_weight=spec["min_child"],reg_lambda=spec["lambda"],reg_alpha=spec["alpha"],
        subsample=.8,colsample_bytree=.8,tree_method="hist",n_jobs=2,random_state=seed,eval_metric="logloss")


def logit(prob):
    prob=np.clip(prob,1e-7,1-1e-7)
    return np.log(prob/(1-prob)).reshape(-1,1)


def fit_candidate(spec,frame,views,boundary,protocol):
    train=cohort(frame,end=boundary)
    calibration=None
    fit=train
    if spec.get("calibration")=="platt":
        cut=pd.Timestamp(boundary)-pd.Timedelta(days=protocol["calibration_days"])
        fit=cohort(frame,end=cut)
        calibration=cohort(frame,start=cut,end=boundary)
    model=estimator(spec,protocol["seed"])
    X=views[spec["features"]]
    y=frame.target_direction.astype(int)
    with threadpool_limits(limits=2):
        model.fit(X.iloc[fit],y.iloc[fit])
        calibrator=None
        if calibration is not None:
            p=model.predict_proba(X.iloc[calibration])[:,1]
            calibrator=LogisticRegression(C=1.,random_state=protocol["seed"])
            calibrator.fit(logit(p),y.iloc[calibration])
    bundle=dict(estimator=model,calibrator=calibrator,spec=spec,feature_names=X.columns.tolist())
    bounds=dict(fit=describe(frame,fit),calibration=describe(frame,calibration) if calibration is not None else None)
    return bundle,bounds


def predict_bundle(bundle,features):
    X=features[bundle["feature_names"]]
    with threadpool_limits(limits=2):
        prob=bundle["estimator"].predict_proba(X)[:,1]
        if bundle["calibrator"] is not None:prob=bundle["calibrator"].predict_proba(logit(prob))[:,1]
    return prob


def load_data(protocol):
    raw,descriptor=load_locked_candles()
    if descriptor["run_id"]!=protocol["snapshot_run_id"] or descriptor["gold_sha256"]!=protocol["snapshot_sha256"]:
        raise ValueError("Protocol and locked snapshot differ")
    frame,views=build_research_dataset(raw,protocol["horizon_minutes"])
    return raw,descriptor,frame,views


def select(output=None):
    protocol=json.loads(PROTOCOL.read_text(encoding="utf-8"))
    run=Path(output) if output else ROOT/"data/ml_experiments"/datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    run.mkdir(parents=True,exist_ok=False)
    write_json(run/"protocol.json",protocol)
    provenance=dict(code_sha256=fingerprints(),protected_sha256=protected_fingerprints(),
        created_at=datetime.now(timezone.utc).isoformat(),runtime=dict(python=platform.python_version(),numpy=np.__version__,
        pandas=pd.__version__,sklearn=sklearn.__version__,xgboost=xgboost.__version__,platform=platform.platform()))
    write_json(run/"provenance.json",provenance)
    print(f"RUN {run}",flush=True)
    raw,descriptor,frame,views=load_data(protocol)
    write_json(run/"dataset.json",dict(descriptor=descriptor,raw_rows=len(raw),usable_rows=len(frame),
        excluded_warmup=119,excluded_terminal_labels=5,features={k:v.columns.tolist() for k,v in views.items()},
        microstructure="Not imputed; true REST turnover used, buy/sell flow remains unavailable"))
    records=[]
    out_of_fold=[]
    partitions=[]
    for fold,(start,end) in enumerate(protocol["validation_windows"],1):
        train=cohort(frame,end=start)
        valid=cohort(frame,start,end)
        part=frame.iloc[valid]
        y=part.target_direction.to_numpy(dtype=int)
        partition=dict(fold=fold,train=describe(frame,train),validation=describe(frame,valid),candidate_bounds={})
        for name,(pred,prob) in baselines(frame.target_direction.iloc[train],part,protocol["baselines"]).items():
            records.append(dict(fold=fold,name=name,threshold=.5,**classification_metrics(y,pred,prob)))
        for spec in protocol["candidates"]:
            print(f"fold {fold}/4 {spec['name']}",flush=True)
            bundle,bounds=fit_candidate(spec,frame,views,start,protocol)
            partition["candidate_bounds"][spec["name"]]=bounds
            prob=predict_bundle(bundle,views[spec["features"]].iloc[valid])
            for threshold in protocol["thresholds"]:
                records.append(dict(fold=fold,name=spec["name"],threshold=threshold,
                    **classification_metrics(y,(prob>=threshold).astype(int),prob)))
            out_of_fold.append(pd.DataFrame(dict(fold=fold,name=spec["name"],window_start=part.window_start.to_numpy(),
                                                actual=y,probability_up=prob)))
        partitions.append(partition)
    cv=pd.DataFrame(records)
    cv.to_csv(run/"validation_scores.csv",index=False)
    pd.concat(out_of_fold,ignore_index=True).to_parquet(run/"validation_predictions.parquet",index=False)
    write_json(run/"partitions.json",partitions)
    ranking=cv.groupby(["name","threshold"],as_index=False)[["balanced_accuracy","log_loss","brier","accuracy","macro_f1"]].mean()
    candidate_names=[s["name"] for s in protocol["candidates"]]
    ranking=ranking[ranking.name.isin(candidate_names)].sort_values(["balanced_accuracy","log_loss","name","threshold"],
                                                                 ascending=[False,True,True,True])
    ranking.to_csv(run/"validation_ranking.csv",index=False)
    winner=ranking.iloc[0]
    baseline_ranking=cv[cv.name.isin(protocol["baselines"])].groupby("name").balanced_accuracy.mean().sort_values(ascending=False)
    champion=str(baseline_ranking.index[0])
    bestfold=cv[(cv.name==winner["name"]) & (cv.threshold==winner.threshold)].set_index("fold")
    basefold=cv[cv.name.isin(protocol["baselines"])].groupby("fold").balanced_accuracy.max()
    winning=int((bestfold.balanced_accuracy>basefold).sum())
    spec=next(s for s in protocol["candidates"] if s["name"]==winner["name"])
    print(f"SELECTED {spec['name']} threshold={winner.threshold}",flush=True)
    bundle,bounds=fit_candidate(spec,frame,views,protocol["final_fit_before"],protocol)
    bundle["threshold"]=float(winner.threshold)
    joblib.dump(bundle,run/"candidate.joblib")
    # Artifact is trusted local output only, never unpickle third-party inputs.
    selection=dict(selected=spec,threshold=float(winner.threshold),validation_balanced_accuracy=float(winner.balanced_accuracy),
        winning_validation_folds=winning,validation_baseline_champion=champion,
        baseline_validation_balanced_accuracy={k:float(v) for k,v in baseline_ranking.items()},final_fit=bounds,
        candidate_sha256=sha(run/"candidate.joblib"),code_sha256=provenance["code_sha256"],
        status="selection sealed before holdout access",sealed_at=datetime.now(timezone.utc).isoformat())
    write_json(run/"selection.json",selection)
    return run


def check_parity(raw,frame,views,ids,bundle):
    offsets=pd.Index(pd.to_datetime(raw.window_start,utc=True)).get_indexer(frame.window_start.iloc[ids])
    normalized_ok=True
    legacy_ok=True
    prediction_max=0.
    for row,offset in zip(ids,offsets):
        window=raw.iloc[offset-119:offset+1].reset_index(drop=True)
        legacy,normalized=feature_views(window)
        expected=compute_features(window)[COMMON_FEATURE_COLUMNS].iloc[-1].to_numpy(dtype=float)
        legacy_ok &= bool(np.allclose(views["legacy"].iloc[row],expected,rtol=1e-6,atol=1e-6))
        normalized_ok &= bool(np.allclose(views["stationary"].iloc[row],normalized.iloc[-1],rtol=1e-6,atol=1e-7))
        single=(legacy[COMMON_FEATURE_COLUMNS] if bundle["spec"]["features"]=="legacy" else normalized).tail(1)
        prediction_max=max(prediction_max,float(abs(predict_bundle(bundle,single)[0]-
            predict_bundle(bundle,views[bundle["spec"]["features"]].iloc[[row]])[0])))
    return dict(windows_checked=len(ids),legacy_matches_existing_120_bar_inference=legacy_ok,
        normalized_matches_120_bar_inference=normalized_ok,max_prediction_difference=prediction_max,
        passed=legacy_ok and normalized_ok and prediction_max<1e-6)


def evaluate(run):
    run=Path(run).resolve()
    if (run/"holdout_access.json").exists():raise ValueError("Holdout already opened; inspect saved results, do not select again")
    protocol=json.loads((run/"protocol.json").read_text(encoding="utf-8"))
    selection=json.loads((run/"selection.json").read_text(encoding="utf-8"))
    provenance=json.loads((run/"provenance.json").read_text(encoding="utf-8"))
    if fingerprints()!=selection["code_sha256"] or sha(run/"candidate.joblib")!=selection["candidate_sha256"]:
        raise ValueError("Selection source/model changed before evaluation")
    if protected_fingerprints()!=provenance["protected_sha256"]:raise ValueError("Protected active artifacts changed")
    raw,descriptor,frame,views=load_data(protocol)
    bundle=joblib.load(run/"candidate.joblib")
    test=cohort(frame,*protocol["test_window"])
    ids=test[np.linspace(0,len(test)-1,32,dtype=int)]
    parity=check_parity(raw,frame,views,ids,bundle)
    write_json(run/"feature_parity.json",parity)
    # Record access BEFORE labels/predictions are scored. A failed run cannot pretend this is unseen.
    write_json(run/"holdout_access.json",dict(opened_at=datetime.now(timezone.utc).isoformat(),
        selection_sha256=sha(run/"selection.json"),protocol_sha256=sha(run/"protocol.json"),
        warning="Retrospective sealed comparison, not a prospective blind trial. Nov5-9 previously inspected."))
    incumbent=XGBClassifier()
    incumbent.load_model(ROOT/"ml/artifacts/btc_xgboost_model.json")
    incumbent.set_params(n_jobs=2)
    schema=json.loads((ROOT/"ml/artifacts/feature_schema.json").read_text())["feature_names"]
    if schema!=COMMON_FEATURE_COLUMNS:raise ValueError("Incumbent schema mismatch")
    train=cohort(frame,end=protocol["final_fit_before"])
    results={}
    for period,ids in [("primary_test",test),("previously_inspected_diagnostic",cohort(frame,start=protocol["previously_inspected_diagnostic_from"]))]:
        part=frame.iloc[ids]
        y=part.target_direction.to_numpy(dtype=int)
        prob=predict_bundle(bundle,views[bundle["spec"]["features"]].iloc[ids])
        with threadpool_limits(limits=2):oldp=incumbent.predict_proba(views["legacy"].iloc[ids])[:,1]
        predictions=baselines(frame.target_direction.iloc[train],part,protocol["baselines"])
        predictions["incumbent_v2.0.0"]=(oldp>=.5,oldp)
        predictions[selection["selected"]["name"]]=(prob>=selection["threshold"],prob)
        scores={name:classification_metrics(y,pred,p) for name,(pred,p) in predictions.items()}
        comparisons={name:paired_day_bootstrap(y,prob>=selection["threshold"],pred,part.window_start,
                    protocol["bootstrap_repetitions"],protocol["seed"]) for name,(pred,_) in predictions.items()
                    if name in (selection["validation_baseline_champion"],"incumbent_v2.0.0")}
        output=part[["window_start","decision_time","target_timestamp","label_available_at","close","future_return"]].copy()
        output["actual_up"]=y
        for name,(pred,p) in predictions.items():
            output[f"prediction_{name}"]=np.asarray(pred,dtype=int)
            if p is not None:output[f"probability_{name}"]=p
        output.to_parquet(run/f"{period}_predictions.parquet",index=False)
        daily=[]
        days=part.window_start.dt.strftime("%Y-%m-%d")
        for day in days.unique():
            mask=days.eq(day).to_numpy()
            for name,(pred,p) in predictions.items():
                daily.append(dict(day=day,name=name,**classification_metrics(y[mask],np.asarray(pred)[mask],p[mask] if p is not None else None)))
        pd.DataFrame(daily).to_csv(run/f"{period}_daily_scores.csv",index=False)
        pd.DataFrame([dict(name=name,**score) for name,score in scores.items()]).to_csv(run/f"{period}_scores.csv",index=False)
        bins=np.minimum((prob*10).astype(int),9)
        reliability=[dict(bin_lower=b/10,bin_upper=(b+1)/10,count=int((bins==b).sum()),
            mean_probability=float(prob[bins==b].mean()),observed_up_frequency=float(y[bins==b].mean()))
            for b in range(10) if (bins==b).any()]
        pd.DataFrame(reliability).to_csv(run/f"{period}_reliability.csv",index=False)
        results[period]=dict(cohort=describe(frame,ids),scores=scores,paired_day_intervals=comparisons)
    chosen=results["primary_test"]["scores"][selection["selected"]["name"]]
    competitors={n:s for n,s in results["primary_test"]["scores"].items() if n!=selection["selected"]["name"]}
    frequency=competitors["train_majority"]
    gate=dict(validation_balanced_accuracy=selection["validation_balanced_accuracy"]>=protocol["promotion_gate"]["minimum_validation_balanced_accuracy"],
        winning_validation_folds=selection["winning_validation_folds"]>=protocol["promotion_gate"]["minimum_winning_validation_folds"],
        beats_all_test_competitors=all(chosen["balanced_accuracy"]>s["balanced_accuracy"] for s in competitors.values()),
        paired_day_intervals_positive=all(c["lower_95"]>0 for c in results["primary_test"]["paired_day_intervals"].values()),
        beats_frequency_log_loss=chosen["log_loss"]<frequency["log_loss"],beats_frequency_brier=chosen["brier"]<frequency["brier"],
        inference_feature_parity=parity["passed"])
    if protected_fingerprints()!=provenance["protected_sha256"]:raise ValueError("Active artifacts unexpectedly modified")
    model=bundle["estimator"]
    importance=getattr(model,"feature_importances_",None)
    if importance is None and hasattr(model,"steps"):importance=model.steps[-1][1].coef_[0]
    if importance is not None:
        pd.DataFrame(dict(feature=bundle["feature_names"],importance=importance)).sort_values("importance",key=abs,ascending=False).to_csv(run/"selected_feature_importance.csv",index=False)
    result=dict(selection=selection,results=results,feature_parity=parity,promotion_checks=gate,
        eligible_for_promotion=all(gate.values()),active_artifacts_unchanged=True,
        long_term_forecast="Unchanged; 36-month regression is a separate task",
        completed_at=datetime.now(timezone.utc).isoformat())
    write_json(run/"evaluation.json",result)
    print(json.dumps(dict(selected=selection["selected"]["name"],primary=chosen,promotion=gate),indent=2),flush=True)
    return result
