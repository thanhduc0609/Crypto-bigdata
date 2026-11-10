"""Read-only verification of saved ML evidence and protected production artifacts."""
import argparse
import json
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import numpy as np
import pandas as pd
import joblib
from ml.short_term_experiment import (sha,fingerprints,protected_fingerprints,load_data,
                                      predict_bundle,write_json,ROOT)


def verify(run):
    run=Path(run)
    protocol=json.loads((run/"protocol.json").read_text(encoding="utf-8"))
    selection=json.loads((run/"selection.json").read_text(encoding="utf-8"))
    provenance=json.loads((run/"provenance.json").read_text(encoding="utf-8"))
    evaluation=json.loads((run/"evaluation.json").read_text(encoding="utf-8"))
    access=json.loads((run/"holdout_access.json").read_text(encoding="utf-8"))
    partitions=json.loads((run/"partitions.json").read_text(encoding="utf-8"))
    raw,descriptor,frame,views=load_data(protocol)
    prediction=pd.read_parquet(run/"primary_test_predictions.parquet")
    name=selection["selected"]["name"]
    actual=prediction.actual_up.to_numpy(dtype=int)
    predicted=prediction[f"prediction_{name}"].to_numpy(dtype=int)
    tp=int(((actual==1)&(predicted==1)).sum());tn=int(((actual==0)&(predicted==0)).sum())
    direct_accuracy=(tp+tn)/len(actual)
    direct_balanced=.5*(tp/(actual==1).sum()+tn/(actual==0).sum())
    scores=evaluation["results"]["primary_test"]["scores"][name]
    cutoff=pd.Timestamp(protocol["test_window"][0])
    bounds_ok=True
    for part,(start,end) in zip(partitions,protocol["validation_windows"]):
        bounds_ok &= pd.Timestamp(part["train"]["max_label_available_at"])<pd.Timestamp(start)
        bounds_ok &= pd.Timestamp(part["validation"]["max_label_available_at"])<pd.Timestamp(end)<=cutoff
        for bound in part["candidate_bounds"].values():
            bounds_ok &= pd.Timestamp(bound["fit"]["max_label_available_at"])<pd.Timestamp(start)
            if bound["calibration"]:
                bounds_ok &= pd.Timestamp(bound["fit"]["max_label_available_at"])<pd.Timestamp(bound["calibration"]["start"])
                bounds_ok &= pd.Timestamp(bound["calibration"]["max_label_available_at"])<pd.Timestamp(start)
    ids=pd.Index(frame.window_start).get_indexer(prediction.window_start)
    if (ids<0).any():raise ValueError("Prediction timestamp outside snapshot")
    bundle=joblib.load(run/"candidate.joblib")
    probability=predict_bundle(bundle,views[bundle["spec"]["features"]].iloc[ids])
    closes=raw.set_index("window_start").close
    source_current=closes.reindex(prediction.window_start).to_numpy()
    original_label=(closes.reindex(prediction.target_timestamp).to_numpy()>source_current).astype(int)
    checks=dict(dataset_sha256_matches_protocol=descriptor["gold_sha256"]==protocol["snapshot_sha256"],
        code_matches_sealed_selection=fingerprints()==selection["code_sha256"],
        candidate_matches_sealed_selection=sha(run/"candidate.joblib")==selection["candidate_sha256"],
        active_model_canonical_pointer_and_long_term_unchanged=protected_fingerprints()==provenance["protected_sha256"],
        selected_before_test_access=pd.Timestamp(selection["sealed_at"])<pd.Timestamp(access["opened_at"]),
        access_binds_selection=access["selection_sha256"]==sha(run/"selection.json"),
        access_binds_protocol=access["protocol_sha256"]==sha(run/"protocol.json"),
        all_cv_and_calibration_boundaries_purged=bool(bounds_ok),
        final_fit_labels_mature_before_test=pd.Timestamp(selection["final_fit"]["fit"]["max_label_available_at"])<cutoff,
        all_test_target_candles_closed_before_end=bool(prediction.label_available_at.lt(pd.Timestamp(protocol["test_window"][1])).all()),
        saved_labels_match_source_prices=bool(np.array_equal(actual,original_label) and np.array_equal(source_current,prediction.close.to_numpy())),
        accuracy_matches_direct_confusion_counts=abs(direct_accuracy-scores["accuracy"])<1e-12,
        balanced_accuracy_matches_direct_confusion_counts=abs(direct_balanced-scores["balanced_accuracy"])<1e-12,
        saved_probabilities_reproduce_from_fitted_candidate=bool(np.allclose(probability,prediction[f"probability_{name}"],rtol=0,atol=1e-12)),
        feature_parity_passed=evaluation["feature_parity"]["passed"],
        promotion_decision_matches_all_checks=evaluation["eligible_for_promotion"]==all(evaluation["promotion_checks"].values()))
    checks={name:bool(value) for name,value in checks.items()}
    p=prediction[f"probability_{name}"]
    result=dict(checks=checks,passed=all(checks.values()),primary_test_rows=len(prediction),
        probability_quantiles=p.quantile([0,.01,.1,.5,.9,.99,1]).to_dict(),
        primary_tied_price_labels=int(prediction.future_return.eq(0).sum()),
        source_sha256=descriptor["gold_sha256"],protected_sha256=protected_fingerprints())
    write_json(run/"delivery_verification.json",result)
    print(json.dumps(result,indent=2))
    if not result["passed"]:raise SystemExit("Evidence verification failed")
    return result


if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument("run",type=Path)
    verify(parser.parse_args().run)
