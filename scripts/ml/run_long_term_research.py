"""Additional retrospective nested evaluation; original forecasts remain frozen."""
from datetime import datetime,timezone
import hashlib
import json
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[2]))
import pandas as pd
from ml.long_term_3y.long_term_research import research
from runtime.environment import ROOT
from runtime.medallion import atomic_json

if __name__=="__main__":
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    protocol_file=ROOT/"configs/long_term_research.json"
    protocol=json.loads(protocol_file.read_text())
    source=ROOT/"data/long_term/runs"/protocol["source_run"]
    run=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    output=ROOT/"data/long_term_research"/run;output.mkdir(parents=True)
    atomic_json(output/"protocol.json",protocol)
    protected={str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in
        [ROOT/"data/long_term/current.json",ROOT/"configs/canonical_snapshot.json",ROOT/"ml/artifacts/btc_xgboost_model.json"]}
    monthly=pd.read_parquet(source/"monthly.parquet")
    result=research(monthly,protocol)
    code_file = ROOT / "ml/long_term_3y/long_term_research.py" if (ROOT / "ml/long_term_3y/long_term_research.py").exists() else ROOT / "ml/long_term_research.py"
    result.update(run_id=run,generated_at=datetime.now(timezone.utc).isoformat(),source_run=protocol["source_run"],
        source_sha256=hashlib.sha256((source/"monthly.parquet").read_bytes()).hexdigest(),
        code_sha256=hashlib.sha256(code_file.read_bytes()).hexdigest(),
        protected_sha256=protected)
    assert all(hashlib.sha256((ROOT/p).read_bytes()).hexdigest()==digest for p,digest in protected.items())
    atomic_json(output/"research.json",result)
    pd.DataFrame(result["candidate_outer_scores"]).to_csv(output/"candidate_scores.csv",index=False)
    pd.DataFrame(result["outer_predictions"]).to_csv(output/"outer_predictions.csv",index=False)
    pd.DataFrame(result["research_forecast"]).to_csv(output/"research_forecast.csv",index=False)
    atomic_json(ROOT/"data/long_term_research/current.json",dict(run_id=run,sha256=hashlib.sha256((output/"research.json").read_bytes()).hexdigest()))
    print(json.dumps(dict(run_id=run,winning_outer_folds=result["winning_outer_folds"],mean_outer_mase=result["mean_outer_mase"],
        baseline=result["mean_outer_baseline_mase"],final_research_model=result["final_research_model"],independent_holdout=False)),flush=True)
