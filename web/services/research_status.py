"""Published, checksum-bound research results, distinct from the active model."""
import hashlib
import json
from pathlib import Path
import re

ROOT=Path(__file__).resolve().parents[2]


def ml_review():
    try:
        pointer=json.loads((ROOT/"configs/ml_review.json").read_text(encoding="utf-8"))
        if not re.fullmatch(r"\d{8}T\d{12}Z",pointer["run_id"]):raise ValueError("Bad review version")
        content=(ROOT/"data/ml_experiments"/pointer["run_id"]/"evaluation.json").read_bytes()
        if hashlib.sha256(content).hexdigest()!=pointer["sha256"]:raise ValueError("Review checksum mismatch")
        result=json.loads(content)
        provenance=json.loads((ROOT/"data/ml_experiments"/pointer["run_id"]/"provenance.json").read_text(encoding="utf-8"))
        if hashlib.sha256((ROOT/"ml/artifacts/btc_xgboost_model.json").read_bytes()).hexdigest()!=provenance["protected_sha256"]["ml/artifacts/btc_xgboost_model.json"]:
            raise ValueError("Active model differs from the evaluated incumbent")
        primary=result["results"]["primary_test"]
        chosen=result["selection"]["selected"]["name"]
        return dict(status="success",task="5_minute_direction",run_id=pointer["run_id"],
            evaluated_at=result["completed_at"],cohort=primary["cohort"],
            active_model="XGBoost v2.0.0",active_metrics=primary["scores"]["incumbent_v2.0.0"],
            majority_baseline=primary["scores"]["train_majority"],
            candidate=chosen,candidate_metrics=primary["scores"][chosen],
            candidate_promoted=False,promotion_checks=result["promotion_checks"],
            paired_day_intervals=primary["paired_day_intervals"],
            note="Đánh giá hồi cứu trên cùng 17.274 mẫu; xác suất dự báo không phải tỷ lệ thắng giao dịch."),200
    except (OSError,ValueError,KeyError,TypeError):
        return dict(status="unverified",message="Chưa có bản đánh giá ML đã xác minh trên máy này."),503


def long_term_review():
    try:
        directory=ROOT/"data/long_term_research"
        pointer=json.loads((directory/"current.json").read_text(encoding="utf-8"))
        if not re.fullmatch(r"\d{8}T\d{12}Z",pointer["run_id"]):raise ValueError("Bad research version")
        content=(directory/pointer["run_id"]/"research.json").read_bytes()
        if hashlib.sha256(content).hexdigest()!=pointer["sha256"]:raise ValueError("Research checksum mismatch")
        data=json.loads(content)
        folds=[{k:v for k,v in f.items() if k!="validation"} for f in data["outer_folds"]]
        return dict(status="success",run_id=data["run_id"],generated_at=data["generated_at"],
            outer_folds=folds,independent_holdout=False,outer_windows_overlap=True,
            winning_outer_folds=data["winning_outer_folds"],final_research_model=data["final_research_model"],
            limitations=data["limitations"]),200
    except (OSError,ValueError,KeyError,TypeError):
        return dict(status="unverified",message="Chưa có đánh giá dài hạn bổ sung đã xác minh."),503
