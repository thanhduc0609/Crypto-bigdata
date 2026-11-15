"""
Crypto Big Data - Scripts Root Package
======================================
Phân chia các script theo 4 nhóm chuyên trách:
- scripts.demo: Quản lý demo cục bộ, Docker, supervisor, monitor, recovery tests
- scripts.pipeline: Luồng dữ liệu (ETL, streaming, batch medallion, backfill, S3)
- scripts.ml: Huấn luyện, suy luận, thí nghiệm ngắn hạn & dài hạn 3 năm
- scripts.quality: Kiểm toán chất lượng, đối soát, kiểm tra CI offline
"""

import sys
import importlib
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

_MODULE_MAPPING = {
    # Demo & Ops
    "docker_environment": "scripts.demo.docker_environment",
    "local_service": "scripts.demo.local_service",
    "monitor_demo": "scripts.demo.monitor_demo",
    "check_demo_recovery": "scripts.demo.check_demo_recovery",
    "demo_load_check": "scripts.demo.demo_load_check",
    "audit_demo_pipeline": "scripts.demo.audit_demo_pipeline",
    "export_demo_handover": "scripts.demo.export_demo_handover",
    # Pipeline
    "run_ingestion": "scripts.pipeline.run_ingestion",
    "run_streaming": "scripts.pipeline.run_streaming",
    "run_backfill": "scripts.pipeline.run_backfill",
    "run_batch_medallion": "scripts.pipeline.run_batch_medallion",
    "run_operational_batch": "scripts.pipeline.run_batch_medallion",
    "build_features": "scripts.pipeline.build_features",
    "organize_s3": "scripts.pipeline.organize_s3",
    # ML
    "train_model": "scripts.ml.train_model",
    "run_prediction": "scripts.ml.run_prediction",
    "run_short_term_experiment": "scripts.ml.run_short_term_experiment",
    "verify_short_term_experiment": "scripts.ml.verify_short_term_experiment",
    "export_short_term_experiment": "scripts.ml.export_short_term_experiment",
    "run_long_term": "scripts.ml.run_long_term",
    "run_long_term_research": "scripts.ml.run_long_term_research",
    # Quality
    "check_project": "scripts.quality.check_project",
    "research_artifacts": "scripts.quality.research_artifacts",
    "run_data_quality_audit": "scripts.quality.run_data_quality_audit",
    "render_data_quality_report": "scripts.quality.render_data_quality_report",
    "run_canonical_repair": "scripts.quality.run_canonical_repair",
    "run_legacy_regressions": "scripts.quality.run_legacy_regressions",
}

for name, mod_path in _MODULE_MAPPING.items():
    try:
        mod = importlib.import_module(mod_path)
        sys.modules[f"scripts.{name}"] = mod
        globals()[name] = mod
    except Exception:
        pass

