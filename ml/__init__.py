"""
Crypto Big Data - Machine Learning Package
==========================================
Phân chia thành 2 thư mục chuyên biệt:
- ml.realtime: Dự đoán tức thời / ngắn hạn (+5 phút) với XGBoost Classifier
- ml.long_term_3y: Dự đoán trong 3 năm (36 tháng) với Multi-step Time-series & Ridge
"""

import sys
import importlib
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .realtime import dataset as dataset
    from .realtime import evaluate as evaluate
    from .realtime import locked_dataset as locked_dataset
    from .realtime import model_registry as model_registry
    from .realtime import predict as predict
    from .realtime import prediction_worker as prediction_worker
    from .realtime import short_term_experiment as short_term_experiment
    from .realtime import short_term_features as short_term_features
    from .realtime import short_term_metrics as short_term_metrics
    from .realtime import train as train
    from .long_term_3y import long_term as long_term
    from .long_term_3y import long_term_research as long_term_research

__all__ = [
    "dataset",
    "evaluate",
    "locked_dataset",
    "model_registry",
    "predict",
    "prediction_worker",
    "short_term_experiment",
    "short_term_features",
    "short_term_metrics",
    "train",
    "long_term",
    "long_term_research",
]

_IMPORT_ORDER = [
    ("locked_dataset", "ml.realtime.locked_dataset"),
    ("short_term_features", "ml.realtime.short_term_features"),
    ("short_term_metrics", "ml.realtime.short_term_metrics"),
    ("model_registry", "ml.realtime.model_registry"),
    ("evaluate", "ml.realtime.evaluate"),
    ("long_term", "ml.long_term_3y.long_term"),
    ("long_term_research", "ml.long_term_3y.long_term_research"),
    ("short_term_experiment", "ml.realtime.short_term_experiment"),
    ("dataset", "ml.realtime.dataset"),
    ("predict", "ml.realtime.predict"),
    ("train", "ml.realtime.train"),
    ("prediction_worker", "ml.realtime.prediction_worker"),
]

for name, mod_path in _IMPORT_ORDER:
    mod = importlib.import_module(mod_path)
    sys.modules[f"ml.{name}"] = mod
    globals()[name] = mod

# Configure runtime resolution for short_term_experiment without modifying disk content:
ste = sys.modules.get("ml.short_term_experiment")
if ste is not None:
    ste.ROOT = _project_root
    _orig_sources = getattr(ste, "SOURCES", [])
    def _resolved_fingerprints():
        res = {}
        for p in _orig_sources:
            target = _project_root / p
            if not target.is_file():
                relocated = p.replace("ml/", "ml/realtime/") if "long_term" not in p else p.replace("ml/", "ml/long_term_3y/")
                cand = _project_root / relocated
                if cand.is_file():
                    target = cand
            res[p] = ste.sha(target)
        return res
    ste.fingerprints = _resolved_fingerprints

