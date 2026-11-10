"""Read-only published monthly forecast. HTTP requests never train/download."""
import csv
import hashlib
import io
import json
import re
import math
from pathlib import Path


def evaluation_summary(result):
    """Presentation diagnostics computed from the frozen experiment, never retrain."""
    ev = result['evaluation']
    chosen, baseline = ev['selected']['mae_usdt'], ev['baseline']['mae_usdt']
    validation = {row['model']: row['mean_rmsle'] for row in result['validation']['candidates']}
    comparisons = [{**row, 'validation_rmsle': validation[row['model']],
                    'selected_before_test': row['model'] == result['model']}
                   for row in ev['all_candidates']]
    intervals = []
    for level in (80, 95):
        rows = result['backtest']
        alpha = 1 - level/100
        widths, scores = [], []
        for row in rows:
            lower, upper, actual = row[f'lower_{level}'], row[f'upper_{level}'], row['actual']
            if not (0 < lower <= upper and actual > 0 and all(math.isfinite(v) for v in (lower,upper,actual))):
                raise ValueError('Invalid backtest interval')
            width = upper-lower
            widths.append(width)
            scores.append(width + 2/alpha * max(lower-actual, 0) + 2/alpha * max(actual-upper, 0))
        intervals.append({'nominal_level': level, 'observed_coverage_pct': 100*sum(
            row[f'lower_{level}'] <= row['actual'] <= row[f'upper_{level}'] for row in rows)/len(rows),
            'mean_width_usdt': sum(widths)/len(widths), 'mean_interval_score_usdt': sum(scores)/len(scores)})
    return {'selection_months': len(result['history'])-ev['holdout_months'],
            'refit_months': len(result['history']),
            'mae_change_vs_baseline_pct': 100*(chosen-baseline)/baseline if baseline else None,
            'comparisons':comparisons, 'intervals':intervals,
            'future_accuracy_known':False,
            'status':'beats_baseline_on_this_test' if chosen < baseline else 'no_mae_improvement_on_this_test'}


def load_forecast(directory):
    root = Path(directory)
    pointer = root / 'current.json'
    if not pointer.exists():
        return {'status': 'empty', 'message': 'Chưa có dự báo 3 năm. Chạy python scripts/research_artifacts.py restore để khôi phục phiên đã công bố, hoặc python scripts/run_long_term.py để tạo phiên mới.'}, 404
    try:
        manifest = json.loads(pointer.read_text(encoding='utf-8'))
        run_id = manifest['run_id']
        if not re.fullmatch(r'[0-9]{8}T[0-9]{6,12}Z', run_id):
            raise ValueError('Invalid run identifier')
        raw = (root / 'runs' / run_id / 'forecast.json').read_bytes()
        if hashlib.sha256(raw).hexdigest() != manifest['forecast_sha256']:
            raise ValueError('Forecast checksum mismatch')
        result = json.loads(raw)
        if result['schema_version'] != 1 or result['horizon_months'] != 36 or len(result['forecast']) != 36 or len(result['backtest']) != 36:
            raise ValueError('Invalid forecast schema/horizon')
        for row in result['forecast']:
            if not re.fullmatch(r'\d{4}-\d{2}', row['month']):
                raise ValueError('Invalid forecast month')
            if not all(math.isfinite(row[k]) and row[k] > 0 for k in ('predicted','lower_80','upper_80','lower_95','upper_95')):
                raise ValueError('Invalid forecast values')
            if not row['lower_95'] <= row['lower_80'] <= row['predicted'] <= row['upper_80'] <= row['upper_95']:
                raise ValueError('Invalid uncertainty band order')
        return {'status': 'success', **result, 'reporting':evaluation_summary(result)}, 200
    except (OSError, ValueError, KeyError, TypeError):
        return {'status': 'error', 'message': 'Snapshot dự báo không hợp lệ hoặc chưa đầy đủ; cần chạy lại pipeline.'}, 503


def forecast_csv(result):
    output = io.StringIO(newline='')
    writer = csv.DictWriter(output, fieldnames=['month','predicted','lower_80','upper_80','lower_95','upper_95'])
    writer.writeheader()
    writer.writerows(result['forecast'])
    return output.getvalue()


def evaluation_csv(result):
    output = io.StringIO(newline='')
    fields = ['model','label','selected_before_test','validation_rmsle','mae_usdt','rmse_usdt','smape_pct','rmsle','mase']
    writer = csv.DictWriter(output, fieldnames=fields)
    writer.writeheader()
    writer.writerows(result['reporting']['comparisons'])
    return output.getvalue()
