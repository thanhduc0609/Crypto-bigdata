"""Reproducible monthly price forecasts; independent of the +5m classifier.

Model selection uses only pre-holdout observations. A single untouched 36-month
path tests the actual target horizon; final refitting happens after evaluation.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

MODELS = ('naive_last', 'log_drift', 'damped_log_trend', 'ridge_returns')
LABELS = {'naive_last': 'Giữ nguyên giá tháng gần nhất (baseline)',
          'log_drift': 'Xu hướng lợi suất log trung bình',
          'damped_log_trend': 'Xu hướng log có giảm dần',
          'ridge_returns': 'Ridge tự hồi quy lợi suất tháng'}
LAGS = (1, 2, 3, 6, 12)


def forecast_log(prices, horizon, model):
    prices = np.asarray(prices, dtype=float)
    if prices.ndim != 1 or len(prices) < 24 or not np.isfinite(prices).all() or (prices <= 0).any():
        raise ValueError('Need at least 24 finite positive monthly prices')
    if not isinstance(horizon, int) or not 1 <= horizon <= 36:
        raise ValueError('Forecast horizon must be 1..36 months')
    logs = np.log(prices)
    steps = np.arange(1, horizon + 1)
    if model == 'naive_last':
        predicted = np.repeat(logs[-1], horizon)
    elif model == 'log_drift':
        predicted = logs[-1] + steps * np.diff(logs).mean()
    elif model == 'damped_log_trend':
        slope = np.polyfit(np.arange(min(24, len(logs))), logs[-24:], 1)[0]
        predicted = logs[-1] + slope * np.cumsum(0.9 ** steps)
    elif model == 'ridge_returns':
        returns = list(np.diff(logs))
        X, y = [], []
        for t in range(max(LAGS), len(returns)):
            X.append([returns[t - lag] for lag in LAGS])
            y.append(returns[t])
        if len(y) < 8:
            raise ValueError('Not enough lagged observations for Ridge')
        reg = make_pipeline(StandardScaler(), Ridge(alpha=10.0))
        reg.fit(np.asarray(X), np.asarray(y))
        future = []
        for _ in steps:
            value = float(reg.predict([[returns[-lag] for lag in LAGS]])[0])
            returns.append(value)
            future.append(value)
        predicted = logs[-1] + np.cumsum(future)
    else:
        raise ValueError(f'Unknown model: {model}')
    if not np.isfinite(predicted).all() or (np.abs(predicted) > 600).any():
        raise ValueError('Forecast is numerically unstable')
    return np.asarray(predicted)


def metrics(actual, predicted, train):
    actual, predicted, train = map(lambda v: np.asarray(v, dtype=float), (actual, predicted, train))
    error = actual - predicted
    scale = np.abs(np.diff(train)).mean()
    return {'mae_usdt': float(np.abs(error).mean()), 'rmse_usdt': float(np.sqrt(np.mean(error ** 2))),
            'smape_pct': float(np.mean(200 * np.abs(error) / (np.abs(actual) + np.abs(predicted)))),
            'rmsle': float(np.sqrt(np.mean((np.log(actual) - np.log(predicted)) ** 2))),
            'mase': float(np.abs(error).mean() / scale) if scale > 0 else None}


def select_model(train, min_train=24, validation_horizon=36, stride=6):
    """Rolling-origin validation, all contained strictly before the test boundary."""
    train = np.asarray(train, dtype=float)
    origins = list(range(min_train, len(train) - validation_horizon + 1, stride))
    final_origin = len(train) - validation_horizon
    if final_origin >= min_train and final_origin not in origins:
        origins.append(final_origin)
    if not origins:
        raise ValueError('Not enough pre-holdout history for rolling validation')
    scores = []
    for model in MODELS:
        folds = []
        for origin in origins:
            predicted = np.exp(forecast_log(train[:origin], validation_horizon, model))
            result = metrics(train[origin:origin + validation_horizon], predicted, train[:origin])
            folds.append({'train_months': origin, 'validation_months': validation_horizon, **result})
        scores.append({'model': model, 'label': LABELS[model],
                       'mean_rmsle': float(np.mean([f['rmsle'] for f in folds])), 'folds': folds})
    # MODELS order resolves exact ties in favor of the simplest baseline.
    winner = min(scores, key=lambda entry: entry['mean_rmsle'])['model']
    return winner, scores


def uncertainty_band(prices, model, horizon, simulations=3000, seed=42, block_size=3):
    """Approximate log-error bands from training-only rolling 1-step residuals.

Circular 3-month blocks preserve some dependence; cumulative errors widen the
band. These are conditional, uncalibrated intervals, not confidence guarantees.
"""
    values = np.asarray(prices, dtype=float)
    residuals = np.array([np.log(values[t]) - forecast_log(values[:t], 1, model)[0]
                          for t in range(36, len(values))])
    if len(residuals) < 12:
        raise ValueError('Need at least 12 rolling residuals for uncertainty estimates')
    centered = residuals - residuals.mean()
    rng = np.random.default_rng(seed)
    starts = rng.integers(0, len(centered), size=(simulations, (horizon + block_size - 1) // block_size))
    indices = (starts[:, :, None] + np.arange(block_size)) % len(centered)
    noise = centered[indices].reshape(simulations, -1)[:, :horizon].cumsum(axis=1)
    point = forecast_log(values, horizon, model)
    bounds = np.quantile(noise + point, [0.025, 0.10, 0.90, 0.975], axis=0)
    # Include the point estimate in both nested bands, even with skewed residuals.
    bounds[0] = np.minimum(bounds[0], point)
    bounds[1] = np.minimum(bounds[1], point)
    bounds[2] = np.maximum(bounds[2], point)
    bounds[3] = np.maximum(bounds[3], point)
    if not np.isfinite(bounds).all() or (np.abs(bounds) > 600).any():
        raise ValueError('Uncertainty bands are numerically unstable')
    return np.exp(bounds), {'method': 'centered_rolling_log_residual_circular_block_bootstrap',
                           'block_months': block_size, 'simulations': simulations, 'seed': seed,
                           'residual_count': len(residuals), 'nominal_levels': [0.80, 0.95],
                           'calibrated': False}


def build_forecast(monthly, config=None):
    cfg = {'horizon_months': 36, 'holdout_months': 36, 'min_train_months': 24,
           'validation_months': 36, 'validation_stride': 6, 'simulations': 3000, 'seed': 42}
    cfg.update(config or {})
    horizon, holdout = int(cfg['horizon_months']), int(cfg['holdout_months'])
    if horizon != 36 or holdout != 36:
        raise ValueError('This task requires both forecast and untouched holdout to be 36 months')
    if int(cfg['min_train_months']) < 24 or int(cfg['validation_months']) != 36 or int(cfg['validation_stride']) < 1:
        raise ValueError('Need min_train>=24, validation=36 months, stride>=1')
    if int(cfg['simulations']) < 500:
        raise ValueError('Need at least 500 bootstrap simulations')
    months = list(monthly['month'])
    if not months:
        raise ValueError('No monthly history')
    periods = pd.PeriodIndex(months, freq='M')
    if periods.has_duplicates or not periods.is_monotonic_increasing or len(pd.period_range(periods[0], periods[-1], freq='M')) != len(periods):
        raise ValueError('Monthly history must be contiguous, unique and increasing')
    values = monthly['close'].to_numpy(dtype=float)
    minimum = holdout + int(cfg['min_train_months']) + int(cfg['validation_months'])
    if len(values) < minimum:
        raise ValueError(f'Need at least {minimum} complete months; got {len(values)}')
    if not np.isfinite(values).all() or (values <= 0).any():
        raise ValueError('Monthly prices must be finite and positive')
    split = len(values) - holdout
    train, actual = values[:split], values[split:]
    selected, scores = select_model(train, int(cfg['min_train_months']), int(cfg['validation_months']), int(cfg['validation_stride']))
    holdout_predictions = {model: np.exp(forecast_log(train, holdout, model)) for model in MODELS}
    evaluations = [{'model': model, 'label': LABELS[model], **metrics(actual, pred, train)}
                   for model, pred in holdout_predictions.items()]
    holdout_bands, holdout_interval_method = uncertainty_band(train, selected, holdout, int(cfg['simulations']), int(cfg['seed']))
    chosen = holdout_predictions[selected]
    baseline = holdout_predictions['naive_last']
    evaluation = next(row for row in evaluations if row['model'] == selected)
    baseline_evaluation = evaluations[0]
    yearly = []
    for index in range(3):
        part = slice(index * 12, (index + 1) * 12)
        yearly.append({'year_ahead': index + 1, **metrics(actual[part], chosen[part], train)})
    backtest = [{'month': months[split + i], 'actual': float(actual[i]),
                 'predicted': float(chosen[i]), 'baseline': float(baseline[i]),
                 'lower_80': float(holdout_bands[1, i]), 'upper_80': float(holdout_bands[2, i]),
                 'lower_95': float(holdout_bands[0, i]), 'upper_95': float(holdout_bands[3, i])}
                for i in range(holdout)]
    # Freeze choice and holdout metrics, then refit the SAME model on all available history.
    final = np.exp(forecast_log(values, horizon, selected))
    final_bands, interval_method = uncertainty_band(values, selected, horizon, int(cfg['simulations']), int(cfg['seed']))
    future_months = pd.period_range(periods[-1] + 1, periods=36, freq='M')
    forecast = [{'month': str(month), 'predicted': float(final[i]),
                 'lower_80': float(final_bands[1, i]), 'upper_80': float(final_bands[2, i]),
                 'lower_95': float(final_bands[0, i]), 'upper_95': float(final_bands[3, i])}
                for i, month in enumerate(future_months)]
    return {'schema_version': 1, 'symbol': 'BTC-USDT', 'target': 'monthly_close_usdt',
            'frequency': 'calendar_month_utc', 'horizon_months': 36,
            'as_of_month': months[-1], 'model': selected, 'model_label': LABELS[selected],
            'model_specifications': {'naive_last': {}, 'log_drift': {'window': 'all_training_log_returns'},
                                     'damped_log_trend': {'window_months':24, 'damping':0.9},
                                     'ridge_returns': {'lags':list(LAGS), 'alpha':10.0, 'standardize':True}},
            'config': cfg, 'history': monthly.to_dict(orient='records'),
            'validation': {'selection_metric': 'mean_fold_rmsle', 'horizon_months': 36,
                           'latest_month_available_for_selection': months[split - 1],
                           'candidates': scores, 'folds_overlap': True},
            'evaluation': {'holdout_months': 36, 'train_start': months[0], 'train_end': months[split - 1],
                           'test_start': months[split], 'test_end': months[-1], 'selected': evaluation,
                           'baseline': baseline_evaluation, 'all_candidates': evaluations,
                           'beats_baseline_mae': evaluation['mae_usdt'] < baseline_evaluation['mae_usdt'],
                           'by_year_ahead': yearly,
                           'coverage_80': float(np.mean((actual >= holdout_bands[1]) & (actual <= holdout_bands[2]))),
                           'coverage_95': float(np.mean((actual >= holdout_bands[0]) & (actual <= holdout_bands[3]))),
                           'interval_method': holdout_interval_method,
                           'protocol': 'select_on_pre_holdout_rolling_36m_then_freeze_and_evaluate_one_untouched_36m_path'},
            'backtest': backtest, 'forecast': forecast, 'interval_method': interval_method,
            'limitations': ['Chỉ một cửa sổ holdout 36 tháng; chưa đại diện nhiều chu kỳ độc lập.',
                           'Các fold validation 36 tháng chồng lấn; lịch sử sàn chỉ đủ ít cửa sổ dài hạn.',
                           'Dải 80%/95% là mức danh nghĩa từ bootstrap, chưa bảo đảm độ phủ tương lai.',
                           'Dự báo dựa trên lịch sử giá OKX; không bao gồm biến vĩ mô hoặc thay đổi chế độ thị trường.']}
