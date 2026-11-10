"""Auditable public OKX daily history for the independent 36-month task.

Uses UTC daily candles, never the minute-candle parser's one-minute duration.
Raw pages are retained; a failed/incomplete download cannot replace a published run.
"""
from __future__ import annotations

import calendar
import json
import time
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd
import requests

ENDPOINT = '/api/v5/market/history-candles'
DOCS = 'https://www.okx.com/docs-v5/en/#order-book-trading-market-data-get-candlesticks-history'


def utc(value=None):
    value = pd.Timestamp.now(tz='UTC') if value is None else pd.Timestamp(value)
    return value.tz_localize('UTC') if value.tzinfo is None else value.tz_convert('UTC')


def fetch_daily_pages(directory: Path, start='2017-01-01', as_of=None,
                      base_url='https://www.okx.com', session=None, pause=0.15):
    """Paginate backwards with bounded retries and detect stalled pagination."""
    start, as_of = utc(start).normalize(), utc(as_of)
    if start >= as_of:
        raise ValueError('start must precede as_of')
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    session = session or requests.Session()
    session.headers.update({'User-Agent': 'Mozilla/5.0 CryptoBigdataResearch/1.0'})
    cursor = int(as_of.timestamp() * 1000)
    pages, requests_count = [], 0
    for page_index in range(200):
        params = {'instId': 'BTC-USDT', 'bar': '1Dutc', 'limit': 100, 'after': cursor}
        for attempt in range(4):
            requests_count += 1
            try:
                response = session.get(base_url.rstrip('/') + ENDPOINT, params=params, timeout=(10, 30))
                response.raise_for_status()
                payload = response.json()
                if payload.get('code') != '0' or not isinstance(payload.get('data'), list):
                    raise ValueError('OKX returned an invalid/unsuccessful response')
                break
            except (requests.RequestException, ValueError):
                if attempt == 3:
                    raise
                time.sleep(min(2 ** attempt, 4))
        rows = payload['data']
        page = {'request': params, 'fetched_at': utc().isoformat(), 'data': rows}
        (directory / f'page_{page_index:04d}.json').write_text(json.dumps(page), encoding='utf-8')
        pages.extend(rows)
        if not rows:
            break
        try:
            oldest = min(int(row[0]) for row in rows)
        except (TypeError, ValueError, IndexError) as exc:
            raise ValueError('Cannot advance pagination: invalid timestamp') from exc
        if oldest >= cursor:
            raise ValueError('OKX pagination did not move backwards')
        print(f'[DAILY] page={page_index + 1} rows={len(pages)} oldest={pd.to_datetime(oldest, unit="ms", utc=True).date()}', flush=True)
        if oldest <= int(start.timestamp() * 1000):
            break
        cursor = oldest
        time.sleep(pause)
    else:
        raise ValueError('Pagination limit reached before the requested history was exhausted')
    return pages, {'endpoint': base_url.rstrip('/') + ENDPOINT, 'documentation': DOCS,
                   'symbol': 'BTC-USDT', 'bar': '1Dutc', 'requested_start': start.isoformat(),
                   'as_of': as_of.isoformat(), 'http_attempts': requests_count,
                   'raw_rows': len(pages), 'source': 'okx_public_rest'}


def clean_daily(rows, start='2017-01-01', as_of=None):
    """Reject bad geometry/non-finite values; never interpolate absent days."""
    start, as_of = utc(start).normalize(), utc(as_of)
    rejected = Counter()
    parsed = {}
    duplicates = 0
    for row in rows:
        if not isinstance(row, (list, tuple)) or len(row) < 9:
            rejected['malformed'] += 1
            continue
        try:
            ts = pd.to_datetime(int(row[0]), unit='ms', utc=True)
            if str(row[8]) != '1':
                rejected['unclosed'] += 1
                continue
            if ts < start or ts + pd.Timedelta(days=1) > as_of:
                rejected['outside_requested_closed_range'] += 1
                continue
            if ts != ts.normalize():
                rejected['not_utc_midnight'] += 1
                continue
            o, h, l, c, volume, turnover = [float(row[i]) for i in (1, 2, 3, 4, 5, 7)]
            if not np.isfinite([o, h, l, c, volume, turnover]).all():
                rejected['non_finite'] += 1
                continue
            if not (0 < l <= min(o, c) <= max(o, c) <= h and volume >= 0 and turnover >= 0):
                rejected['invalid_ohlcv'] += 1
                continue
            record = {'date': ts, 'open': o, 'high': h, 'low': l, 'close': c,
                      'volume_btc': volume, 'turnover_usdt': turnover}
            if ts in parsed:
                if parsed[ts] != record:
                    raise ValueError(f'Conflicting duplicate daily candle: {ts}')
                duplicates += 1
            parsed[ts] = record
        except (TypeError, OverflowError, IndexError):
            rejected['malformed'] += 1
        except ValueError as exc:
            if str(exc).startswith('Conflicting duplicate'):
                raise
            rejected['malformed'] += 1
    if not parsed:
        raise ValueError('No valid completed daily candles')
    daily = pd.DataFrame(parsed.values()).sort_values('date').reset_index(drop=True)
    expected = pd.date_range(daily.date.min(), daily.date.max(), freq='D', tz='UTC')
    missing = expected.difference(pd.DatetimeIndex(daily.date))
    quality = {'raw_rows': len(rows), 'valid_daily_rows': len(daily),
               'duplicate_rows_removed': duplicates, 'rejected_rows': dict(rejected),
               'missing_daily_count': len(missing), 'missing_daily_dates': [str(d.date()) for d in missing],
               'first_day': str(daily.date.iloc[0].date()), 'last_day': str(daily.date.iloc[-1].date()),
               'interpolated_rows': 0}
    if len(missing):
        raise ValueError(f'Daily history has {len(missing)} missing days; repair source before forecasting')
    # Catch truncated/latest-page responses: only the current unfinished day may be absent.
    if daily.date.iloc[-1] != as_of.normalize() - pd.Timedelta(days=1):
        raise ValueError('Daily source is stale or truncated; latest completed UTC day is missing')
    return daily, quality


def aggregate_complete_months(daily, as_of=None):
    """Keep only full calendar months with all real daily rows present."""
    as_of = utc(as_of)
    source = daily.sort_values('date').copy()
    if source.date.duplicated().any():
        raise ValueError('Daily candles must be unique before monthly aggregation')
    source['month'] = source.date.dt.strftime('%Y-%m')
    monthly, excluded = [], []
    for month, group in source.groupby('month', sort=True):
        period = pd.Period(month, freq='M')
        expected = calendar.monthrange(period.year, period.month)[1]
        next_month = (period + 1).start_time.tz_localize('UTC')
        if next_month > as_of or len(group) != expected:
            excluded.append({'month': month, 'observed_days': len(group), 'expected_days': expected})
            continue
        monthly.append({'month': month, 'open': float(group.open.iloc[0]),
                        'high': float(group.high.max()), 'low': float(group.low.min()),
                        'close': float(group.close.iloc[-1]), 'volume_btc': float(group.volume_btc.sum()),
                        'turnover_usdt': float(group.turnover_usdt.sum()), 'days': len(group)})
    frame = pd.DataFrame(monthly)
    if frame.empty:
        raise ValueError('No complete calendar months')
    periods = pd.PeriodIndex(frame.month, freq='M')
    if len(pd.period_range(periods[0], periods[-1], freq='M')) != len(periods):
        raise ValueError('Incomplete interior month: no interpolation is allowed')
    expected_latest = pd.Period(as_of.tz_localize(None), freq='M') - 1
    if periods[-1] != expected_latest:
        raise ValueError('Latest complete calendar month is missing')
    return frame, excluded
