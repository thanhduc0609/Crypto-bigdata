"""
=============================================================================
Crypto Big Data - OKX REST 1-Minute Candle Parser & Normalizer
=============================================================================
Shared parser for historical backfill and near-real-time prediction fetchers.
Enforces:
  1. Strict validation of OKX candle schema:
     [ts, open, high, low, close, vol, volCcy, volCcyQuote, confirm]
  2. volume: row[5] (BTC base volume)
  3. total_trade_value: row[7] (USDT quote volume from volCcyQuote), NOT row[6]
  4. Finite numeric values (no NaN, no Inf, positive prices/volumes)
  5. Closed-candle confirmation: strictly confirm == "1"
  6. Strict UTC timestamp conversion
  7. Retains microstructure fields as None (no fake 0s)
=============================================================================
"""

import math
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, List


def parse_okx_rest_candle(
    row: List[Any],
    symbol: str = "BTC-USDT",
    require_closed: bool = False
) -> Optional[Dict[str, Any]]:
    """
    Parses a single raw OKX 1m candle array into a canonical candle dictionary.
    Returns None if candle is incomplete, malformed, or invalid.
    
    OKX Candle Schema:
      [0] ts: String timestamp in milliseconds
      [1] o: Open price
      [2] h: High price
      [3] l: Low price
      [4] c: Close price
      [5] vol: Volume in base currency (BTC)
      [6] volCcy: Volume in base/contract currency
      [7] volCcyQuote: Volume in quote currency (USDT) -> total_trade_value
      [8] confirm: "1" = closed, "0" = unclosed

    Contract:
      - Sets is_closed=True strictly when confirm == "1", otherwise False.
      - If require_closed=True, returns None immediately for unclosed candles.
    """
    if not isinstance(row, (list, tuple)) or len(row) < 9:
        return None

    try:
        # 1. Parse and validate timestamp
        ts_ms = int(row[0])
        if ts_ms <= 0:
            return None

        # 2. Check confirmation: strictly "1" indicates a finalized closed candle
        confirm_str = str(row[8]).strip()
        is_closed = (confirm_str == "1")
        if require_closed and not is_closed:
            return None

        # 3. Parse OHLC prices
        open_p = float(row[1])
        high_p = float(row[2])
        low_p = float(row[3])
        close_p = float(row[4])

        # Validate finite positive prices and valid candlestick geometry:
        # 0 < low <= min(open, close) and max(open, close) <= high
        for p in (open_p, high_p, low_p, close_p):
            if not math.isfinite(p) or p <= 0:
                return None
        if not (0 < low_p <= min(open_p, close_p) and max(open_p, close_p) <= high_p):
            return None

        # 4. Volume in BTC (base currency)
        vol_btc = float(row[5])
        if not math.isfinite(vol_btc) or vol_btc < 0:
            return None

        # 5. Total trade value in USDT (from volCcyQuote, row[7])
        # NOT row[6] (which is volCcy in coin/base asset)
        quote_vol_raw = row[7]
        if quote_vol_raw is not None and str(quote_vol_raw).strip() != "":
            turnover_usdt = float(quote_vol_raw)
            if not math.isfinite(turnover_usdt) or turnover_usdt < 0:
                return None
        else:
            turnover_usdt = None

        # 6. Standardize timestamps to UTC
        window_start = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
        window_end = window_start + timedelta(minutes=1)

        return {
            "symbol": symbol,
            "window_start": window_start,
            "window_end": window_end,
            "open": open_p,
            "high": high_p,
            "low": low_p,
            "close": close_p,
            "volume": vol_btc,                # Unit: BTC
            "total_trade_value": turnover_usdt, # Unit: USDT (from row[7] volCcyQuote)
            "trade_count": None,              # Unknown from REST -> strictly None
            "buy_volume": None,               # Unknown from REST -> strictly None
            "sell_volume": None,              # Unknown from REST -> strictly None
            "vwap": None,                     # Unknown from REST -> strictly None
            "trade_imbalance": None,          # Unknown from REST -> strictly None
            "source": "historical_rest",
            "is_closed": is_closed,
            "created_at": datetime.now(timezone.utc)
        }
    except (ValueError, TypeError, OverflowError):
        return None
