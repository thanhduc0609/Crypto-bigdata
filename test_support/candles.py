"""Synthetic, UTC-aligned fixtures, never actual market observations."""
from datetime import datetime, timezone, timedelta
from unittest.mock import patch
import numpy as np
import pandas as pd


def canonical_candles(n=120,as_of=None,start_price=85000.0):
    as_of=as_of or datetime.now(timezone.utc)
    latest=as_of.replace(second=0,microsecond=0)-timedelta(minutes=1)
    first=latest-timedelta(minutes=n-1)
    rows=[]; price=start_price
    for i in range(n):
        start=first+timedelta(minutes=i)
        close=price+np.sin(i/10)*20+(10 if i%2==0 else -8)
        volume=5+abs(np.cos(i/5))*10
        rows.append({'symbol':'BTC-USDT','window_start':start,'window_end':start+timedelta(minutes=1),
                     'open':price,'high':max(price,close)+15,'low':min(price,close)-15,'close':close,
                     'volume':volume,'total_trade_value':volume*close,'source':'historical_rest','is_closed':True,
                     'trade_count':None,'buy_volume':None,'sell_volume':None,'vwap':None,'trade_imbalance':None})
        price=close
    return pd.DataFrame(rows)


def prediction_clock(moment):
    """Freeze only inference's wall clock; SQL still checks real DB time."""
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):
            return moment.astimezone(tz) if tz else moment.replace(tzinfo=None)
    return patch('ml.predict.datetime',Clock)
