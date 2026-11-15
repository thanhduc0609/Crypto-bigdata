"""Causal features with a fixed 120-candle context shared by fit and inference."""
from functools import lru_cache
import numpy as np
import pandas as pd
from numpy.lib.stride_tricks import sliding_window_view

from processing.features.feature_engineering import compute_features, COMMON_FEATURE_COLUMNS

HISTORY=120


def _ewm_kernel(length,alpha):
    weights=alpha*(1-alpha)**np.arange(length-1,-1,-1,dtype=float)
    weights[0]=(1-alpha)**(length-1)
    return weights


def _rolling_dot(values,kernel):
    result=np.full(len(values),np.nan)
    if len(values)>=len(kernel):
        result[len(kernel)-1:]=sliding_window_view(np.asarray(values,dtype=float),len(kernel))@kernel
    return result


@lru_cache(maxsize=4)
def _macd_signal_kernel(history):
    basis=pd.DataFrame(np.eye(history))
    macd=basis.ewm(span=12,adjust=False).mean()-basis.ewm(span=26,adjust=False).mean()
    return macd.ewm(span=9,adjust=False).mean().iloc[-1].to_numpy()


def legacy_bounded(candles,history=HISTORY):
    """15 legacy indicators, recomputed as if every row had only its last 120 bars.

    Linear EWM kernels avoid a slow Python loop over 154k windows. RSI kernels
    use the 119 observed deltas; the oldest window's unknown delta is excluded.
    """
    frame=compute_features(candles,feature_mode="common")
    close=frame.close.to_numpy(dtype=float)
    frame["ema_12"]=_rolling_dot(close,_ewm_kernel(history,2/13))
    frame["ema_26"]=_rolling_dot(close,_ewm_kernel(history,2/27))
    frame["macd"]=frame.ema_12-frame.ema_26
    frame["macd_signal"]=_rolling_dot(close,_macd_signal_kernel(history))
    delta=np.diff(close)
    kernel=_ewm_kernel(history-1,1/14)
    gains=np.r_[np.nan,_rolling_dot(np.maximum(delta,0),kernel)]
    losses=np.r_[np.nan,_rolling_dot(np.maximum(-delta,0),kernel)]
    frame["rsi_14"]=np.clip(100-100/(1+gains/(losses+1e-8)),0,100)
    frame.loc[:history-2,COMMON_FEATURE_COLUMNS]=np.nan
    return frame


def feature_views(candles,history=HISTORY):
    legacy=legacy_bounded(candles,history)
    close=legacy.close.astype(float)
    volume=legacy.volume.astype(float)
    logprice=np.log(close)
    normalized=pd.DataFrame(index=legacy.index)
    for lag in (1,2,3,5,10,15,30,60):
        normalized[f"log_return_{lag}"]=logprice.diff(lag)
    for lag in (1,2,3):
        normalized[f"previous_return_{lag}"]=logprice.diff().shift(lag)
    normalized["candle_body"]=(close-legacy.open)/legacy.open
    normalized["range_relative"]=(legacy.high-legacy.low)/close
    spread=(legacy.high-legacy.low).replace(0,np.nan)
    normalized["upper_wick"]=(legacy.high-legacy[["open","close"]].max(axis=1)).div(spread).fillna(0)
    normalized["lower_wick"]=(legacy[["open","close"]].min(axis=1)-legacy.low).div(spread).fillna(0)
    for period in (5,15,30,60,120):
        normalized[f"sma_gap_{period}"]=close/close.rolling(period).mean()-1
    for period in (12,26):
        normalized[f"ema_gap_{period}"]=close/legacy[f"ema_{period}"]-1
    normalized["macd_relative"]=legacy.macd/close
    normalized["macd_signal_relative"]=legacy.macd_signal/close
    normalized["macd_hist_relative"]=(legacy.macd-legacy.macd_signal)/close
    normalized["bollinger_z"]=(close-close.rolling(20).mean())/(close.rolling(20).std()+1e-12)
    normalized["bandwidth"]=legacy.bb_bandwidth
    rsi=(legacy.rsi_14-50)/50
    normalized["rsi_centered"]=rsi.where(close.rolling(history).max().ne(close.rolling(history).min()),0)
    for period in (5,15,30,60):
        normalized[f"volatility_{period}"]=logprice.diff().rolling(period).std()
    normalized["log_volume_change"]=np.log1p(volume).diff()
    for period in (30,120):
        normalized[f"log_volume_relative_{period}"]=np.log1p(volume)-np.log1p(volume.rolling(period).mean())
    normalized["volume_z_60"]=(volume-volume.rolling(60).mean())/(volume.rolling(60).std()+1e-8)
    normalized["zero_volume"]=volume.eq(0).astype(float)
    # Quote turnover is observed REST data. No buy/sell flow is imputed.
    turnover=pd.to_numeric(legacy.total_trade_value,errors="coerce")
    actual_vwap=turnover/volume.replace(0,np.nan)
    normalized["observed_vwap_gap"]=(close-actual_vwap)/close
    normalized.loc[volume.eq(0),"observed_vwap_gap"]=0.
    stamps=pd.to_datetime(legacy.window_start,utc=True)
    phase=2*np.pi*(stamps.dt.hour*60+stamps.dt.minute)/1440
    normalized["utc_time_sin"],normalized["utc_time_cos"]=np.sin(phase),np.cos(phase)
    day=2*np.pi*stamps.dt.dayofweek/7
    normalized["weekday_sin"],normalized["weekday_cos"]=np.sin(day),np.cos(day)
    normalized=normalized.replace([np.inf,-np.inf],np.nan).clip(-20,20)
    normalized.iloc[:history-1]=np.nan
    return legacy,normalized


def build_research_dataset(candles,horizon=5):
    from processing.features.feature_engineering import add_future_direction_target
    stamps=pd.to_datetime(candles.window_start,utc=True)
    if stamps.duplicated().any() or not stamps.is_monotonic_increasing or not stamps.diff().dropna().eq(pd.Timedelta(minutes=1)).all():
        raise ValueError("ML requires an ordered, unique, consecutive minute cohort")
    legacy,normalized=feature_views(candles)
    labeled=add_future_direction_target(legacy,horizon_minutes=horizon)
    keep=(labeled.target_direction.notna() & np.isfinite(labeled[COMMON_FEATURE_COLUMNS].to_numpy()).all(axis=1)
          & np.isfinite(normalized.to_numpy()).all(axis=1))
    selected=labeled.loc[keep].reset_index(drop=True)
    normalized=normalized.loc[keep].reset_index(drop=True)
    selected["decision_time"]=pd.to_datetime(selected.window_start,utc=True)+pd.Timedelta(minutes=1)
    selected["target_timestamp"]=pd.to_datetime(selected.window_start,utc=True)+pd.Timedelta(minutes=horizon)
    selected["label_available_at"]=selected.target_timestamp+pd.Timedelta(minutes=1)
    return selected,{"legacy":selected[COMMON_FEATURE_COLUMNS].astype(float),"stationary":normalized.astype(float)}
