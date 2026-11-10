"""Nested 36-month rolling research with explicitly previously inspected outcomes."""
import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from ml.long_term import forecast_log,metrics,MODELS,LAGS,LABELS

RESEARCH_LABELS={**LABELS,"ridge_strong":"Ridge lợi suất, regularization mạnh (alpha=100)",
                 "anchored_ensemble":"Trung bình log: 50% baseline + 50% Ridge mạnh"}


def predict_log(prices,horizon,model):
    if model in MODELS:return forecast_log(prices,horizon,model)
    values=np.asarray(prices,dtype=float)
    if len(values)<24 or not np.isfinite(values).all() or (values<=0).any():raise ValueError("Invalid research prices")
    if not 1<=horizon<=36:raise ValueError("Horizon must be 1..36")
    if model=="anchored_ensemble":
        return .5*forecast_log(values,horizon,"naive_last")+.5*predict_log(values,horizon,"ridge_strong")
    if model!="ridge_strong":raise ValueError("Unknown research model")
    returns=list(np.diff(np.log(values)))
    X=[];y=[]
    for t in range(max(LAGS),len(returns)):
        X.append([returns[t-lag] for lag in LAGS]);y.append(returns[t])
    reg=make_pipeline(StandardScaler(),Ridge(alpha=100.))
    reg.fit(X,y)
    forecast=[]
    for _ in range(horizon):
        value=float(reg.predict([[returns[-lag] for lag in LAGS]])[0]);returns.append(value);forecast.append(value)
    result=np.log(values[-1])+np.cumsum(forecast)
    if not np.isfinite(result).all() or (np.abs(result)>600).any():raise ValueError("Unstable research forecast")
    return result


def origins(length,horizon=36,minimum=24,stride=6):
    values=list(range(minimum,length-horizon+1,stride))
    last=length-horizon
    if last>=minimum and last not in values:values.append(last)
    if not values:raise ValueError("Not enough inner history for full 36-month validation")
    return values


def select(train,protocol):
    records=[]
    for model in protocol["candidates"]:
        folds=[]
        for origin in origins(len(train),minimum=protocol["inner_minimum_training_months"],stride=protocol["inner_stride_months"]):
            pred=np.exp(predict_log(train[:origin],36,model))
            score=metrics(train[origin:origin+36],pred,train[:origin])
            if score["mase"] is None:raise ValueError("MASE undefined on constant training history")
            folds.append(dict(training_months=origin,validation_end_exclusive=origin+36,**score))
        records.append(dict(model=model,label=RESEARCH_LABELS[model],mean_mase=float(np.mean([f["mase"] for f in folds])),
                            mean_rmsle=float(np.mean([f["rmsle"] for f in folds])),folds=folds))
    # Protocol ordering resolves exact ties, simplest baseline first.
    winner=min(records,key=lambda row:row["mean_mase"])["model"]
    return winner,records


def bands(train,model,protocol):
    residuals=np.array([np.log(train[t])-predict_log(train[:t],1,model)[0] for t in range(36,len(train))])
    if len(residuals)<12:raise ValueError("Too few residuals")
    rng=np.random.default_rng(protocol["seed"])
    starts=rng.integers(0,len(residuals),size=(protocol["simulations"],12))
    indices=(starts[:,:,None]+np.arange(3))%len(residuals)
    noise=(residuals-residuals.mean())[indices].reshape(protocol["simulations"],36).cumsum(axis=1)
    point=predict_log(train,36,model)
    quantiles=np.quantile(noise+point,[.025,.1,.9,.975],axis=0)
    quantiles[:2]=np.minimum(quantiles[:2],point);quantiles[2:]=np.maximum(quantiles[2:],point)
    if not np.isfinite(quantiles).all() or (np.abs(quantiles)>600).any():raise ValueError("Unstable intervals")
    return np.exp(quantiles)


def interval_metrics(actual,lower,upper,level):
    alpha=1-level
    return dict(nominal_level=level,coverage=float(np.mean((actual>=lower)&(actual<=upper))),
        mean_width_usdt=float(np.mean(upper-lower)),interval_score_usdt=float(np.mean(
            upper-lower+2/alpha*np.maximum(lower-actual,0)+2/alpha*np.maximum(actual-upper,0))))


def research(monthly,protocol):
    periods=pd.PeriodIndex(monthly.month,freq="M")
    if periods.has_duplicates or not periods.is_monotonic_increasing or len(pd.period_range(periods[0],periods[-1],freq="M"))!=len(periods):
        raise ValueError("Monthly history must be consecutive")
    values=monthly.close.to_numpy(dtype=float)
    if not np.isfinite(values).all() or (values<=0).any():raise ValueError("Invalid monthly prices")
    result=[];predictions=[];all_scores=[]
    for origin in protocol["outer_training_months"]:
        if origin+36>len(values):raise ValueError("Outer fold reaches unobserved future")
        train=values[:origin];actual=values[origin:origin+36]
        model,validation=select(train,protocol)
        point=np.exp(predict_log(train,36,model));baseline=np.repeat(train[-1],36)
        selected=metrics(actual,point,train);base=metrics(actual,baseline,train)
        intervals=bands(train,model,protocol)
        yearly=[dict(year_ahead=y+1,selected=metrics(actual[y*12:(y+1)*12],point[y*12:(y+1)*12],train),
                     baseline=metrics(actual[y*12:(y+1)*12],baseline[y*12:(y+1)*12],train)) for y in range(3)]
        result.append(dict(origin=origin,train_end=str(periods[origin-1]),test_start=str(periods[origin]),test_end=str(periods[origin+35]),
            selected_model=model,validation=validation,selected=selected,baseline=base,by_year=yearly,
            interval_80=interval_metrics(actual,intervals[1],intervals[2],.8),interval_95=interval_metrics(actual,intervals[0],intervals[3],.95)))
        for candidate in protocol["candidates"]:
            all_scores.append(dict(origin=origin,model=candidate,selected_before_outer_scoring=candidate==model,
                **metrics(actual,np.exp(predict_log(train,36,candidate)),train)))
        for i in range(36):predictions.append(dict(origin=origin,month=str(periods[origin+i]),horizon=i+1,actual=float(actual[i]),
            predicted=float(point[i]),baseline=float(baseline[i]),lower_80=float(intervals[1,i]),upper_80=float(intervals[2,i]),
            lower_95=float(intervals[0,i]),upper_95=float(intervals[3,i])))
    final,final_validation=select(values,protocol)
    finalpoint=np.exp(predict_log(values,36,final));finalbands=bands(values,final,protocol)
    future=pd.period_range(periods[-1]+1,periods=36,freq="M")
    return dict(schema_version=1,task="monthly_close_36_months",protocol=protocol,outer_folds=result,outer_predictions=predictions,
        candidate_outer_scores=all_scores,outer_windows_overlap=True,independent_holdout=False,
        mean_outer_mase=float(np.mean([r["selected"]["mase"] for r in result])),
        mean_outer_baseline_mase=float(np.mean([r["baseline"]["mase"] for r in result])),
        winning_outer_folds=sum(r["selected"]["mae_usdt"]<r["baseline"]["mae_usdt"] for r in result),
        final_research_model=final,final_validation=final_validation,
        research_forecast=[dict(month=str(month),predicted=float(finalpoint[i]),lower_80=float(finalbands[1,i]),upper_80=float(finalbands[2,i]),
            lower_95=float(finalbands[0,i]),upper_95=float(finalbands[3,i])) for i,month in enumerate(future)],
        limitations=[protocol["prior_exposure"],"Only 105 months: three outer windows overlap heavily and are not independent.",
                    "One-step residual bootstrap is uncalibrated at 36 months; assess coverage AND width.",
                    "This research does not overwrite the frozen original forecast or establish future accuracy."])
