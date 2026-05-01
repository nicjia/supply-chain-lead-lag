"""Signals (lagged linear, multi-hop), matrix distance, and portfolio / structural metrics."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


def multihop_signal(r: np.ndarray, A: np.ndarray, hop: int) -> np.ndarray:
    """y = A^hop @ r for one period (r vector of peer returns)."""
    if hop < 1:
        raise ValueError("hop >= 1")
    Ap = np.linalg.matrix_power(np.asarray(A, dtype=float), hop)
    return Ap @ r


def lagged_linear_signal(
    R: pd.DataFrame,
    C: np.ndarray,
    k: int,
) -> pd.DataFrame:
    """y_{i,t} = Σ_j C_ij r_{j,t−k}. Rows = dates, columns = firms (same order as R.columns)."""
    C = np.asarray(C, dtype=float)
    if R.shape[1] != C.shape[1]:
        raise ValueError("C must match R columns")
    rows = []
    idx = []
    for t in range(k, len(R.index)):
        r_lag = R.iloc[t - k].to_numpy(dtype=float)
        rows.append(C @ r_lag)
        idx.append(R.index[t])
    return pd.DataFrame(np.vstack(rows), index=pd.DatetimeIndex(idx), columns=R.columns)


def matrix_compare_frobenius(C1: pd.DataFrame, C2: pd.DataFrame) -> float:
    nodes = sorted(set(C1.index).union(C2.index))
    A = C1.reindex(index=nodes, columns=nodes, fill_value=0.0).to_numpy()
    B = C2.reindex(index=nodes, columns=nodes, fill_value=0.0).to_numpy()
    return float(np.linalg.norm(A - B, ord="fro"))


def predictability_ols(y: pd.Series, x: pd.Series) -> dict[str, float]:
    df = pd.concat([y, x], axis=1).dropna()
    if df.shape[0] < 20:
        return {"beta": np.nan, "t_beta": np.nan, "r2": np.nan, "n": float(df.shape[0])}
    yv = df.iloc[:, 0].to_numpy()
    xv = df.iloc[:, 1].to_numpy()
    X = np.column_stack([np.ones(len(xv)), xv])
    beta, _, _, _ = np.linalg.lstsq(X, yv, rcond=None)
    resid = yv - X @ beta
    sse = float(np.sum(resid**2))
    sst = float(np.sum((yv - np.mean(yv)) ** 2))
    r2 = 1.0 - sse / sst if sst > 0 else np.nan
    n, k = len(yv), X.shape[1]
    s2 = sse / max(n - k, 1)
    cov = s2 * np.linalg.inv(X.T @ X)
    se = np.sqrt(np.diag(cov))
    t_beta = beta[1] / se[1] if se[1] > 0 else np.nan
    return {"beta": float(beta[1]), "t_beta": float(t_beta), "r2": float(r2), "n": float(n)}


def sharpe(x: pd.Series, ann: int = 252) -> float:
    x = x.dropna()
    if x.std() == 0 or len(x) < 10:
        return float(np.nan)
    return float(np.sqrt(ann) * x.mean() / x.std())


def max_drawdown(cum: pd.Series) -> float:
    if cum.size == 0:
        return float(np.nan)
    wealth = 1.0 + cum
    peak = wealth.cummax()
    dd = (wealth / peak) - 1.0
    return float(dd.min())


def portfolio_metrics(
    daily_ret: pd.Series,
    ann: int = 252,
    *,
    gross_daily_ret: pd.Series | None = None,
    daily_cost: pd.Series | None = None,
    turnover: pd.Series | None = None,
) -> dict[str, Any]:
    dr = daily_ret.dropna()
    pnl = (1.0 + dr.fillna(0.0)).cumprod() - 1.0
    out = {
        "sharpe": sharpe(dr, ann=ann),
        "mean_daily": float(dr.mean()),
        "vol_daily": float(dr.std()),
        "cum_return": float(pnl.iloc[-1]) if len(pnl) else np.nan,
        "max_drawdown": max_drawdown(pnl),
        "n_days": int(dr.shape[0]),
    }
    if gross_daily_ret is not None:
        g = gross_daily_ret.dropna()
        gpnl = (1.0 + g.fillna(0.0)).cumprod() - 1.0
        out["gross_sharpe"] = sharpe(g, ann=ann)
        out["gross_cum_return"] = float(gpnl.iloc[-1]) if len(gpnl) else np.nan
    if daily_cost is not None:
        dc = daily_cost.reindex(dr.index).fillna(0.0)
        out["avg_daily_cost_bps"] = float(dc.mean() * 1e4)
        out["total_cost"] = float(dc.sum())
    if turnover is not None:
        tv = turnover.reindex(dr.index).fillna(0.0)
        out["avg_daily_turnover"] = float(tv.mean())
        out["total_turnover"] = float(tv.sum())
    return out


def structural_summary(C: np.ndarray) -> dict[str, float]:
    C = np.asarray(C, dtype=float)
    S = C - C.T
    H = 1j * S
    ev = np.linalg.eigvalsh(H)
    return {
        "lambda_max_H": float(ev.max()),
        "lambda_min_H": float(ev.min()),
        "fro_C": float(np.linalg.norm(C, ord="fro")),
    }
