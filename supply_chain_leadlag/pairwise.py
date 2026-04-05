"""Alternative pairwise scores (cross-correlation, regression, Granger, Lévy) on aligned series."""

from __future__ import annotations

from enum import Enum
from typing import Tuple

import numpy as np


def _corr(x: np.ndarray, y: np.ndarray) -> float:
    x = x - np.nanmean(x)
    y = y - np.nanmean(y)
    d = np.nanstd(x) * np.nanstd(y)
    if d <= 0 or np.isnan(d):
        return np.nan
    return float(np.nanmean(x * y) / d)


def leadlag_score_regression(
    leader: np.ndarray,
    follower: np.ndarray,
    max_lag: int = 5,
) -> Tuple[float, int]:
    """Maximize R² of follower[t] ~ leader[t−lag] (single lag)."""
    best, best_lag = -np.inf, 1
    T = len(leader)
    for lag in range(1, max_lag + 1):
        if T - lag <= 5:
            break
        x = leader[: T - lag]
        y = follower[lag:]
        ok = np.isfinite(x) & np.isfinite(y)
        if ok.sum() < 10:
            continue
        xv, yv = x[ok], y[ok]
        xc, yc = xv - np.mean(xv), yv - np.mean(yv)
        sxx = float(np.sum(xc**2))
        if sxx <= 0:
            continue
        beta = float(np.sum(xc * yc) / sxx)
        pred = beta * xc
        sse = float(np.sum((yc - pred) ** 2))
        sst = float(np.sum(yc**2))
        if sst <= 0:
            continue
        r2 = 1.0 - sse / sst
        if r2 > best:
            best, best_lag = r2, lag
    if best == -np.inf:
        return np.nan, 1
    return float(best), best_lag


def leadlag_score_levy(
    leader: np.ndarray,
    follower: np.ndarray,
    max_lag: int = 5,
) -> Tuple[float, int]:
    """Maximize |Lévy area| on aligned segments at each lag (path-ordering strength)."""
    best, best_lag = -np.inf, 1
    T = len(leader)
    for lag in range(1, max_lag + 1):
        if T - lag <= 5:
            break
        x = leader[: T - lag]
        y = follower[lag:]
        L = abs(levy_area_stat(x, y))
        if np.isnan(L):
            continue
        if L > best:
            best, best_lag = L, lag
    if best == -np.inf:
        return np.nan, 1
    return float(best), best_lag


def leadlag_score_corr(leader: np.ndarray, follower: np.ndarray, max_lag: int = 5) -> Tuple[float, int]:
    best, best_lag = -np.inf, 1
    T = len(leader)
    for lag in range(1, max_lag + 1):
        if T - lag <= 5:
            break
        c = _corr(leader[: T - lag], follower[lag:])
        if np.isnan(c):
            continue
        if c > best:
            best, best_lag = c, lag
    if best == -np.inf:
        return np.nan, 1
    return best, best_lag


def levy_area_stat(x: np.ndarray, y: np.ndarray) -> float:
    ok = np.isfinite(x) & np.isfinite(y)
    if ok.sum() < 5:
        return np.nan
    x = x[ok] - np.nanmean(x[ok])
    y = y[ok] - np.nanmean(y[ok])
    dx, dy = np.diff(x), np.diff(y)
    xm, ym = x[:-1], y[:-1]
    L = 0.5 * np.sum(xm * dy - ym * dx)
    return float(L / (len(x) + 1e-12))


class PairwiseMethod(str, Enum):
    """Labels aligned with :data:`supply_chain_leadlag.matrix.EdgeScoreMethod` (pairwise subset)."""

    CROSS_CORR = "cross_corr"
    REGRESSION_R2 = "regression_r2"
    GRANGER = "granger"
    LEVY = "levy"


def _ols_sse(y: np.ndarray, X: np.ndarray) -> Tuple[float, int]:
    b, _, _, _ = np.linalg.lstsq(X, y, rcond=None)
    r = y - X @ b
    return float(np.sum(r**2)), X.shape[1]


def leadlag_score_granger(
    leader: np.ndarray,
    follower: np.ndarray,
    max_lag: int = 5,
    n_lags: int = 2,
) -> Tuple[float, int]:
    best_f, best_lag = -np.inf, 1
    T = len(leader)
    L = min(n_lags, 3)
    for lag in range(1, max_lag + 1):
        start = max(lag, L)
        if T - start <= L + 15:
            continue
        y = follower[start:T]
        nlen = len(y)
        Xr = np.ones((nlen, L + 1))
        Xf = np.ones((nlen, L + 2))
        for ell in range(1, L + 1):
            Xr[:, ell] = follower[start - ell : T - ell]
            Xf[:, ell] = follower[start - ell : T - ell]
        Xf[:, L + 1] = leader[start - lag : T - lag]
        ok = np.isfinite(y) & np.all(np.isfinite(Xr), axis=1) & np.all(np.isfinite(Xf), axis=1)
        if ok.sum() < 15:
            continue
        yv, Xr, Xf = y[ok], Xr[ok], Xf[ok]
        sse_r, kr = _ols_sse(yv, Xr)
        sse_f, kf = _ols_sse(yv, Xf)
        df1, df2 = kf - kr, len(yv) - kf
        if df1 <= 0 or df2 <= 0 or sse_f <= 0:
            continue
        f_stat = ((sse_r - sse_f) / df1) / (sse_f / df2)
        if f_stat > best_f:
            best_f, best_lag = f_stat, lag
    if best_f == -np.inf:
        return np.nan, 1
    return float(best_f), best_lag
