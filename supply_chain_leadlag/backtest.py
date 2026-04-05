"""
Rolling long–short backtest: PIT edges + trailing return window → C → ranks → portfolio.

At rebalance date T, uses returns through T and edges with `date <= T` only. Weights apply
from the first trading day strictly after T (no same-day trading on information dated T).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import numpy as np
import pandas as pd

from supply_chain_leadlag.global_structure import global_rank_spectral_df
from supply_chain_leadlag.matrix import EdgeScoreMethod, LeadLagResult, build_lead_lag_matrix_gvkey


def filter_edges_pit(edges: pd.DataFrame, asof: pd.Timestamp) -> pd.DataFrame:
    asof = pd.Timestamp(asof)
    return edges.loc[edges["date"] <= asof].copy()


def returns_window(R: pd.DataFrame, end: pd.Timestamp, n_rows: int) -> pd.DataFrame:
    end = pd.Timestamp(end)
    sub = R.loc[:end]
    if len(sub) < n_rows:
        return pd.DataFrame()
    return sub.iloc[-n_rows:].copy()


def scores_from_result(
    res: LeadLagResult,
    method: Literal["leadingness", "spectral"] = "leadingness",
) -> pd.Series:
    if method == "leadingness":
        return res.leadingness.sort_index()
    return global_rank_spectral_df(res.C).sort_index()


def long_short_weights(
    scores: pd.Series,
    q: float = 0.2,
    *,
    long_high: bool = True,
) -> pd.Series:
    s = scores.dropna()
    if len(s) < max(20, int(2 / max(q, 1e-6))):
        return pd.Series(dtype=float)
    order = s.sort_values(ascending=False)
    k = max(1, int(len(s) * q))
    if long_high:
        long_ix = order.head(k).index
        short_ix = order.tail(k).index
    else:
        long_ix = order.tail(k).index
        short_ix = order.head(k).index
    w = pd.Series(0.0, index=s.index)
    w.loc[long_ix] = 1.0 / k
    w.loc[short_ix] = -1.0 / k
    return w


@dataclass
class BacktestResult:
    daily_ret: pd.Series
    cumulative: pd.Series
    rebalance_log: pd.DataFrame


def run_rolling_long_short(
    R: pd.DataFrame,
    edges: pd.DataFrame,
    *,
    lookback_rows: int = 504,
    rebalance_freq: str = "BME",
    score: EdgeScoreMethod = "tstat_diff",
    rank_method: Literal["leadingness", "spectral"] = "leadingness",
    q: float = 0.2,
    long_high: bool = True,
    min_obs: int = 80,
    horizon: int = 1,
    max_lag: int = 5,
    granger_n_lags: int = 2,
    winsor_q: float | None = 0.001,
    max_rebalances: int | None = None,
) -> BacktestResult:
    """
    Business-month (`BM`) rebalances by default. Builds C at each rebalance T with
    `R.loc[:T]` trailing `lookback_rows` rows and PIT edges. Weights take effect on the
    first trading day **strictly after** T.
    """
    R = R.sort_index()
    idx = R.index
    start = idx.min()
    end = idx.max()
    rebalances = pd.bdate_range(start, end, freq=rebalance_freq)
    rebalances = rebalances[rebalances >= idx[min(lookback_rows, len(idx) - 1)]]
    if max_rebalances is not None:
        rebalances = rebalances[: max_rebalances]

    events: list[tuple[pd.Timestamp, pd.Series]] = []
    for T in rebalances:
        e_pit = filter_edges_pit(edges, T)
        R_win = returns_window(R, T, lookback_rows)
        if R_win.empty or len(e_pit) < 5:
            continue
        try:
            res = build_lead_lag_matrix_gvkey(
                R_win,
                e_pit,
                horizon=horizon,
                min_obs=min_obs,
                winsor_q=winsor_q,
                score=score,
                max_lag=max_lag,
                granger_n_lags=granger_n_lags,
            )
        except ValueError:
            continue
        sc = scores_from_result(res, method=rank_method)
        w = long_short_weights(sc, q=q, long_high=long_high)
        if w.empty or w.abs().sum() == 0:
            continue
        w_full = pd.Series(0.0, index=R.columns, dtype=float)
        w_full = w_full.add(w, fill_value=0.0).reindex(R.columns).fillna(0.0)
        events.append((T, w_full))

    events.sort(key=lambda x: x[0])
    log_rows: list[dict] = []
    for T, wf in events:
        log_rows.append(
            {
                "rebalance": T,
                "n_long": int((wf > 0).sum()),
                "n_short": int((wf < 0).sum()),
            }
        )

    daily_rets: list[float] = []
    daily_dates: list[pd.Timestamp] = []
    ev_i = 0
    current_w: pd.Series | None = None

    for d in idx:
        d = pd.Timestamp(d)
        while ev_i < len(events) and events[ev_i][0] < d:
            current_w = events[ev_i][1]
            ev_i += 1
        if current_w is None:
            daily_rets.append(np.nan)
            daily_dates.append(d)
            continue
        r_row = R.loc[d]
        pr = float((current_w * r_row.reindex(current_w.index).fillna(0.0)).sum())
        daily_rets.append(pr if np.isfinite(pr) else np.nan)
        daily_dates.append(d)

    sret = pd.Series(daily_rets, index=pd.DatetimeIndex(daily_dates), name="strategy_ret")
    cum = (1.0 + sret.fillna(0.0)).cumprod() - 1.0
    log_df = pd.DataFrame(log_rows) if log_rows else pd.DataFrame()
    return BacktestResult(daily_ret=sret, cumulative=cum, rebalance_log=log_df)
