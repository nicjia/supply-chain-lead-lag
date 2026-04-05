"""
Rolling long–short backtest: PIT edges + trailing return window → C → ranks → portfolio.

At rebalance date T, uses returns through T and edges with `date <= T` only. Weights apply
from the first trading day strictly after T (no same-day trading on information dated T).

Optional **baselines** (same rebalances, same universe, same q): random ranks, trailing
return momentum, structure-only leadingness, equal-weight long-only on the network.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import numpy as np
import pandas as pd

from supply_chain_leadlag.global_structure import global_rank_spectral_df
from supply_chain_leadlag.matrix import (
    EdgeScoreMethod,
    LeadLagResult,
    build_lead_lag_matrix_gvkey,
    structural_C_from_edges,
)


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


def momentum_scores(R_win: pd.DataFrame, window: int) -> pd.Series:
    """Sum of simple returns over the last `window` rows (cross-sectional momentum)."""
    w = min(window, len(R_win))
    if w < 1:
        return pd.Series(dtype=float)
    return R_win.iloc[-w:].sum(axis=0, skipna=True)


def shuffle_scores(scores: pd.Series, rng: np.random.Generator) -> pd.Series:
    v = scores.to_numpy(dtype=float).copy()
    rng.shuffle(v)
    return pd.Series(v, index=scores.index)


def structural_leadingness_scores(e_pit: pd.DataFrame, nodes: list[str]) -> pd.Series:
    """Row-sum of S = C − Cᵀ from pure supply weights (no return-based C)."""
    C_sup = structural_C_from_edges(e_pit, nodes)
    S_sup = C_sup - C_sup.T
    return S_sup.sum(axis=1)


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


def equal_weight_universe(scores: pd.Series) -> pd.Series:
    """Long-only equal weight on the same node set as `scores.index` (sums to 1)."""
    s = scores.dropna()
    if len(s) < 1:
        return pd.Series(dtype=float)
    n = len(s)
    w = pd.Series(1.0 / n, index=s.index)
    return w


def expand_to_columns(w: pd.Series, R: pd.DataFrame) -> pd.Series:
    w_full = pd.Series(0.0, index=R.columns, dtype=float)
    return w_full.add(w, fill_value=0.0).reindex(R.columns).fillna(0.0)


@dataclass
class BacktestResult:
    daily_ret: pd.Series
    cumulative: pd.Series
    rebalance_log: pd.DataFrame


def _daily_returns_from_events(
    R: pd.DataFrame,
    events: list[tuple[pd.Timestamp, pd.Series]],
    *,
    name: str = "ret",
) -> BacktestResult:
    R = R.sort_index()
    idx = R.index
    events = sorted(events, key=lambda x: x[0])
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

    sret = pd.Series(daily_rets, index=pd.DatetimeIndex(daily_dates), name=name)
    cum = (1.0 + sret.fillna(0.0)).cumprod() - 1.0
    log_df = pd.DataFrame(log_rows) if log_rows else pd.DataFrame()
    return BacktestResult(daily_ret=sret, cumulative=cum, rebalance_log=log_df)


@dataclass
class ComparisonResult:
    """Main lead–lag long–short plus baselines on the same calendar."""

    main: BacktestResult
    random: BacktestResult
    momentum: BacktestResult
    structural: BacktestResult
    equal_weight: BacktestResult


def run_rolling_comparison(
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
    momentum_window: int = 20,
    baseline_seed: int = 0,
    include_baselines: bool = True,
) -> ComparisonResult:
    """
    Same as `run_rolling_long_short` for **main**, plus baselines when `include_baselines`:

    - **random**: same long–short rule on a random permutation of the main scores.
    - **momentum**: long–short on trailing-sum returns over `momentum_window` days.
    - **structural**: long–short on leadingness from `structural_C_from_edges` only.
    - **equal_weight**: long-only equal weight on the same network nodes (not dollar-neutral).
    """
    R = R.sort_index()
    idx = R.index
    start = idx.min()
    end = idx.max()
    rebalances = pd.bdate_range(start, end, freq=rebalance_freq)
    rebalances = rebalances[rebalances >= idx[min(lookback_rows, len(idx) - 1)]]
    if max_rebalances is not None:
        rebalances = rebalances[: max_rebalances]

    events_main: list[tuple[pd.Timestamp, pd.Series]] = []
    events_rand: list[tuple[pd.Timestamp, pd.Series]] = []
    events_mom: list[tuple[pd.Timestamp, pd.Series]] = []
    events_str: list[tuple[pd.Timestamp, pd.Series]] = []
    events_ew: list[tuple[pd.Timestamp, pd.Series]] = []

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
        w_main = long_short_weights(sc, q=q, long_high=long_high)
        if w_main.empty or w_main.abs().sum() == 0:
            continue

        wm = expand_to_columns(w_main, R)
        events_main.append((T, wm))

        if include_baselines:
            rng = np.random.default_rng((baseline_seed + int(pd.Timestamp(T).value)) % (2**32))
            w_rand = long_short_weights(shuffle_scores(sc, rng), q=q, long_high=long_high)
            if not w_rand.empty:
                events_rand.append((T, expand_to_columns(w_rand, R)))

            sc_mom = momentum_scores(R_win, momentum_window).reindex(sc.index).fillna(0.0)
            w_mom = long_short_weights(sc_mom, q=q, long_high=long_high)
            if not w_mom.empty:
                events_mom.append((T, expand_to_columns(w_mom, R)))

            nodes = list(res.C.index)
            sc_str = structural_leadingness_scores(e_pit, nodes).reindex(sc.index)
            sc_str = sc_str.fillna(0.0)
            w_str = long_short_weights(sc_str, q=q, long_high=long_high)
            if not w_str.empty:
                events_str.append((T, expand_to_columns(w_str, R)))

            w_ew = equal_weight_universe(sc)
            if not w_ew.empty:
                events_ew.append((T, expand_to_columns(w_ew, R)))

    main_bt = _daily_returns_from_events(R, events_main, name="main")
    if not include_baselines:
        empty = BacktestResult(
            daily_ret=pd.Series(dtype=float, name="empty"),
            cumulative=pd.Series(dtype=float),
            rebalance_log=pd.DataFrame(),
        )
        return ComparisonResult(
            main=main_bt,
            random=empty,
            momentum=empty,
            structural=empty,
            equal_weight=empty,
        )

    return ComparisonResult(
        main=main_bt,
        random=_daily_returns_from_events(R, events_rand, name="random"),
        momentum=_daily_returns_from_events(R, events_mom, name="momentum"),
        structural=_daily_returns_from_events(R, events_str, name="structural"),
        equal_weight=_daily_returns_from_events(R, events_ew, name="equal_weight"),
    )


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
    """Lead–lag long–short only; skips baseline portfolios (faster)."""
    return run_rolling_comparison(
        R,
        edges,
        lookback_rows=lookback_rows,
        rebalance_freq=rebalance_freq,
        score=score,
        rank_method=rank_method,
        q=q,
        long_high=long_high,
        min_obs=min_obs,
        horizon=horizon,
        max_lag=max_lag,
        granger_n_lags=granger_n_lags,
        winsor_q=winsor_q,
        max_rebalances=max_rebalances,
        include_baselines=False,
    ).main


def comparison_metrics_table(comp: ComparisonResult) -> pd.DataFrame:
    """Sharpe, mean daily, etc., for each leg (uses `portfolio_metrics`)."""
    from supply_chain_leadlag.signals import portfolio_metrics

    rows = []
    for label, res in [
        ("main", comp.main),
        ("random", comp.random),
        ("momentum", comp.momentum),
        ("structural", comp.structural),
        ("equal_weight", comp.equal_weight),
    ]:
        if res.daily_ret.empty or res.daily_ret.notna().sum() == 0:
            rows.append({"strategy": label, "sharpe": np.nan, "n_days": 0})
            continue
        m = portfolio_metrics(res.daily_ret)
        m = {"strategy": label, **m}
        rows.append(m)
    return pd.DataFrame(rows)


DEFAULT_SCORE_GRID: tuple[str, ...] = (
    "tstat_diff",
    "beta_diff",
    "cross_corr",
    "regression_r2",
    "granger",
    "levy",
)


def grid_search_main_backtest(
    R: pd.DataFrame,
    edges: pd.DataFrame,
    *,
    scores: list[str] | None = None,
    rank_methods: list[Literal["leadingness", "spectral"]] | None = None,
    **kwargs,
) -> pd.DataFrame:
    """
    Sweep **(A)** how each directed edge is scored when building \(C\) (`score` in
    :func:`build_lead_lag_matrix_gvkey`) × **(B)** how node scores are derived from \(C\)
    (`rank_method`: row-sum leadingness vs spectral GlobalRank).

    Each cell runs a full rolling backtest (main leg only) and records `portfolio_metrics`.
    Use `max_rebalances` in `kwargs` to cap cost while searching.

    Returns a DataFrame sorted by Sharpe descending (failed rows last).
    """
    from supply_chain_leadlag.signals import portfolio_metrics

    if scores is None:
        scores = list(DEFAULT_SCORE_GRID)
    if rank_methods is None:
        rank_methods = ["leadingness", "spectral"]

    rows: list[dict] = []
    for score in scores:
        for rank_method in rank_methods:
            rm: Literal["leadingness", "spectral"] = (
                rank_method if rank_method in ("leadingness", "spectral") else "leadingness"
            )
            try:
                comp = run_rolling_comparison(
                    R,
                    edges,
                    score=score,  # type: ignore[arg-type]
                    rank_method=rm,
                    include_baselines=False,
                    **kwargs,
                )
                m = portfolio_metrics(comp.main.daily_ret)
                rows.append(
                    {
                        "score": score,
                        "rank_method": rm,
                        "n_rebalances": int(len(comp.main.rebalance_log)),
                        **m,
                    }
                )
            except Exception as exc:
                rows.append(
                    {
                        "score": score,
                        "rank_method": rm,
                        "sharpe": np.nan,
                        "mean_daily": np.nan,
                        "vol_daily": np.nan,
                        "cum_return": np.nan,
                        "max_drawdown": np.nan,
                        "n_days": 0,
                        "n_rebalances": 0,
                        "error": str(exc),
                    }
                )

    df = pd.DataFrame(rows)
    if "sharpe" in df.columns and df["sharpe"].notna().any():
        df = df.sort_values("sharpe", ascending=False, na_position="last")
    return df.reset_index(drop=True)
