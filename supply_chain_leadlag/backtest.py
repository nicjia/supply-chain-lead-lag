"""
Rolling long–short backtest: PIT edges + trailing return window → C → ranks → portfolio.

At rebalance date T, uses returns through T and edges with `date <= T` only. Weights apply
from the first trading day strictly after T (no same-day trading on information dated T).

Optional **baselines** (same rebalances, same universe, same q): random ranks, trailing
return momentum, structure-only leadingness, equal-weight long-only on the network.
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass
from itertools import product
from typing import Literal

import numpy as np
import pandas as pd

from supply_chain_leadlag.global_structure import cluster_rank_series, global_rank_spectral_df
from supply_chain_leadlag.matrix import (
    EdgeScoreMethod,
    LeadLagResult,
    build_lead_lag_matrix_gvkey,
    hybrid_matrix,
    leadlag_result_replace_C,
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


RankMethod = Literal["leadingness", "spectral", "cluster", "cluster_eigen"]


def scores_from_result(
    res: LeadLagResult,
    method: RankMethod = "leadingness",
    *,
    n_clusters: int = 4,
    cluster_random_state: int = 0,
) -> pd.Series:
    """
    Turn estimated C into a cross-sectional score per node.

    - **leadingness:** row-sum of S = C − Cᵀ (local net “lead” from skew part).
    - **spectral:** GlobalRank — leading eigenvector of H = i(C − Cᵀ).
    - **cluster:** MetaCluster + ClusterRank with local row-sums inside clusters.
    - **cluster_eigen:** same clusters, local spectral rank within each cluster.
    """
    if method == "leadingness":
        return res.leadingness.sort_index()
    if method == "spectral":
        return global_rank_spectral_df(res.C).sort_index()
    if method == "cluster":
        return cluster_rank_series(
            res.C,
            local="row_sum",
            n_clusters=n_clusters,
            random_state=cluster_random_state,
        ).sort_index()
    if method == "cluster_eigen":
        return cluster_rank_series(
            res.C,
            local="eigen",
            n_clusters=n_clusters,
            random_state=cluster_random_state,
        ).sort_index()
    raise ValueError(f"Unknown rank_method {method!r}")


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
    gross_daily_ret: pd.Series
    daily_cost: pd.Series
    turnover: pd.Series
    cumulative: pd.Series
    rebalance_log: pd.DataFrame


def _daily_returns_from_events(
    R: pd.DataFrame,
    events: list[tuple[pd.Timestamp, pd.Series]],
    *,
    name: str = "ret",
    commission_bps: float = 0.0,
    slippage_bps: float = 0.0,
    borrow_bps_annual: float = 0.0,
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
    gross_daily_rets: list[float] = []
    daily_costs: list[float] = []
    daily_turnover: list[float] = []
    daily_dates: list[pd.Timestamp] = []
    ev_i = 0
    current_w: pd.Series | None = None
    prev_w: pd.Series | None = None

    for d in idx:
        d = pd.Timestamp(d)
        turnover = 0.0
        while ev_i < len(events) and events[ev_i][0] < d:
            current_w = events[ev_i][1]
            if prev_w is None:
                turnover += 0.5 * float(current_w.abs().sum())
            else:
                aligned_prev = prev_w.reindex(current_w.index).fillna(0.0)
                turnover += 0.5 * float((current_w - aligned_prev).abs().sum())
            prev_w = current_w.copy()
            ev_i += 1
        if current_w is None:
            daily_rets.append(np.nan)
            gross_daily_rets.append(np.nan)
            daily_costs.append(0.0)
            daily_turnover.append(0.0)
            daily_dates.append(d)
            continue
        r_row = R.loc[d]
        gross = float((current_w * r_row.reindex(current_w.index).fillna(0.0)).sum())
        short_notional = float((-current_w.clip(upper=0.0)).sum())
        turnover_cost = turnover * (commission_bps + slippage_bps) / 1e4
        borrow_cost = short_notional * borrow_bps_annual / 1e4 / 252.0
        total_cost = turnover_cost + borrow_cost
        net = gross - total_cost
        gross_daily_rets.append(gross if np.isfinite(gross) else np.nan)
        daily_rets.append(net if np.isfinite(net) else np.nan)
        daily_costs.append(total_cost)
        daily_turnover.append(turnover)
        daily_dates.append(d)

    sret = pd.Series(daily_rets, index=pd.DatetimeIndex(daily_dates), name=name)
    gross_ret = pd.Series(gross_daily_rets, index=pd.DatetimeIndex(daily_dates), name=f"{name}_gross")
    cost_ser = pd.Series(daily_costs, index=pd.DatetimeIndex(daily_dates), name=f"{name}_cost")
    turnover_ser = pd.Series(daily_turnover, index=pd.DatetimeIndex(daily_dates), name=f"{name}_turnover")
    cum = (1.0 + sret.fillna(0.0)).cumprod() - 1.0
    log_df = pd.DataFrame(log_rows) if log_rows else pd.DataFrame()
    return BacktestResult(
        daily_ret=sret,
        gross_daily_ret=gross_ret,
        daily_cost=cost_ser,
        turnover=turnover_ser,
        cumulative=cum,
        rebalance_log=log_df,
    )


def _normalize_long_short_weights(w: pd.Series) -> pd.Series:
    if w.empty:
        return w
    out = w.copy()
    long_sum = float(out.clip(lower=0.0).sum())
    short_sum = float((-out.clip(upper=0.0)).sum())
    if long_sum > 0:
        out[out > 0] = out[out > 0] / long_sum
    if short_sum > 0:
        out[out < 0] = out[out < 0] / short_sum
    if long_sum > 0 and short_sum > 0:
        out = out - out.mean()
    return out


def _estimate_betas(
    R_win: pd.DataFrame,
    market_ret: pd.Series,
    beta_lookback_rows: int,
) -> pd.Series:
    if market_ret.empty:
        return pd.Series(dtype=float)
    m = market_ret.reindex(R_win.index).dropna()
    if m.empty:
        return pd.Series(dtype=float)
    m = m.iloc[-min(beta_lookback_rows, len(m)) :]
    if len(m) < 10 or float(m.var()) <= 1e-12:
        return pd.Series(dtype=float)
    X = m - m.mean()
    var_x = float((X * X).sum())
    out: dict[str, float] = {}
    for c in R_win.columns:
        y = R_win[c].reindex(m.index).dropna()
        if len(y) < 10:
            continue
        z = (y - y.mean()).reindex(m.index).fillna(0.0)
        out[c] = float((z * X).sum() / var_x)
    return pd.Series(out, dtype=float)


def apply_risk_overlays(
    w: pd.Series,
    *,
    max_abs_weight: float | None = None,
    beta_neutralize: bool = False,
    betas: pd.Series | None = None,
    sector_neutralize: bool = False,
    sector_map: pd.Series | None = None,
) -> pd.Series:
    out = w.copy()
    if out.empty:
        return out

    if sector_neutralize and sector_map is not None and not sector_map.empty:
        aligned_sector = sector_map.reindex(out.index)
        for _, ix in aligned_sector.dropna().groupby(aligned_sector.dropna()).groups.items():
            grp = out.loc[ix]
            out.loc[ix] = grp - grp.mean()

    if max_abs_weight is not None and max_abs_weight > 0:
        out = out.clip(lower=-max_abs_weight, upper=max_abs_weight)

    out = _normalize_long_short_weights(out)
    if beta_neutralize and betas is not None and not betas.empty:
        b = betas.reindex(out.index).fillna(0.0)
        denom = float((b * b).sum())
        if denom > 1e-12:
            lam = float((out * b).sum()) / denom
            out = out - lam * b
            if max_abs_weight is not None and max_abs_weight > 0:
                out = out.clip(lower=-max_abs_weight, upper=max_abs_weight)
    return out


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
    rank_method: RankMethod = "leadingness",
    n_clusters: int = 4,
    cluster_random_state: int = 0,
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
    hybrid_alpha: float | None = None,
    commission_bps: float = 0.0,
    slippage_bps: float = 0.0,
    borrow_bps_annual: float = 0.0,
    max_abs_weight: float | None = None,
    beta_neutralize: bool = False,
    beta_lookback_rows: int = 252,
    market_ret: pd.Series | None = None,
    sector_neutralize: bool = False,
    sector_map: pd.Series | None = None,
    show_progress: bool = False,
) -> ComparisonResult:
    """
    Same as `run_rolling_long_short` for **main**, plus baselines when `include_baselines`:

    - **random**: same long–short rule on a random permutation of the main scores.
    - **momentum**: long–short on trailing-sum returns over `momentum_window` days.
    - **structural**: long–short on leadingness from `structural_C_from_edges` only.
    - **equal_weight**: long-only equal weight on the same network nodes (not dollar-neutral).

    If ``hybrid_alpha`` is set in ``[0, 1]``, the **main** leg ranks from
    ``α·C_data + (1−α)·C_supply`` (see :func:`~supply_chain_leadlag.matrix.hybrid_matrix`);
    baselines are unchanged.

    If ``show_progress`` is True, shows a tqdm bar over rebalance dates (requires tqdm).
    """
    R = R.sort_index()
    idx = R.index
    start = idx.min()
    end = idx.max()
    rebalances = pd.bdate_range(start, end, freq=rebalance_freq)
    rebalances = rebalances[rebalances >= idx[min(lookback_rows, len(idx) - 1)]]
    if max_rebalances is not None:
        rebalances = rebalances[: max_rebalances]

    if show_progress:
        from tqdm.auto import tqdm

        rebalance_iter = tqdm(rebalances, desc="rebalances", unit="date")
    else:
        rebalance_iter = rebalances

    events_main: list[tuple[pd.Timestamp, pd.Series]] = []
    events_rand: list[tuple[pd.Timestamp, pd.Series]] = []
    events_mom: list[tuple[pd.Timestamp, pd.Series]] = []
    events_str: list[tuple[pd.Timestamp, pd.Series]] = []
    events_ew: list[tuple[pd.Timestamp, pd.Series]] = []

    for T in rebalance_iter:
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

        if hybrid_alpha is not None:
            nodes = list(res.C.index)
            C_sup = structural_C_from_edges(e_pit, nodes)
            cmax = float(np.nanmax(np.abs(res.C.to_numpy(dtype=float))))
            # If C_data ≈ 0, C_mix ∝ C_supply; leadingness ranks match structural → identical main vs structural.
            if cmax < 1e-15:
                warnings.warn(
                    "Return-based C is numerically zero (e.g. all edge asymmetry scores zero). "
                    "Hybrid main uses only supply weights, so with rank_method='leadingness' it "
                    "matches the structural baseline. Use a score with nonzero asymmetry "
                    "(e.g. tstat_diff, beta_diff) or omit hybrid_alpha.",
                    UserWarning,
                    stacklevel=2,
                )
            res = leadlag_result_replace_C(
                res, hybrid_matrix(res.C, C_sup, float(hybrid_alpha))
            )

        sc = scores_from_result(
            res,
            method=rank_method,
            n_clusters=n_clusters,
            cluster_random_state=cluster_random_state,
        )
        w_main = long_short_weights(sc, q=q, long_high=long_high)
        betas = _estimate_betas(R_win, market_ret, beta_lookback_rows) if (beta_neutralize and market_ret is not None) else None
        w_main = apply_risk_overlays(
            w_main,
            max_abs_weight=max_abs_weight,
            beta_neutralize=beta_neutralize,
            betas=betas,
            sector_neutralize=sector_neutralize,
            sector_map=sector_map,
        )
        if w_main.empty or w_main.abs().sum() == 0:
            continue

        wm = expand_to_columns(w_main, R)
        events_main.append((T, wm))

        if include_baselines:
            rng = np.random.default_rng((baseline_seed + int(pd.Timestamp(T).value)) % (2**32))
            w_rand = long_short_weights(shuffle_scores(sc, rng), q=q, long_high=long_high)
            w_rand = apply_risk_overlays(
                w_rand,
                max_abs_weight=max_abs_weight,
                beta_neutralize=beta_neutralize,
                betas=betas,
                sector_neutralize=sector_neutralize,
                sector_map=sector_map,
            )
            if not w_rand.empty:
                events_rand.append((T, expand_to_columns(w_rand, R)))

            sc_mom = momentum_scores(R_win, momentum_window).reindex(sc.index).fillna(0.0)
            w_mom = long_short_weights(sc_mom, q=q, long_high=long_high)
            w_mom = apply_risk_overlays(
                w_mom,
                max_abs_weight=max_abs_weight,
                beta_neutralize=beta_neutralize,
                betas=betas,
                sector_neutralize=sector_neutralize,
                sector_map=sector_map,
            )
            if not w_mom.empty:
                events_mom.append((T, expand_to_columns(w_mom, R)))

            nodes = list(res.C.index)
            sc_str = structural_leadingness_scores(e_pit, nodes).reindex(sc.index)
            sc_str = sc_str.fillna(0.0)
            w_str = long_short_weights(sc_str, q=q, long_high=long_high)
            w_str = apply_risk_overlays(
                w_str,
                max_abs_weight=max_abs_weight,
                beta_neutralize=beta_neutralize,
                betas=betas,
                sector_neutralize=sector_neutralize,
                sector_map=sector_map,
            )
            if not w_str.empty:
                events_str.append((T, expand_to_columns(w_str, R)))

            w_ew = equal_weight_universe(sc)
            if not w_ew.empty:
                events_ew.append((T, expand_to_columns(w_ew, R)))

    main_bt = _daily_returns_from_events(
        R,
        events_main,
        name="main",
        commission_bps=commission_bps,
        slippage_bps=slippage_bps,
        borrow_bps_annual=borrow_bps_annual,
    )
    if not include_baselines:
        empty = BacktestResult(
            daily_ret=pd.Series(dtype=float, name="empty"),
            gross_daily_ret=pd.Series(dtype=float, name="empty_gross"),
            daily_cost=pd.Series(dtype=float, name="empty_cost"),
            turnover=pd.Series(dtype=float, name="empty_turnover"),
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
        random=_daily_returns_from_events(
            R,
            events_rand,
            name="random",
            commission_bps=commission_bps,
            slippage_bps=slippage_bps,
            borrow_bps_annual=borrow_bps_annual,
        ),
        momentum=_daily_returns_from_events(
            R,
            events_mom,
            name="momentum",
            commission_bps=commission_bps,
            slippage_bps=slippage_bps,
            borrow_bps_annual=borrow_bps_annual,
        ),
        structural=_daily_returns_from_events(
            R,
            events_str,
            name="structural",
            commission_bps=commission_bps,
            slippage_bps=slippage_bps,
            borrow_bps_annual=borrow_bps_annual,
        ),
        equal_weight=_daily_returns_from_events(
            R,
            events_ew,
            name="equal_weight",
            commission_bps=commission_bps,
            slippage_bps=slippage_bps,
            borrow_bps_annual=borrow_bps_annual,
        ),
    )


def run_rolling_long_short(
    R: pd.DataFrame,
    edges: pd.DataFrame,
    *,
    lookback_rows: int = 504,
    rebalance_freq: str = "BME",
    score: EdgeScoreMethod = "tstat_diff",
    rank_method: RankMethod = "leadingness",
    n_clusters: int = 4,
    cluster_random_state: int = 0,
    q: float = 0.2,
    long_high: bool = True,
    min_obs: int = 80,
    horizon: int = 1,
    max_lag: int = 5,
    granger_n_lags: int = 2,
    winsor_q: float | None = 0.001,
    max_rebalances: int | None = None,
    hybrid_alpha: float | None = None,
    commission_bps: float = 0.0,
    slippage_bps: float = 0.0,
    borrow_bps_annual: float = 0.0,
    max_abs_weight: float | None = None,
    beta_neutralize: bool = False,
    beta_lookback_rows: int = 252,
    market_ret: pd.Series | None = None,
    sector_neutralize: bool = False,
    sector_map: pd.Series | None = None,
    show_progress: bool = False,
) -> BacktestResult:
    """Lead–lag long–short only; skips baseline portfolios (faster)."""
    return run_rolling_comparison(
        R,
        edges,
        lookback_rows=lookback_rows,
        rebalance_freq=rebalance_freq,
        score=score,
        rank_method=rank_method,
        n_clusters=n_clusters,
        cluster_random_state=cluster_random_state,
        q=q,
        long_high=long_high,
        min_obs=min_obs,
        horizon=horizon,
        max_lag=max_lag,
        granger_n_lags=granger_n_lags,
        winsor_q=winsor_q,
        max_rebalances=max_rebalances,
        include_baselines=False,
        hybrid_alpha=hybrid_alpha,
        commission_bps=commission_bps,
        slippage_bps=slippage_bps,
        borrow_bps_annual=borrow_bps_annual,
        max_abs_weight=max_abs_weight,
        beta_neutralize=beta_neutralize,
        beta_lookback_rows=beta_lookback_rows,
        market_ret=market_ret,
        sector_neutralize=sector_neutralize,
        sector_map=sector_map,
        show_progress=show_progress,
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
        m = portfolio_metrics(
            res.daily_ret,
            gross_daily_ret=res.gross_daily_ret,
            daily_cost=res.daily_cost,
            turnover=res.turnover,
        )
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
    rank_methods: list[str] | None = None,
    n_clusters_grid: list[int] | None = None,
    max_rebalances_grid: list[int | None] | None = None,
    show_progress: bool = True,
    **kwargs,
) -> pd.DataFrame:
    r"""
    Sweep (A) edge scoring for C (`score` in `build_lead_lag_matrix_gvkey`) × (B) global
    ranking from C: `leadingness`, `spectral` (GlobalRank), `cluster`, `cluster_eigen`
    (MetaCluster + ClusterRank; pass `n_clusters` / `cluster_random_state` in kwargs).

    Each cell runs a full rolling backtest (main leg only) and records `portfolio_metrics`.
    Use `max_rebalances` in `kwargs` to cap cost while searching.

    Optional ``n_clusters_grid`` / ``max_rebalances_grid`` (lists) add outer loops on those
    hyperparameters; if omitted, single values from ``kwargs`` are used (``n_clusters`` default 4,
    ``max_rebalances`` default ``None``).

    If ``show_progress`` is True, shows a tqdm bar over grid cells (requires tqdm).

    Returns a DataFrame sorted by Sharpe descending (failed rows last).
    """
    from supply_chain_leadlag.signals import portfolio_metrics
    from tqdm.auto import tqdm

    valid_rank: tuple[str, ...] = ("leadingness", "spectral", "cluster", "cluster_eigen")

    if scores is None:
        scores = list(DEFAULT_SCORE_GRID)
    if rank_methods is None:
        rank_methods = ["leadingness", "spectral"]

    kw = dict(kwargs)
    n_clusters_default = int(kw.pop("n_clusters", 4))
    mr_default = kw.pop("max_rebalances", None)
    nc_loop = n_clusters_grid if n_clusters_grid is not None else [n_clusters_default]
    mr_loop = max_rebalances_grid if max_rebalances_grid is not None else [mr_default]

    total = len(scores) * len(rank_methods) * len(nc_loop) * len(mr_loop)
    combos = product(scores, rank_methods, nc_loop, mr_loop)
    if show_progress:
        combos = tqdm(
            combos,
            total=total,
            desc="grid",
            unit="cell",
        )

    rows: list[dict] = []
    for score, rank_method, nc, mr in combos:
        if rank_method not in valid_rank:
            raise ValueError(f"rank_method must be one of {valid_rank}, got {rank_method!r}")
        rm: RankMethod = rank_method  # type: ignore[assignment]
        try:
            comp = run_rolling_comparison(
                R,
                edges,
                score=score,  # type: ignore[arg-type]
                rank_method=rm,
                include_baselines=False,
                n_clusters=int(nc),
                max_rebalances=mr,
                **kw,
            )
            m = portfolio_metrics(comp.main.daily_ret)
            rows.append(
                {
                    "score": score,
                    "rank_method": rm,
                    "n_clusters": int(nc),
                    "max_rebalances": mr,
                    "n_rebalances": int(len(comp.main.rebalance_log)),
                    **m,
                }
            )
        except Exception as exc:
            rows.append(
                {
                    "score": score,
                    "rank_method": rm,
                    "n_clusters": int(nc),
                    "max_rebalances": mr,
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
