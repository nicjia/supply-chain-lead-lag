"""
Strategy family interface: supplier_pressure, globalrank, metacluster, clusterrank.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from supply_chain_leadlag.backtest import (
    expand_to_columns,
    long_short_weights,
    scores_from_result,
    supplier_pressure_signal,
)
from supply_chain_leadlag.clusterrank_strategy import clusterrank_daily_weights
from supply_chain_leadlag.matrix import LeadLagResult, hybrid_matrix, leadlag_result_replace_C, structural_C_from_edges
from supply_chain_leadlag.metacluster_strategy import metacluster_daily_weights


def _supplier_nodes(C: pd.DataFrame) -> list[str]:
    """Tradable suppliers = columns of C (customer -> supplier edges)."""
    return list(C.columns.astype(str))


def _daily_returns_from_weight_events(
    R: pd.DataFrame,
    events: list[tuple[pd.Timestamp, pd.Series]],
    *,
    apply_next_day: bool = True,
) -> pd.Series:
    """Weights at t earn return at t+1 when ``apply_next_day`` (default PIT convention)."""
    R = R.sort_index()
    idx = R.index
    out = pd.Series(0.0, index=idx, dtype=float)
    events = sorted(events, key=lambda x: x[0])
    for i, (T, w) in enumerate(events):
        if apply_next_day:
            pos = idx.get_indexer([T], method="pad")[0]
            if pos < 0 or pos + 1 >= len(idx):
                continue
            earn_dates = idx[pos + 1 :]
            if i + 1 < len(events):
                next_t = events[i + 1][0]
                earn_dates = earn_dates[earn_dates <= next_t]
        else:
            earn_dates = idx[idx >= T]
            if i + 1 < len(events):
                earn_dates = earn_dates[earn_dates < events[i + 1][0]]
        if len(earn_dates) == 0:
            continue
        w = w.reindex(R.columns).fillna(0.0)
        for d in earn_dates:
            out.loc[d] = float((w * R.loc[d]).sum())
    return out


def run_strategy_family(
    family: str,
    C: pd.DataFrame,
    returns_window: pd.DataFrame,
    returns_forward: pd.DataFrame,
    edges_pit: pd.DataFrame,
    cluster_labels: pd.Series | None = None,
    q: float = 0.2,
    *,
    apply_next_day: bool = True,
    hybrid_alpha: float | None = None,
    globalrank_method: str = "spectral",
    n_clusters: int = 10,
    cluster_random_state: int = 0,
    res: LeadLagResult | None = None,
    **kwargs: Any,
) -> dict:
    """
    Run one strategy family for one rebalance window.

    Returns dict with: daily_returns, weights (DataFrame), scores (DataFrame or Series), metadata.
    """
    del kwargs
    C_use = C.copy()
    if hybrid_alpha is not None:
        nodes = sorted(set(C.index).union(C.columns))
        C_sup = structural_C_from_edges(edges_pit, nodes)
        C_use = hybrid_matrix(C_use, C_sup, float(hybrid_alpha))

    events: list[tuple[pd.Timestamp, pd.Series]] = []
    meta: dict[str, Any] = {"family": family, "hybrid_alpha": hybrid_alpha}

    if family == "supplier_pressure":
        suppliers = _supplier_nodes(C_use)
        for d in returns_forward.index:
            r_d = returns_forward.loc[d]
            sig = supplier_pressure_signal(C_use, r_d)
            sig = sig.reindex(suppliers).dropna()
            w = long_short_weights(sig, q=q, long_high=True)
            if w.empty:
                continue
            events.append((pd.Timestamp(d), expand_to_columns(w, returns_forward)))
        if len(returns_forward):
            score_rows = [
                supplier_pressure_signal(C_use, returns_forward.loc[d]).reindex(suppliers)
                for d in returns_forward.index
            ]
            scores_df = pd.DataFrame(score_rows, index=returns_forward.index)
        else:
            scores_df = pd.DataFrame()
        meta["traded_universe"] = "suppliers_only"

    elif family == "globalrank":
        if res is None:
            raise ValueError("globalrank requires LeadLagResult `res`")
        # Always rank from C_use (pure data, blended, or supply-only when caller pre-blends C).
        C_res = leadlag_result_replace_C(res, C_use)
        sc = scores_from_result(
            C_res,
            method=globalrank_method,  # type: ignore[arg-type]
            n_clusters=n_clusters,
            cluster_random_state=cluster_random_state,
        )
        w = long_short_weights(sc, q=q, long_high=True)
        if not w.empty and len(returns_forward) > 0:
            T0 = returns_forward.index[0]
            events.append((pd.Timestamp(T0), expand_to_columns(w, returns_forward)))
        scores_df = sc.to_frame("score").T
        meta["globalrank_method"] = globalrank_method

    elif family == "metacluster":
        if cluster_labels is None:
            raise ValueError("metacluster requires cluster_labels")
        w_df, sc_df, F = metacluster_daily_weights(
            C_use, returns_forward, cluster_labels, q=q
        )
        meta["meta_flow_matrix"] = F
        for d, row in w_df.iterrows():
            w = row[row.abs() > 1e-12]
            if w.empty:
                continue
            events.append((pd.Timestamp(d), expand_to_columns(w, returns_forward)))
        scores_df = sc_df

    elif family == "clusterrank":
        if cluster_labels is None:
            raise ValueError("clusterrank requires cluster_labels")
        w_df, sc_df = clusterrank_daily_weights(C_use, returns_forward, cluster_labels, q=q)
        for d, row in w_df.iterrows():
            w = row[row.abs() > 1e-12]
            if w.empty:
                continue
            events.append((pd.Timestamp(d), expand_to_columns(w, returns_forward)))
        scores_df = sc_df

    else:
        raise ValueError(f"Unknown strategy family {family!r}")

    daily = _daily_returns_from_weight_events(
        returns_forward,
        events,
        apply_next_day=apply_next_day,
    )
    weights_df = pd.DataFrame(
        {str(t): ev for t, ev in events}
    ).T if events else pd.DataFrame()

    return {
        "daily_returns": daily,
        "weights": weights_df,
        "scores": scores_df,
        "metadata": meta,
        "events": events,
    }
