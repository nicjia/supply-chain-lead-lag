"""
Pairwise lead–lag matrix C and skew part S = C − Cᵀ from supply-chain edges and returns.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Tuple

import numpy as np
import pandas as pd

from supply_chain_leadlag.pairwise import (
    leadlag_score_corr,
    leadlag_score_granger,
    leadlag_score_levy,
    leadlag_score_regression,
)

EdgeScoreMethod = Literal[
    "tstat_diff",
    "beta_diff",
    "cross_corr",
    "regression_r2",
    "granger",
    "levy",
]


def _ols_slope_tstat(x: np.ndarray, y: np.ndarray) -> Tuple[float, float, int]:
    """OLS y ~ a + b x. Returns (beta, tstat, nobs)."""
    mask = np.isfinite(x) & np.isfinite(y)
    x = x[mask]
    y = y[mask]
    n = int(x.size)
    if n < 10:
        return np.nan, np.nan, n

    x_mean = x.mean()
    y_mean = y.mean()
    x_c = x - x_mean
    y_c = y - y_mean
    sxx = float(np.dot(x_c, x_c))
    if sxx <= 1e-20:
        return np.nan, np.nan, n

    beta = float(np.dot(x_c, y_c) / sxx)
    y_hat = y_mean + beta * (x - x_mean)
    resid = y - y_hat
    dof = n - 2
    if dof <= 0:
        return beta, np.nan, n

    sigma2 = float(np.dot(resid, resid) / dof)
    se_beta = float(np.sqrt(sigma2 / sxx)) if sigma2 >= 0 else np.nan
    tstat = float(beta / se_beta) if (np.isfinite(se_beta) and se_beta > 0) else np.nan
    return beta, tstat, n


def _count_overlap(x: np.ndarray, y: np.ndarray) -> int:
    return int(np.sum(np.isfinite(x) & np.isfinite(y)))


def _edge_raw_asymmetry_pairwise(
    rc: np.ndarray,
    rs: np.ndarray,
    *,
    score: EdgeScoreMethod,
    max_lag: int,
    granger_n_lags: int,
) -> Tuple[float, int]:
    """
    Forward: customer leads supplier. Reverse: supplier leads customer.
    raw = forward_metric - reverse_metric (same definition as tstat/beta asymmetry).
    """
    if score == "cross_corr":
        fwd, _ = leadlag_score_corr(rc, rs, max_lag=max_lag)
        rev, _ = leadlag_score_corr(rs, rc, max_lag=max_lag)
    elif score == "regression_r2":
        fwd, _ = leadlag_score_regression(rc, rs, max_lag=max_lag)
        rev, _ = leadlag_score_regression(rs, rc, max_lag=max_lag)
    elif score == "granger":
        fwd, _ = leadlag_score_granger(rc, rs, max_lag=max_lag, n_lags=granger_n_lags)
        rev, _ = leadlag_score_granger(rs, rc, max_lag=max_lag, n_lags=granger_n_lags)
    elif score == "levy":
        fwd, _ = leadlag_score_levy(rc, rs, max_lag=max_lag)
        rev, _ = leadlag_score_levy(rs, rc, max_lag=max_lag)
    else:
        raise ValueError(score)

    if np.isnan(fwd) or np.isnan(rev):
        return np.nan, 0
    n_eff = min(_count_overlap(rc, rs), _count_overlap(rs, rc))
    return float(fwd - rev), n_eff


@dataclass
class LeadLagResult:
    edge_scores: pd.DataFrame
    C: pd.DataFrame
    S: pd.DataFrame
    leadingness: pd.Series


def build_lead_lag_matrix_gvkey(
    returns_wide: pd.DataFrame,
    edges_resolved: pd.DataFrame,
    *,
    horizon: int = 1,
    min_obs: int = 80,
    winsor_q: float | None = 0.001,
    score: EdgeScoreMethod = "tstat_diff",
    max_lag: int = 5,
    granger_n_lags: int = 2,
) -> LeadLagResult:
    """
    For each edge (customer, supplier), build a directed score and fill C[customer, supplier].

    **Regression asymmetry** (`tstat_diff`, `beta_diff`): forward = r_sup(t+h) ~ r_cust(t),
    reverse = r_cust(t+h) ~ r_sup(t); raw = t_fwd − t_rev or beta_fwd − beta_rev; scaled by w.

    **Pairwise methods** (`cross_corr`, `regression_r2`, `granger`, `levy`): forward/reverse use
    the same max-lag search on aligned return windows; raw = metric_fwd − metric_rev; scaled by w.

    Supply-only weights use :func:`structural_C_from_edges` (no returns).
    """
    R = returns_wide.sort_index()
    if winsor_q is not None and 0 < winsor_q < 0.5:
        lo = R.quantile(winsor_q)
        hi = R.quantile(1 - winsor_q)
        R = R.clip(lower=lo, upper=hi, axis=1)

    Y = R.shift(-horizon)
    records = []
    colset = set(R.columns)
    ols_scores = {"tstat_diff", "beta_diff"}
    pairwise_scores = {"cross_corr", "regression_r2", "granger", "levy"}

    for row in edges_resolved.itertuples(index=False):
        cust = getattr(row, "customer_gvkey")
        supp = getattr(row, "supplier_gvkey")
        w = float(getattr(row, "weight_wji"))

        if cust not in colset or supp not in colset:
            continue

        if score in ols_scores:
            x_fwd = R[cust].to_numpy()
            y_fwd = Y[supp].to_numpy()
            beta_fwd, t_fwd, n_fwd = _ols_slope_tstat(x_fwd, y_fwd)

            x_rev = R[supp].to_numpy()
            y_rev = Y[cust].to_numpy()
            beta_rev, t_rev, n_rev = _ols_slope_tstat(x_rev, y_rev)

            n_eff = min(n_fwd, n_rev)
            if n_eff < min_obs:
                continue

            if score == "tstat_diff":
                raw = t_fwd - t_rev
            else:
                raw = beta_fwd - beta_rev

            records.append(
                dict(
                    customer_gvkey=cust,
                    supplier_gvkey=supp,
                    w=w,
                    score_method=score,
                    beta_fwd=beta_fwd,
                    t_fwd=t_fwd,
                    beta_rev=beta_rev,
                    t_rev=t_rev,
                    n=n_eff,
                    score=w * raw,
                )
            )
        elif score in pairwise_scores:
            rc = R[cust].to_numpy()
            rs = R[supp].to_numpy()
            raw, n_eff = _edge_raw_asymmetry_pairwise(
                rc,
                rs,
                score=score,
                max_lag=max_lag,
                granger_n_lags=granger_n_lags,
            )
            if n_eff < min_obs or np.isnan(raw):
                continue
            records.append(
                dict(
                    customer_gvkey=cust,
                    supplier_gvkey=supp,
                    w=w,
                    score_method=score,
                    n=n_eff,
                    score=w * raw,
                )
            )
        else:
            raise ValueError(
                f"Unknown score {score!r}; expected one of {ols_scores | pairwise_scores}"
            )

    edge_scores = pd.DataFrame.from_records(records)
    if edge_scores.empty:
        raise ValueError("No valid edge scores computed. Check ID coverage and min_obs.")

    C = edge_scores.pivot_table(
        index="customer_gvkey",
        columns="supplier_gvkey",
        values="score",
        aggfunc="mean",
        fill_value=0.0,
    )
    nodes = sorted(set(C.index).union(C.columns))
    C = C.reindex(index=nodes, columns=nodes, fill_value=0.0)
    S = C - C.T
    leadingness = S.sum(axis=1).sort_values(ascending=False)

    return LeadLagResult(
        edge_scores=edge_scores.sort_values("score", ascending=False).reset_index(drop=True),
        C=C,
        S=S,
        leadingness=leadingness,
    )


def structural_C_from_edges(edges: pd.DataFrame, nodes: list[str]) -> pd.DataFrame:
    """
    C^supply with C_ij = w_{j→i} on edges (customer j, supplier i), aggregated by mean weight.
    Square matrix on `nodes`.
    """
    g = edges.groupby(["customer_gvkey", "supplier_gvkey"], as_index=False)["weight_wji"].mean()
    C = g.pivot(index="customer_gvkey", columns="supplier_gvkey", values="weight_wji")
    C = C.reindex(index=nodes, columns=nodes, fill_value=0.0)
    return C.fillna(0.0)


def hybrid_matrix(C_data: pd.DataFrame, C_supply: pd.DataFrame, alpha: float) -> pd.DataFrame:
    """C_hybrid = α C_data + (1−α) C_supply (aligned indices)."""
    if not (0.0 <= alpha <= 1.0):
        raise ValueError("alpha must be in [0,1]")
    nodes = sorted(set(C_data.index).union(C_data.columns).union(C_supply.index).union(C_supply.columns))
    A = C_data.reindex(index=nodes, columns=nodes, fill_value=0.0)
    B = C_supply.reindex(index=nodes, columns=nodes, fill_value=0.0)
    return alpha * A + (1.0 - alpha) * B


def load_returns_wide_by_gvkey(returns_parquet: str) -> pd.DataFrame:
    r = pd.read_parquet(returns_parquet)
    r["date"] = pd.to_datetime(r["date"])
    r["RET"] = pd.to_numeric(r["RET"], errors="coerce")
    r = r[np.isfinite(r["RET"])].copy()
    r["gvkey"] = r["gvkey"].astype(str).str.zfill(6)
    return r.pivot_table(index="date", columns="gvkey", values="RET", aggfunc="last").sort_index()


def load_edges(edges_csv: str, source_filter: str | None = None) -> pd.DataFrame:
    e = pd.read_csv(edges_csv)
    e["date"] = pd.to_datetime(e["srcdate"])
    if source_filter is not None and "source" in e.columns:
        e = e[e["source"] == source_filter].copy()
    e["supplier_gvkey"] = e["supplier_gvkey"].astype(str).str.zfill(6)
    e["customer_gvkey"] = (
        pd.to_numeric(e["customer_gvkey"], errors="coerce").astype("Int64").astype(str).str.zfill(6)
    )
    e["weight_wji"] = pd.to_numeric(e["weight_wji"], errors="coerce")
    e = e[np.isfinite(e["weight_wji"])].copy()
    e = e[
        e["customer_gvkey"].notna()
        & (e["customer_gvkey"] != "nan")
        & (e["customer_gvkey"] != "<NA>")
        & (e["customer_gvkey"] != "00<NA>")
    ].copy()
    return e
