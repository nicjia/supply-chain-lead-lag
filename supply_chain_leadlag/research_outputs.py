"""Write derived research artifacts (drawdowns, spectral summary, factor alpha, holdings)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from supply_chain_leadlag.global_structure import hermitian_from_skew, permutation_test_max_eig
from supply_chain_leadlag.signals import portfolio_metrics, structural_summary


def drawdown_series(daily_ret: pd.Series) -> pd.Series:
    dr = daily_ret.fillna(0.0)
    wealth = (1.0 + dr).cumprod()
    peak = wealth.cummax()
    return (wealth / peak) - 1.0


def write_drawdowns_csv(daily_df: pd.DataFrame, path: Path) -> None:
    rows = {c: drawdown_series(daily_df[c]) for c in daily_df.columns}
    pd.DataFrame(rows).to_csv(path)


def spectral_summary_from_C(C: pd.DataFrame, *, n_perm: int = 100, seed: int = 42) -> pd.DataFrame:
    """One-row spectral diagnostics plus top eigenvalues table."""
    S = C.to_numpy(dtype=float) - C.to_numpy(dtype=float).T
    summ = structural_summary(C.to_numpy(dtype=float))
    obs, pval, _ = permutation_test_max_eig(S, n_perm=n_perm, seed=seed)
    row = {
        "lambda_max_H": summ["lambda_max_H"],
        "fro_norm_C": summ.get("fro_C", summ.get("fro_norm_C", np.nan)),
        "obs_max_eig_perm": obs,
        "perm_p_value": pval,
        "n_nodes": int(C.shape[0]),
        "n_edges_nonzero": int((C.to_numpy() != 0).sum()),
    }
    H = hermitian_from_skew(S)
    w = np.linalg.eigvalsh(H)
    w = np.sort(np.abs(w))[::-1]
    top = pd.DataFrame(
        {
            "eigen_index": np.arange(min(20, len(w))),
            "eigenvalue": np.real(w[:20]),
            "abs_eigenvalue": np.abs(w[:20]),
        }
    )
    summary = pd.DataFrame([row])
    return summary, top


def factor_exposure_alpha(
    daily_df: pd.DataFrame,
    market_ret: pd.Series | None,
) -> pd.DataFrame:
    """
    CAPM-style alpha per strategy column: r_s = alpha + beta * r_m + eps.
    If no market series, uses cross-sectional mean return as proxy.
    """
    rows = []
    if market_ret is None or market_ret.empty:
        mkt = daily_df.mean(axis=1)
        market_note = "cross_sectional_mean_proxy"
    else:
        mkt = market_ret.reindex(daily_df.index).fillna(0.0)
        market_note = "market_gvkey"

    xm = mkt.to_numpy(dtype=float)
    for col in daily_df.columns:
        y = daily_df[col].fillna(0.0).to_numpy(dtype=float)
        mask = np.isfinite(y) & np.isfinite(xm)
        if mask.sum() < 30:
            rows.append(
                {
                    "strategy_family": col,
                    "alpha_daily": np.nan,
                    "alpha_annual": np.nan,
                    "beta_market": np.nan,
                    "r_squared": np.nan,
                    "n_obs": int(mask.sum()),
                    "market_proxy": market_note,
                }
            )
            continue
        yv, xv = y[mask], xm[mask]
        X = np.column_stack([np.ones(len(xv)), xv])
        beta_hat, _, _, _ = np.linalg.lstsq(X, yv, rcond=None)
        resid = yv - X @ beta_hat
        sse = float(np.sum(resid**2))
        sst = float(np.sum((yv - yv.mean()) ** 2))
        r2 = 1.0 - sse / sst if sst > 0 else np.nan
        rows.append(
            {
                "strategy_family": col,
                "alpha_daily": float(beta_hat[0]),
                "alpha_annual": float(beta_hat[0] * 252),
                "beta_market": float(beta_hat[1]),
                "r_squared": float(r2),
                "n_obs": int(mask.sum()),
                "market_proxy": market_note,
            }
        )
    return pd.DataFrame(rows)


def cluster_labels_to_frames(
    records: list[dict],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not records:
        return pd.DataFrame(), pd.DataFrame()
    lab_df = pd.DataFrame(records)
    sizes = (
        lab_df.groupby(["rebalance_date", "cluster_method", "cluster_label"], as_index=False)
        .size()
        .rename(columns={"size": "cluster_size"})
    )
    return lab_df, sizes


def holdings_from_events(
    events: list[tuple[pd.Timestamp, pd.Series]],
    strategy_family: str = "supplier_pressure",
) -> pd.DataFrame:
    rows = []
    for dt, w in events:
        for gv, wt in w.items():
            if abs(wt) > 1e-12:
                rows.append(
                    {
                        "date": dt,
                        "gvkey": str(gv),
                        "weight": float(wt),
                        "strategy_family": strategy_family,
                    }
                )
    return pd.DataFrame(rows)


def stability_by_method(stability_df: pd.DataFrame) -> pd.DataFrame:
    if stability_df.empty:
        return stability_df
    g = stability_df.groupby("cluster_method", as_index=False)
    return g.agg(
        ari_mean=("ari_prev", "mean"),
        ari_std=("ari_prev", "std"),
        eigenspace_drift_mean=("eigenspace_drift", "mean"),
        eigenspace_drift_std=("eigenspace_drift", "std"),
        n_rebalances=("rebalance_date", "count"),
    )
