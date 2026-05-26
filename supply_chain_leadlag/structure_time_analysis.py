"""
Intra- vs inter-cluster predictability and crisis-period subsample analysis.

Uses rolling PIT C matrices, spectral (and baseline) cluster labels, and decomposes
customer pressure into same-cluster vs cross-cluster components per supplier-day.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from supply_chain_leadlag.backtest import filter_edges_pit, returns_window
from supply_chain_leadlag.clustering_methods import ClusteringMethodError, get_cluster_labels
from supply_chain_leadlag.matrix import build_lead_lag_matrix_gvkey, load_edges, load_returns_wide_by_gvkey
from supply_chain_leadlag.research_config import flat_research_params
from supply_chain_leadlag.signals import predictability_ols, sharpe
from supply_chain_leadlag.yaml_config import load_yaml_config


# Presentation-oriented crisis windows (union used for binary non-crisis complement).
CRISIS_WINDOWS: dict[str, tuple[str, str]] = {
    "gfc": ("2008-09-01", "2009-03-31"),
    "euro_debt": ("2011-08-01", "2011-12-31"),
    "covid_crash": ("2020-02-20", "2020-04-30"),
    "rate_shock_2022": ("2022-01-01", "2022-10-31"),
}

NETWORK_CLUSTER_METHODS = ("signed", "hermitian", "symmetric_spectral", "sector")


@dataclass(frozen=True)
class PeriodSpec:
    name: str
    start: pd.Timestamp | None
    end: pd.Timestamp | None


def crisis_period_specs() -> list[PeriodSpec]:
    specs = [PeriodSpec("all_sample", None, None)]
    for name, (s, e) in CRISIS_WINDOWS.items():
        specs.append(PeriodSpec(name, pd.Timestamp(s), pd.Timestamp(e)))
    specs.append(PeriodSpec("non_crisis", None, None))  # filtered in mask
    return specs


def _in_crisis_union(dates: pd.DatetimeIndex) -> pd.Series:
    out = pd.Series(False, index=dates)
    for start, end in CRISIS_WINDOWS.values():
        m = (dates >= pd.Timestamp(start)) & (dates <= pd.Timestamp(end))
        out = out | m
    return out


def period_mask(dates: pd.DatetimeIndex | np.ndarray, spec: PeriodSpec) -> np.ndarray:
    idx = pd.DatetimeIndex(dates)
    if spec.name == "all_sample":
        return np.ones(len(idx), dtype=bool)
    if spec.name == "non_crisis":
        return np.asarray(~_in_crisis_union(idx), dtype=bool)
    if spec.start is None or spec.end is None:
        raise ValueError(spec)
    return np.asarray((idx >= spec.start) & (idx <= spec.end), dtype=bool)


def _multivariate_ols(y: np.ndarray, X: np.ndarray) -> dict[str, float]:
    """OLS y ~ X (X includes intercept). Returns coefs, t-stats for non-intercept cols."""
    n, k = X.shape
    if n < max(30, k + 5):
        return {"n": float(n), "r2": np.nan, **{f"beta_{j}": np.nan for j in range(k - 1)}}
    beta, _, _, _ = np.linalg.lstsq(X, y, rcond=None)
    resid = y - X @ beta
    sse = float(np.sum(resid**2))
    sst = float(np.sum((y - np.mean(y)) ** 2))
    r2 = 1.0 - sse / sst if sst > 0 else np.nan
    s2 = sse / max(n - k, 1)
    cov = s2 * np.linalg.inv(X.T @ X)
    se = np.sqrt(np.maximum(np.diag(cov), 0.0))
    out: dict[str, float] = {"n": float(n), "r2": float(r2)}
    for j in range(1, k):
        t = beta[j] / se[j] if se[j] > 0 else np.nan
        out[f"beta_{j - 1}"] = float(beta[j])
        out[f"t_{j - 1}"] = float(t)
    return out


def _regress_panel(
    panel: pd.DataFrame,
    *,
    period: str,
    cluster_method: str,
) -> list[dict]:
    """Run univariate and joint regressions for one (method, period) slice."""
    sub = panel.dropna(subset=["y", "signal_intra", "signal_inter", "signal_total"])
    if sub.empty:
        return []
    rows: list[dict] = []
    base = {
        "cluster_method": cluster_method,
        "period": period,
        "n_obs": int(len(sub)),
        "n_suppliers": int(sub["supplier_gvkey"].nunique()),
        "mean_share_intra": float(
            (sub["signal_intra"].abs() / (sub["signal_total"].abs() + 1e-12)).clip(0, 1).mean()
        ),
    }
    y = sub["y"].to_numpy(dtype=float)
    for col, label in [
        ("signal_intra", "intra_only"),
        ("signal_inter", "inter_only"),
        ("signal_total", "total_pressure"),
    ]:
        if sub[col].std() < 1e-14:
            continue
        res = predictability_ols(sub["y"], sub[col])
        rows.append(
            {
                **base,
                "spec": label,
                "beta_intra": res["beta"] if label == "intra_only" else np.nan,
                "beta_inter": res["beta"] if label == "inter_only" else np.nan,
                "beta_total": res["beta"] if label == "total_pressure" else np.nan,
                "t_stat": res["t_beta"],
                "r2": res["r2"],
            }
        )
    if sub["signal_intra"].std() < 1e-14 and sub["signal_inter"].std() < 1e-14:
        return rows
    X = np.column_stack(
        [
            np.ones(len(y)),
            sub["signal_intra"].to_numpy(dtype=float),
            sub["signal_inter"].to_numpy(dtype=float),
        ]
    )
    try:
        joint = _multivariate_ols(y, X)
    except np.linalg.LinAlgError:
        joint = {"n": float(len(y)), "r2": np.nan, "beta_0": np.nan, "beta_1": np.nan}
    rows.append(
        {
            **base,
            "spec": "joint_intra_inter",
            "beta_intra": joint.get("beta_0", np.nan),
            "beta_inter": joint.get("beta_1", np.nan),
            "beta_total": np.nan,
            "t_stat": np.nan,
            "t_intra": joint.get("t_0", np.nan),
            "t_inter": joint.get("t_1", np.nan),
            "r2": joint.get("r2", np.nan),
        }
    )
    return rows


def _decompose_pressure_day(
    R_row: pd.Series,
    edges: pd.DataFrame,
    labels: pd.Series,
) -> pd.DataFrame:
    """Supplier-level intra / inter / total pressure for one trading day."""
    e = edges[["customer_gvkey", "supplier_gvkey", "weight_wji"]].copy()
    e["customer_gvkey"] = e["customer_gvkey"].astype(str).str.zfill(6)
    e["supplier_gvkey"] = e["supplier_gvkey"].astype(str).str.zfill(6)
    cr = R_row.reindex(e["customer_gvkey"].unique()).astype(float)
    e["cust_r"] = e["customer_gvkey"].map(cr)
    e = e.dropna(subset=["cust_r"])
    if e.empty:
        return pd.DataFrame()
    e["cust_cluster"] = e["customer_gvkey"].map(labels)
    e["sup_cluster"] = e["supplier_gvkey"].map(labels)
    e = e.dropna(subset=["cust_cluster", "sup_cluster"])
    if e.empty:
        return pd.DataFrame()
    e["term"] = e["weight_wji"].astype(float) * e["cust_r"]
    e["intra"] = e["cust_cluster"].astype(int) == e["sup_cluster"].astype(int)
    intra = e.loc[e["intra"]].groupby("supplier_gvkey", as_index=False)["term"].sum()
    intra = intra.rename(columns={"term": "signal_intra"})
    inter = e.loc[~e["intra"]].groupby("supplier_gvkey", as_index=False)["term"].sum()
    inter = inter.rename(columns={"term": "signal_inter"})
    tot = e.groupby("supplier_gvkey", as_index=False)["term"].sum().rename(columns={"term": "signal_total"})
    out = tot.merge(intra, on="supplier_gvkey", how="left").merge(inter, on="supplier_gvkey", how="left")
    out["signal_intra"] = out["signal_intra"].fillna(0.0)
    out["signal_inter"] = out["signal_inter"].fillna(0.0)
    return out


def build_supplier_pressure_panel(
    R: pd.DataFrame,
    edges: pd.DataFrame,
    params: dict[str, Any],
    *,
    cluster_methods: tuple[str, ...] = NETWORK_CLUSTER_METHODS,
    max_rebalances: int | None = None,
    show_progress: bool = True,
) -> pd.DataFrame:
    """
    Long panel: supplier_gvkey × date × cluster_method with y, signal_intra, signal_inter.
    """
    idx = R.index.sort_values()
    start, end = idx.min(), idx.max()
    rebalances = pd.bdate_range(start, end, freq=params["rebalance_freq"])
    lb = min(params["lookback_rows"], len(idx) - 1)
    rebalances = rebalances[rebalances >= idx[lb]]
    if max_rebalances is not None:
        rebalances = rebalances[: max_rebalances]

    sector_map = params.get("_sector_map")
    firm_map = params.get("_firm_map")
    chunks: list[pd.DataFrame] = []

    reb_iter: Any = rebalances
    if show_progress:
        from tqdm.auto import tqdm

        reb_iter = tqdm(rebalances, desc="structure panel", unit="reb")

    for i, T in enumerate(reb_iter):
        T = pd.Timestamp(T)
        e_pit = filter_edges_pit(edges, T, expiry_days=params.get("edge_expiry_days"))
        R_win = returns_window(R, T, params["lookback_rows"])
        if R_win.empty or len(e_pit) < 5:
            continue
        try:
            res = build_lead_lag_matrix_gvkey(
                R_win,
                e_pit,
                horizon=params["horizon"],
                min_obs=params["min_obs"],
                winsor_q=params.get("winsor_q"),
                score=params["edge_score"],
                max_lag=params["max_lag"],
            )
        except ValueError:
            continue

        if i + 1 < len(rebalances):
            next_T = pd.Timestamp(rebalances[i + 1])
        else:
            cal = pd.bdate_range(T + pd.Timedelta(days=1), end, freq=params["rebalance_freq"])
            next_T = cal[0] if len(cal) else end
        hold_idx = idx[(idx > T) & (idx <= next_T)]
        if len(hold_idx) < 2:
            continue

        C = res.C
        labels_by_method: dict[str, pd.Series] = {}
        for method in cluster_methods:
            try:
                labels_by_method[method] = get_cluster_labels(
                    C,
                    method,
                    n_clusters=params["n_clusters"],
                    sector_map=sector_map,
                    firm_map=firm_map,
                    random_state=params["cluster_random_state"],
                    edges_pit=e_pit,
                )
            except ClusteringMethodError:
                continue

        if not labels_by_method:
            continue

        for j, d in enumerate(hold_idx[:-1]):
            d_next = hold_idx[j + 1]
            r_row = R.loc[d]
            y_next = R.loc[d_next]
            for method, labels in labels_by_method.items():
                sig = _decompose_pressure_day(r_row, e_pit, labels)
                if sig.empty:
                    continue
                sig["y"] = sig["supplier_gvkey"].map(y_next)
                sig["date"] = d
                sig["cluster_method"] = method
                sig["rebalance_date"] = T
                chunks.append(sig.dropna(subset=["y"]))

    if not chunks:
        return pd.DataFrame()
    return pd.concat(chunks, ignore_index=True)


def row_period_mask(dates: pd.Series, spec: PeriodSpec) -> np.ndarray:
    """Boolean mask per panel row from calendar period spec."""
    idx = pd.to_datetime(dates)
    if spec.name == "all_sample":
        return np.ones(len(idx), dtype=bool)
    if spec.name == "non_crisis":
        crisis = np.zeros(len(idx), dtype=bool)
        for start, end in CRISIS_WINDOWS.values():
            crisis |= (idx >= pd.Timestamp(start)) & (idx <= pd.Timestamp(end))
        return ~crisis
    if spec.start is None or spec.end is None:
        raise ValueError(spec)
    return np.asarray((idx >= spec.start) & (idx <= spec.end), dtype=bool)


def run_intra_inter_regressions(panel: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []
    for method in panel["cluster_method"].unique():
        mp = panel.loc[panel["cluster_method"] == method]
        for spec in crisis_period_specs():
            sub = mp.loc[row_period_mask(mp["date"], spec)]
            rows.extend(_regress_panel(sub, period=spec.name, cluster_method=method))
    return pd.DataFrame(rows)


def run_edge_level_intra_inter(
    R: pd.DataFrame,
    edges: pd.DataFrame,
    params: dict[str, Any],
    *,
    cluster_method: str = "signed",
    max_rebalances: int | None = 12,
) -> pd.DataFrame:
    """
    Edge-day regression: r_sup(t+1) ~ w*r_cust(t), split by intra-cluster dummy.
    Lighter diagnostic (fewer rebalances by default).
    """
    idx = R.index.sort_values()
    rebalances = pd.bdate_range(idx.min(), idx.max(), freq=params["rebalance_freq"])
    lb = min(params["lookback_rows"], len(idx) - 1)
    rebalances = rebalances[rebalances >= idx[lb]]
    if max_rebalances is not None:
        rebalances = rebalances[:max_rebalances]

    x_intra_all: list[float] = []
    y_intra_all: list[float] = []
    x_inter_all: list[float] = []
    y_inter_all: list[float] = []

    for i, T in enumerate(rebalances):
        T = pd.Timestamp(T)
        e_pit = filter_edges_pit(edges, T, expiry_days=params.get("edge_expiry_days"))
        R_win = returns_window(R, T, params["lookback_rows"])
        if R_win.empty:
            continue
        try:
            res = build_lead_lag_matrix_gvkey(
                R_win,
                e_pit,
                horizon=1,
                min_obs=params["min_obs"],
                winsor_q=params.get("winsor_q"),
                score=params["edge_score"],
                max_lag=params["max_lag"],
            )
        except ValueError:
            continue
        try:
            labels = get_cluster_labels(
                res.C,
                cluster_method,
                n_clusters=params["n_clusters"],
                sector_map=params.get("_sector_map"),
                firm_map=params.get("_firm_map"),
                random_state=params["cluster_random_state"],
                edges_pit=e_pit,
            )
        except ClusteringMethodError:
            continue

        next_T = (
            pd.Timestamp(rebalances[i + 1])
            if i + 1 < len(rebalances)
            else idx.max()
        )
        hold = idx[(idx > T) & (idx <= next_T)]
        e = e_pit[["customer_gvkey", "supplier_gvkey", "weight_wji"]].copy()
        e["customer_gvkey"] = e["customer_gvkey"].astype(str).str.zfill(6)
        e["supplier_gvkey"] = e["supplier_gvkey"].astype(str).str.zfill(6)
        e["lc"] = e["customer_gvkey"].map(labels)
        e["ls"] = e["supplier_gvkey"].map(labels)
        e = e.dropna(subset=["lc", "ls"])
        if e.empty:
            continue
        e["intra"] = e["lc"].astype(int) == e["ls"].astype(int)
        for d in hold[:-1]:
            d1 = hold[hold.get_loc(d) + 1]
            cust_r = R.loc[d].reindex(e["customer_gvkey"]).to_numpy(dtype=float)
            sup_y = R.loc[d1].reindex(e["supplier_gvkey"]).to_numpy(dtype=float)
            xv = e["weight_wji"].to_numpy(dtype=float) * cust_r
            mask = np.isfinite(xv) & np.isfinite(sup_y)
            if not mask.any():
                continue
            intra_m = e["intra"].values & mask
            inter_m = (~e["intra"].values) & mask
            if intra_m.any():
                x_intra_all.extend(xv[intra_m].tolist())
                y_intra_all.extend(sup_y[intra_m].tolist())
            if inter_m.any():
                x_inter_all.extend(xv[inter_m].tolist())
                y_inter_all.extend(sup_y[inter_m].tolist())

    rows: list[dict] = []
    for label, xv, yv in [
        ("intra_edge", x_intra_all, y_intra_all),
        ("inter_edge", x_inter_all, y_inter_all),
    ]:
        if len(xv) < 50:
            continue
        reg = predictability_ols(pd.Series(yv), pd.Series(xv))
        rows.append(
            {
                "cluster_method": cluster_method,
                "edge_type": label,
                "beta": reg["beta"],
                "t_stat": reg["t_beta"],
                "r2": reg["r2"],
                "n_obs": reg["n"],
                "n_rebalances_used": len(rebalances),
            }
        )
    return pd.DataFrame(rows)


def crisis_backtest_table(
    daily_returns_path: Path,
    *,
    strategy_col: str | None = None,
) -> pd.DataFrame:
    """Sharpe / ann return by crisis window from saved daily_returns.csv."""
    dr = pd.read_csv(daily_returns_path, parse_dates=["date"])
    fam_cols = [c for c in dr.columns if c != "date"]
    if strategy_col is None:
        return pd.concat(
            [crisis_backtest_table(daily_returns_path, strategy_col=c) for c in fam_cols],
            ignore_index=True,
        )
    if strategy_col not in dr.columns:
        return pd.DataFrame()

    s = dr.set_index("date")[strategy_col].astype(float)
    rows = []
    for spec in crisis_period_specs():
        m = period_mask(s.index, spec)
        sub = s.loc[m]
        if len(sub) < 20:
            continue
        ann = float(sub.mean() * 252)
        rows.append(
            {
                "strategy_family": strategy_col,
                "period": spec.name,
                "sharpe": sharpe(sub),
                "ann_return": ann,
                "ann_vol": float(sub.std() * np.sqrt(252)),
                "n_days": int(len(sub)),
                "cum_return": float((1 + sub).prod() - 1),
            }
        )
    return pd.DataFrame(rows)


def plot_structure_time_results(
    intra_inter_df: pd.DataFrame,
    crisis_bt_df: pd.DataFrame,
    out_dir: Path,
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    import matplotlib.pyplot as plt

    # Joint betas: intra vs inter (all_sample)
    sub = intra_inter_df.loc[
        (intra_inter_df["period"] == "all_sample") & (intra_inter_df["spec"] == "joint_intra_inter")
    ]
    if not sub.empty:
        fig, ax = plt.subplots(figsize=(8, 4))
        x = np.arange(len(sub))
        w = 0.35
        ax.bar(x - w / 2, sub["beta_intra"], w, label="β intra (same cluster)")
        ax.bar(x + w / 2, sub["beta_inter"], w, label="β inter (cross cluster)")
        ax.set_xticks(x)
        ax.set_xticklabels(sub["cluster_method"], rotation=15, ha="right")
        ax.axhline(0, color="k", lw=0.5)
        ax.set_ylabel("Coefficient (supplier return t+1)")
        ax.set_title("Intra- vs inter-cluster pressure (joint regression, full sample)")
        ax.legend()
        fig.tight_layout()
        fig.savefig(out_dir / "intra_inter_betas_by_cluster_method.png", dpi=150)
        plt.close(fig)

    # Crisis Sharpe — supplier pressure
    sp = crisis_bt_df.loc[crisis_bt_df["strategy_family"] == "supplier_pressure"]
    if not sp.empty:
        fig, ax = plt.subplots(figsize=(9, 4))
        order = [p.name for p in crisis_period_specs()]
        sp = sp.set_index("period").reindex([p for p in order if p in sp.index]).reset_index()
        colors = ["#2ecc71" if p == "non_crisis" else "#e74c3c" if p != "all_sample" else "#3498db" for p in sp["period"]]
        ax.bar(sp["period"], sp["sharpe"], color=colors)
        ax.axhline(0, color="k", lw=0.5)
        ax.set_ylabel("Sharpe (daily)")
        ax.set_title("Supplier pressure — Sharpe by period")
        plt.xticks(rotation=25, ha="right")
        fig.tight_layout()
        fig.savefig(out_dir / "crisis_sharpe_supplier_pressure.png", dpi=150)
        plt.close(fig)

    # All families non_crisis vs all_sample
    comp = crisis_bt_df.loc[crisis_bt_df["period"].isin(["all_sample", "non_crisis", "covid_crash", "gfc"])]
    if not comp.empty:
        fig, ax = plt.subplots(figsize=(10, 5))
        pivot = comp.pivot(index="strategy_family", columns="period", values="sharpe")
        pivot.plot(kind="bar", ax=ax)
        ax.set_ylabel("Sharpe")
        ax.set_title("Strategy families — Sharpe by subsample")
        ax.legend(title="Period", bbox_to_anchor=(1.02, 1))
        fig.tight_layout()
        fig.savefig(out_dir / "crisis_sharpe_families.png", dpi=150)
        plt.close(fig)


def write_presentation_summary(
    intra_inter_df: pd.DataFrame,
    crisis_bt_df: pd.DataFrame,
    edge_df: pd.DataFrame,
    path: Path,
) -> None:
    """Short markdown for slides."""
    lines = [
        "# Network structure & time variation (presentation notes)",
        "",
        "## Network structure & clustering",
        "",
        "Setup: at each BME rebalance, estimate return-based \\(C\\) (`tstat_diff`), cluster nodes with "
        "**spectral embedding** methods (`signed`, `hermitian`, `symmetric_spectral`) plus **sector** baseline. "
        "Decompose customer pressure on supplier \\(i\\):",
        "",
        "- **Intra:** \\(\\sum_{j: L(j)=L(i)} w_{ji} r_{j,t}\\)",
        "- **Inter:** \\(\\sum_{j: L(j)\\neq L(i)} w_{ji} r_{j,t}\\)",
        "",
        "Regress \\(r_{i,t+1}\\) on intra and inter signals (joint specification).",
        "",
    ]
    joint = intra_inter_df.loc[
        (intra_inter_df["period"] == "all_sample") & (intra_inter_df["spec"] == "joint_intra_inter")
    ]
    if not joint.empty:
        lines.append("| Cluster method | β intra | t intra | β inter | t inter | R² | share \\|intra\\| |")
        lines.append("|----------------|---------|---------|---------|---------|-----|----------------|")
        for _, r in joint.iterrows():
            lines.append(
                f"| {r['cluster_method']} | {r['beta_intra']:.4f} | {r.get('t_intra', np.nan):.2f} | "
                f"{r['beta_inter']:.4f} | {r.get('t_inter', np.nan):.2f} | {r['r2']:.4f} | "
                f"{r['mean_share_intra']:.2%} |"
            )
        lines.append("")

    uni = intra_inter_df.loc[
        (intra_inter_df["period"] == "all_sample") & (intra_inter_df["spec"].isin(["intra_only", "inter_only"]))
    ]
    if not uni.empty:
        lines.append("**Univariate (full sample):**")
        lines.append("")
        try:
            lines.append(uni.pivot_table(
                index="cluster_method", columns="spec", values=["beta_intra", "beta_inter", "t_stat"], aggfunc="first"
            ).to_markdown())
        except Exception:
            lines.append(uni.to_string())
        lines.append("")

    if not edge_df.empty:
        lines.extend(
            [
                "**Edge-level check** (`signed`, subsample of rebalances):",
                "",
            ]
        )
        try:
            lines.append(edge_df.to_markdown(index=False, floatfmt=".4f"))
        except Exception:
            lines.append(edge_df.to_string())
        lines.append("")

    lines.extend(
        [
            "### Interpretation guide",
            "",
            "- If **β_intra > β_inter** (joint) and intra-only t-stat is larger → lead–lag is **concentrated among "
            "firms grouped by the directed network embedding** (information travels within diffusion communities).",
            "- If **sector** shows weak intra but network methods show strong intra → effect is **network-specific**, "
            "not just industry co-movement.",
            "- **symmetric_spectral** (direction-blind) vs **signed/hermitian** gap → direction-aware clustering matters.",
            "",
            "## Time variation (crisis vs non-crisis)",
            "",
            f"Crisis windows: {', '.join(CRISIS_WINDOWS.keys())}. **non_crisis** = all other days.",
            "",
        ]
    )

    if not crisis_bt_df.empty:
        lines.append("**Supplier-pressure Sharpe by period:**")
        lines.append("")
        sp = crisis_bt_df.loc[crisis_bt_df["strategy_family"] == "supplier_pressure"]
        try:
            lines.append(sp.to_markdown(index=False, floatfmt=".3f"))
        except Exception:
            lines.append(sp.to_string())
        lines.append("")
        lines.extend(
            [
                "**Panel predictability by period** (joint intra/inter, `signed`):",
                "",
            ]
        )
        signed = intra_inter_df.loc[
            (intra_inter_df["cluster_method"] == "signed") & (intra_inter_df["spec"] == "joint_intra_inter")
        ]
        try:
            lines.append(signed.to_markdown(index=False, floatfmt=".4f"))
        except Exception:
            lines.append(signed.to_string())
        lines.append("")

    lines.extend(
        [
            "### Interpretation guide",
            "",
            "- Compare **non_crisis** vs crisis subperiods: amplification during stress supports "
            "**slow diffusion under friction**; collapse suggests the signal is a calm-market artifact.",
            "- Use **GFC** and **COVID** as distinct episodes — different supply-chain disruption mechanisms.",
            "",
            "Plots: `plots/intra_inter_betas_by_cluster_method.png`, `plots/crisis_sharpe_*.png`.",
            "",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def run_structure_time_analysis(
    root: Path,
    out_dir: Path,
    *,
    max_rebalances: int | None = None,
    quick: bool = False,
) -> dict[str, pd.DataFrame]:
    cfg = load_yaml_config(root / "config" / "research.yaml")
    params = flat_research_params(cfg)
    if max_rebalances is not None:
        params["max_rebalances"] = max_rebalances
    elif quick:
        params["max_rebalances"] = 24

    edges = load_edges(str(root / params["edges_csv"]), date_col=params["edge_date_col"])
    R = load_returns_wide_by_gvkey(str(root / params["returns_parquet"]))
    from supply_chain_leadlag.research_pipeline import load_firm_classification_map, load_sector_map

    params["_sector_map"] = load_sector_map(params.get("sector_map_csv"))
    params["_firm_map"] = load_firm_classification_map(params.get("firm_classification_map_csv"))

    methods = NETWORK_CLUSTER_METHODS
    if quick:
        methods = ("signed", "sector")

    panel = build_supplier_pressure_panel(
        R,
        edges,
        params,
        cluster_methods=methods,
        max_rebalances=params.get("max_rebalances"),
        show_progress=not quick,
    )
    intra_inter = run_intra_inter_regressions(panel) if not panel.empty else pd.DataFrame()
    edge = run_edge_level_intra_inter(
        R,
        edges,
        params,
        cluster_method="signed",
        max_rebalances=min(params.get("max_rebalances") or 24, 24) if quick else params.get("max_rebalances"),
    )

    daily_path = out_dir / "daily_returns.csv"
    crisis_bt = crisis_backtest_table(daily_path) if daily_path.is_file() else pd.DataFrame()

    out_dir.mkdir(parents=True, exist_ok=True)
    if not panel.empty:
        panel.to_parquet(out_dir / "intra_inter_panel.parquet", index=False)
    intra_inter.to_csv(out_dir / "intra_inter_cluster_predictability.csv", index=False)
    edge.to_csv(out_dir / "edge_level_intra_inter.csv", index=False)
    crisis_bt.to_csv(out_dir / "crisis_period_backtest.csv", index=False)

    plot_structure_time_results(
        intra_inter,
        crisis_bt,
        out_dir / "plots",
    )
    write_presentation_summary(
        intra_inter,
        crisis_bt,
        edge,
        out_dir / "report" / "network_structure_time.md",
    )

    return {
        "panel": panel,
        "intra_inter": intra_inter,
        "edge": edge,
        "crisis_bt": crisis_bt,
    }
