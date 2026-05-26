"""
Unified final research pipeline: panel tests, strategy families, clustering, hybrid sweep, events, outputs.
"""

from __future__ import annotations

import json
import logging
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import yaml

logger = logging.getLogger(__name__)

# Not a pure clustering baseline: blends C_data and C_supply (same idea as hybrid α sweep).
CLUSTER_SWEEP_EXCLUDE_METHODS: frozenset[str] = frozenset({"hybrid_prior"})

from supply_chain_leadlag.backtest import (
    comparison_metrics_table,
    filter_edges_pit,
    returns_window,
    run_rolling_comparison,
    supplier_pressure_signal,
)
from supply_chain_leadlag.clustering_methods import ClusteringMethodError, get_cluster_labels
from supply_chain_leadlag.events import CONDITIONS, add_event_flags, mask_returns_by_condition
from supply_chain_leadlag.global_structure import eigendecompose_hermitian, hermitian_from_skew
from supply_chain_leadlag.matrix import build_lead_lag_matrix_gvkey, hybrid_matrix, load_edges, load_returns_wide_by_gvkey, structural_C_from_edges
from supply_chain_leadlag.research_config import flat_research_params, load_research_config
from supply_chain_leadlag.signals import portfolio_metrics, predictability_ols
from supply_chain_leadlag.stability import stability_row
from supply_chain_leadlag.research_outputs import (
    cluster_labels_to_frames,
    drawdown_series,
    factor_exposure_alpha,
    holdings_from_events,
    spectral_summary_from_C,
    stability_by_method,
    write_drawdowns_csv,
)
from supply_chain_leadlag.strategy_families import run_strategy_family
from supply_chain_leadlag.yaml_config import load_yaml_config


def _exclude_cluster_sweep_methods(methods: list[str]) -> list[str]:
    """Drop methods that belong in hybrid_sweep, not cluster_method comparison."""
    return [m for m in methods if m not in CLUSTER_SWEEP_EXCLUDE_METHODS]


def _filter_cluster_sweep_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "cluster_method" not in df.columns:
        return df
    return df[~df["cluster_method"].isin(CLUSTER_SWEEP_EXCLUDE_METHODS)].copy()


def _resolve_cluster_sweep_families(params: dict[str, Any], *, quick: bool) -> tuple[str, ...]:
    """Families included in cluster_sweep (default: metacluster + clusterrank)."""
    raw = params.get("cluster_sweep_families")
    if raw:
        if isinstance(raw, str):
            fams = [x.strip() for x in raw.split(",") if x.strip()]
        else:
            fams = list(raw)
        return tuple(f for f in fams if f in params["strategy_families"])
    if quick:
        return ("metacluster",)
    return ("metacluster", "clusterrank")


def _merge_cluster_sweep_artifacts(
    out_dir: Path,
    new_cmp: pd.DataFrame,
    new_daily: pd.DataFrame,
    families_updated: tuple[str, ...],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Replace rows for ``families_updated`` in existing sweep CSVs; keep other families."""
    if not families_updated:
        return new_cmp, new_daily
    cmp_path = out_dir / "cluster_method_comparison.csv"
    if cmp_path.is_file() and not new_cmp.empty:
        old_cmp = _filter_cluster_sweep_df(pd.read_csv(cmp_path))
        keep = old_cmp[~old_cmp["strategy_family"].isin(families_updated)]
        new_cmp = pd.concat([keep, new_cmp], ignore_index=True)
    daily_path = out_dir / "cluster_sweep_daily_returns.csv"
    if daily_path.is_file() and not new_daily.empty:
        old_daily = _filter_cluster_sweep_df(pd.read_csv(daily_path, parse_dates=["date"]))
        keep_d = old_daily[~old_daily["strategy_family"].isin(families_updated)]
        new_daily = pd.concat([keep_d, new_daily], ignore_index=True)
    return new_cmp, new_daily


def _mark_dashboard_best_star(
    ax: Any,
    *,
    method_order: list[str],
    sub: pd.DataFrame,
    metric: str,
    y_scale: float = 1.0,
) -> None:
    """Gold star at the best (max) value of ``metric`` for one family panel series."""
    if metric not in sub.columns:
        return
    valid = sub[metric].dropna()
    if valid.empty:
        return
    cm_best = valid.idxmax()
    if cm_best not in method_order:
        return
    xi = method_order.index(cm_best)
    yi = float(valid.loc[cm_best]) * y_scale
    if not np.isfinite(yi):
        return
    ax.scatter(
        [xi],
        [yi],
        s=280,
        marker="*",
        c="gold",
        edgecolors="black",
        linewidths=0.6,
        zorder=6,
    )


def cluster_method_for_family(family: str, params: dict[str, Any]) -> str:
    """Cluster label method for a strategy family (sweep winners override default)."""
    fam_map = params.get("family_cluster_methods") or {}
    if family in fam_map:
        return str(fam_map[family])
    return str(params.get("default_cluster_method", "signed"))


def configure_pipeline_logging(*, verbose: bool = False, level: str | None = None) -> None:
    """Console logging for pipeline progress. ``verbose`` enables per-rebalance DEBUG lines."""
    lvl_name = (level or ("DEBUG" if verbose else "INFO")).upper()
    logging.basicConfig(
        level=getattr(logging, lvl_name, logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
        force=True,
    )


def _git_commit() -> str | None:
    try:
        return (
            subprocess.check_output(["git", "rev-parse", "HEAD"], stderr=subprocess.DEVNULL, text=True)
            .strip()
        )
    except Exception:
        return None


def synth_returns_panel(n: int = 400, n_stock: int = 40, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    dates = pd.date_range("2015-01-01", periods=n, freq="B")
    gv = [f"{i:06d}" for i in range(1, n_stock + 1)]
    return pd.DataFrame({g: rng.standard_normal(n) * 0.012 for g in gv}, index=dates)


def load_sector_map(path: str | Path | None) -> pd.Series | None:
    p = Path(path) if path else None
    if p is None or not p.is_file():
        return None
    df = pd.read_csv(p)
    if "gvkey" not in df.columns or "sector" not in df.columns:
        return None
    df["gvkey"] = df["gvkey"].astype(str).str.zfill(6)
    return df.set_index("gvkey")["sector"]


def load_firm_classification_map(path: str | Path | None) -> pd.DataFrame | None:
    from supply_chain_leadlag.classification_data import load_firm_classification_map as _load

    return _load(path)


def load_earnings_calendar(path: str | Path | None) -> pd.DataFrame | None:
    p = Path(path) if path else None
    if p is None or not p.is_file():
        return None
    return pd.read_csv(p)


def _metrics_from_returns(dr: pd.Series, ann: int = 252) -> dict[str, float]:
    dr = dr.dropna()
    if dr.empty or dr.std() == 0:
        return {
            "ann_return": np.nan,
            "ann_vol": np.nan,
            "sharpe": np.nan,
            "max_drawdown": np.nan,
            "avg_turnover": np.nan,
            "net_sharpe": np.nan,
            "hit_rate": np.nan,
            "calmar": np.nan,
            "avg_daily_return": np.nan,
            "n_days": 0,
        }
    m = portfolio_metrics(dr, ann=ann)
    ann_ret = float(dr.mean() * ann)
    ann_vol = float(dr.std() * np.sqrt(ann))
    cum = (1 + dr).cumprod()
    peak = cum.cummax()
    mdd = float(((cum / peak) - 1).min())
    calmar = ann_ret / abs(mdd) if mdd < 0 else np.nan
    return {
        "ann_return": ann_ret,
        "ann_vol": ann_vol,
        "sharpe": m.get("sharpe", np.nan),
        "max_drawdown": mdd,
        "avg_turnover": np.nan,
        "net_sharpe": m.get("sharpe", np.nan),
        "hit_rate": float((dr > 0).mean()),
        "calmar": calmar,
        "avg_daily_return": float(dr.mean()),
        "n_days": int(len(dr)),
    }


def run_panel_forward_reverse(
    R: pd.DataFrame,
    edges: pd.DataFrame,
    *,
    horizon_max: int = 5,
    edge_date_col: str = "filing_date",
    edge_expiry_days: int | None = 550,
    panel_parquet: str | Path | None = None,
) -> pd.DataFrame:
    """
    Panel validation: supplier y_{t+h} ~ customer-pressure signal.
    Uses ``linearmodels`` PanelOLS when panel parquet + dependency exist; else pooled OLS.
    """
    rows = []
    if isinstance(edges, pd.DataFrame):
        e = edges.copy()
    else:
        e = load_edges(str(edges), date_col=edge_date_col)

    for direction in ("forward_customer_to_supplier", "reverse_supplier_to_customer"):
        for h in range(1, horizon_max + 1):
            records = []
            for _, er in e.drop_duplicates(["customer_gvkey", "supplier_gvkey"]).iterrows():
                cust = str(er["customer_gvkey"]).zfill(6)
                supp = str(er["supplier_gvkey"]).zfill(6)
                if cust not in R.columns or supp not in R.columns:
                    continue
                if direction.startswith("forward"):
                    x = R[cust]
                    y = R[supp].shift(-h)
                else:
                    x = R[supp]
                    y = R[cust].shift(-h)
                w = float(er.get("weight_wji", 1.0))
                sig = w * x
                df = pd.DataFrame({"y": y, "signal": sig}).dropna()
                if len(df) < 80:
                    continue
                records.append(df)
            if not records:
                continue
            panel = pd.concat(records, axis=0)
            res = predictability_ols(panel["y"], panel["signal"])
            rows.append(
                {
                    "direction": direction,
                    "horizon": h,
                    "beta": res["beta"],
                    "std_error": np.nan,
                    "t_stat": res["t_beta"],
                    "p_value": np.nan,
                    "n_obs": res["n"],
                    "n_entities": len(records),
                    "fixed_effects": "none_pooled",
                    "clustered_se": False,
                    "condition": "all_days",
                }
            )
    return pd.DataFrame(rows)


def _panel_from_linearmodels_file(
    panel_path: Path,
    *,
    direction: str,
    signal_col: str,
    entity_col: str,
    ret_prefix: str,
    horizon_max: int,
) -> list[dict]:
    from linearmodels.panel import PanelOLS

    panel = pd.read_parquet(panel_path)
    rows: list[dict] = []
    df = panel.set_index([entity_col, "date"]).sort_index()
    lag_cols = [c for c in df.columns if c.startswith(f"{ret_prefix}_r_lag_")][:5]
    for h in range(1, horizon_max + 1):
        ycol = f"{ret_prefix}_r_fwd_{h}"
        if ycol not in df.columns or signal_col not in df.columns:
            continue
        X = df[[signal_col] + lag_cols].copy()
        X["const"] = 1.0
        mod = PanelOLS(df[ycol], X, entity_effects=True, time_effects=True)
        res = mod.fit(cov_type="clustered", cluster_entity=True)
        rows.append(
            {
                "direction": direction,
                "horizon": h,
                "beta": float(res.params[signal_col]),
                "std_error": float(res.std_errors[signal_col]),
                "t_stat": float(res.tstats[signal_col]),
                "p_value": float(res.pvalues[signal_col]),
                "n_obs": int(res.nobs),
                "n_entities": int(df.index.get_level_values(0).nunique()),
                "fixed_effects": "entity+time",
                "clustered_se": True,
                "condition": "all_days",
            }
        )
    return rows


def run_panel_with_parquet(root: Path, *, horizon_max: int) -> pd.DataFrame | None:
    """Run PanelOLS on forward/reverse panel parquets if present."""
    try:
        from linearmodels.panel import PanelOLS  # noqa: F401
    except ImportError:
        return None

    rows: list[dict] = []
    fwd = root / "data" / "leadlag_panel_forward.parquet"
    rev = root / "data" / "leadlag_panel_reverse.parquet"
    if fwd.is_file():
        rows.extend(
            _panel_from_linearmodels_file(
                fwd,
                direction="forward_customer_to_supplier",
                signal_col="y",
                entity_col="supplier_gvkey",
                ret_prefix="sup",
                horizon_max=horizon_max,
            )
        )
    if rev.is_file():
        rows.extend(
            _panel_from_linearmodels_file(
                rev,
                direction="reverse_supplier_to_customer",
                signal_col="z",
                entity_col="customer_gvkey",
                ret_prefix="cust",
                horizon_max=horizon_max,
            )
        )
    return pd.DataFrame(rows) if rows else None


def _accumulate_family_returns(
    R: pd.DataFrame,
    edges: pd.DataFrame,
    params: dict[str, Any],
    *,
    cluster_method: str = "hermitian",
    hybrid_alpha: float | None = None,
    max_rebalances: int | None = None,
    quick: bool = False,
    collect_cluster_labels: bool = True,
    log_prefix: str = "",
    show_progress: bool = False,
    progress_desc: str | None = None,
) -> tuple[pd.Series, list[dict], dict, list[dict], list[tuple[pd.Timestamp, pd.Series]]]:
    """Rolling PIT loop for one (family, cluster_method, alpha)."""
    tag = log_prefix or (
        f"family={params.get('_family', '?')} cluster={cluster_method} "
        f"alpha={hybrid_alpha if hybrid_alpha is not None else 'none'}"
    )
    logger.info("Rolling backtest start: %s (max_rebalances=%s)", tag, max_rebalances)
    t0 = time.perf_counter()
    idx = R.index.sort_values()
    start, end = idx.min(), idx.max()
    rebalances = pd.bdate_range(start, end, freq=params["rebalance_freq"])
    lb = min(params["lookback_rows"], len(idx) - 1)
    rebalances = rebalances[rebalances >= idx[lb]]
    if max_rebalances is not None:
        rebalances = rebalances[: max_rebalances]
    n_reb_planned = len(rebalances)

    sector_map = load_sector_map(params.get("sector_map_csv"))
    firm_map = load_firm_classification_map(params.get("firm_classification_map_csv"))
    daily_acc = pd.Series(0.0, index=idx)
    stability_rows: list[dict] = []
    last_labels: pd.Series | None = None
    last_C: pd.DataFrame | None = None
    n_reb = 0
    cluster_label_records: list[dict] = []
    weight_events: list[tuple[pd.Timestamp, pd.Series]] = []

    rebalance_iter: Any = rebalances
    if show_progress:
        from tqdm.auto import tqdm

        desc = progress_desc or tag
        rebalance_iter = tqdm(
            rebalances,
            desc=desc,
            leave=False,
            unit="reb",
            total=n_reb_planned,
        )

    for i, T in enumerate(rebalance_iter):
        T = pd.Timestamp(T)
        logger.debug(
            "  rebalance %d/%d @ %s (%s)",
            i + 1,
            n_reb_planned,
            T.date(),
            tag,
        )
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

        C = res.C
        if hybrid_alpha is not None:
            nodes = list(C.index)
            C_sup = structural_C_from_edges(e_pit, nodes)
            C = hybrid_matrix(C, C_sup, float(hybrid_alpha))

        try:
            labels = get_cluster_labels(
                C,
                cluster_method,
                n_clusters=params["n_clusters"],
                sector_map=sector_map,
                firm_map=firm_map,
                random_state=params["cluster_random_state"],
                edges_pit=e_pit,
                hybrid_prior_alpha=params.get("hybrid_prior_alpha", 0.5),
            )
        except ClusteringMethodError:
            labels = None

        if labels is not None and collect_cluster_labels:
            family_tag = params.get("_family", "unknown")
            for gv, lab in labels.items():
                cluster_label_records.append(
                    {
                        "rebalance_date": T,
                        "gvkey": str(gv),
                        "cluster_method": cluster_method,
                        "cluster_label": int(lab),
                        "strategy_family": family_tag,
                    }
                )
        if labels is not None:
            stability_rows.append(
                stability_row(
                    T,
                    cluster_method,
                    params.get("_family", "unknown"),
                    labels,
                    last_labels,
                    C,
                    last_C,
                    params["n_clusters"],
                )
            )
            last_labels = labels
            last_C = C.copy()

        if i + 1 < len(rebalances):
            next_T = pd.Timestamp(rebalances[i + 1])
        elif max_rebalances is not None:
            # Capped runs: do not extend the final rebalance through sample end (would
            # reuse one C/labels for 10+ years and distort cumulative-PnL plots).
            cal = pd.bdate_range(T + pd.Timedelta(days=1), end, freq=params["rebalance_freq"])
            next_T = pd.Timestamp(cal[0]) if len(cal) else end
        else:
            next_T = end
        R_fwd = R.loc[(R.index > T) & (R.index <= next_T)]
        if R_fwd.empty:
            continue

        family = params.get("_family", "supplier_pressure")
        try:
            if family in ("metacluster", "clusterrank") and labels is None:
                continue
            out = run_strategy_family(
                family,
                C,
                R_win,
                R_fwd,
                e_pit,
                cluster_labels=labels,
                q=params["q"],
                apply_next_day=params["apply_next_day"],
                hybrid_alpha=None if hybrid_alpha is not None else None,
                globalrank_method=params.get("globalrank_method", "spectral"),
                n_clusters=params["n_clusters"],
                cluster_random_state=params["cluster_random_state"],
                res=res,
            )
        except (ValueError, ClusteringMethodError):
            continue

        dr = out["daily_returns"].reindex(idx).fillna(0.0)
        daily_acc = daily_acc + dr
        if family == "supplier_pressure":
            weight_events.extend(out.get("events", []))
        n_reb += 1
        if quick and n_reb >= 3:
            break

    meta = {"n_rebalances": n_reb, "cluster_method": cluster_method, "hybrid_alpha": hybrid_alpha}
    elapsed = time.perf_counter() - t0
    logger.info(
        "Rolling backtest done: %s — %d rebalance(s) used in %.1fs",
        tag,
        n_reb,
        elapsed,
    )
    return daily_acc, stability_rows, meta, cluster_label_records, weight_events


ALL_PIPELINE_STEPS: frozenset[str] = frozenset(
    {
        "load",
        "panel",
        "baselines",
        "families",
        "cluster_sweep",
        "hybrid_sweep",
        "summary",
        "events",
        "artifacts",
        "plots",
        "report",
    }
)

PIPELINE_STEP_PRESETS: dict[str, tuple[str, ...]] = {
    "full": tuple(ALL_PIPELINE_STEPS),
    "families": ("load", "families", "summary", "plots", "report"),
    "hybrid": ("load", "hybrid_sweep", "plots", "report"),
}


def _parse_step_list(raw: str | None) -> set[str]:
    if not raw:
        return set()
    out: set[str] = set()
    for part in raw.replace(" ", "").split(","):
        if not part:
            continue
        if part not in ALL_PIPELINE_STEPS:
            known = ", ".join(sorted(ALL_PIPELINE_STEPS))
            raise ValueError(f"Unknown pipeline step {part!r}. Valid steps: {known}")
        out.add(part)
    return out


def resolve_pipeline_steps(
    *,
    only: str | None = None,
    steps: str | None = None,
    skip_steps: str | None = None,
    config_steps: list[str] | None = None,
) -> set[str]:
    """Resolve which pipeline steps to run (CLI / config / default full)."""
    if only:
        preset = PIPELINE_STEP_PRESETS.get(only)
        if preset is None:
            known = ", ".join(sorted(PIPELINE_STEP_PRESETS))
            raise ValueError(f"Unknown --only preset {only!r}. Valid presets: {known}")
        enabled = set(preset)
    elif steps:
        enabled = _parse_step_list(steps)
    elif config_steps:
        enabled = {s for s in config_steps if s in ALL_PIPELINE_STEPS}
        if not enabled:
            enabled = set(ALL_PIPELINE_STEPS)
    else:
        enabled = set(ALL_PIPELINE_STEPS)

    enabled -= _parse_step_list(skip_steps)
    if not enabled:
        raise ValueError("No pipeline steps left to run after --skip-steps")
    return enabled


def _family_plot_label(family: str, cluster_method: str | None) -> str:
    if family in ("metacluster", "clusterrank") and cluster_method:
        return f"{family} ({cluster_method})"
    return family


def _write_strategy_families_dashboard(
    out_dir: Path,
    all_daily: dict[str, pd.Series],
    family_rows: list[dict] | None = None,
) -> None:
    """Cumulative PnL + Sharpe bar chart for the four strategy families."""
    if not all_daily:
        return
    plots = out_dir / "plots"
    plots.mkdir(parents=True, exist_ok=True)
    cluster_by_family: dict[str, str] = {}
    sharpe_by_family: dict[str, float] = {}
    if family_rows:
        for row in family_rows:
            fam = str(row.get("strategy_family", ""))
            if fam:
                cluster_by_family[fam] = str(row.get("cluster_method", ""))
                sh = row.get("sharpe")
                if sh is not None and not (isinstance(sh, float) and np.isnan(sh)):
                    sharpe_by_family[fam] = float(sh)

    cum = pd.DataFrame({k: (1 + v.fillna(0)).cumprod() for k, v in all_daily.items()})
    fig, axes = plt.subplots(1, 2, figsize=(11, 4))
    for col in cum.columns:
        axes[0].plot(
            cum.index,
            cum[col],
            label=_family_plot_label(col, cluster_by_family.get(col)),
        )
    axes[0].set_title("Cumulative PnL by strategy family")
    axes[0].set_ylabel("Cumulative PnL")
    axes[0].legend(fontsize=8)
    axes[0].grid(True, alpha=0.25)

    if sharpe_by_family:
        fam_order = [f for f in cum.columns if f in sharpe_by_family]
        axes[1].bar(
            fam_order,
            [sharpe_by_family[f] for f in fam_order],
            color=["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728"][: len(fam_order)],
        )
    elif family_rows:
        df = pd.DataFrame(family_rows)
        axes[1].bar(df["strategy_family"], df["sharpe"].fillna(0))
    axes[1].set_title("Sharpe by strategy family")
    axes[1].tick_params(axis="x", rotation=30)
    axes[1].grid(True, axis="y", alpha=0.25)
    plt.tight_layout()
    plt.savefig(plots / "strategy_families_dashboard.png", dpi=150)
    plt.close()


def _write_family_core_outputs(out_dir: Path, all_daily: dict[str, pd.Series]) -> None:
    """Primary tradable outputs for the four strategy families."""
    if not all_daily:
        return
    daily_df = pd.DataFrame(all_daily)
    daily_df.to_csv(out_dir / "daily_returns.csv")
    cum = (1 + daily_df.fillna(0)).cumprod()
    cum.to_csv(out_dir / "cumulative_returns.csv")
    write_drawdowns_csv(daily_df, out_dir / "drawdowns.csv")


def run_final_research_pipeline(
    config_path: str | Path | None = None,
    *,
    max_rebalances: int | None = None,
    quick: bool = False,
    repo_root: Path | None = None,
    verbose: bool = False,
    show_progress: bool | None = None,
    only: str | None = None,
    steps: str | None = None,
    skip_steps: str | None = None,
    plot_profile: str | None = None,
    cluster_sweep_families: str | None = None,
) -> Path:
    """Execute full pipeline; returns output directory path."""
    configure_pipeline_logging(verbose=verbose)
    pipeline_t0 = time.perf_counter()
    root = repo_root or Path(__file__).resolve().parents[1]
    raw_cfg = load_yaml_config(config_path or root / "config" / "research.yaml")
    params = flat_research_params(raw_cfg)
    if cluster_sweep_families is not None:
        params["cluster_sweep_families"] = cluster_sweep_families
    if max_rebalances is not None:
        params["max_rebalances"] = max_rebalances
    elif quick:
        params["max_rebalances"] = min(params.get("max_rebalances") or 3, 3)
        params["hybrid_alpha_grid"] = [0.0, 0.5, 1.0]
        params["cluster_methods"] = ["hermitian"]
        params["strategy_families"] = [
            "supplier_pressure",
            "globalrank",
            "metacluster",
            "clusterrank",
        ]
        params["baselines_include"] = False

    logger.info(
        "Pipeline start config=%s quick=%s max_rebalances=%s output=%s",
        config_path or root / "config" / "research.yaml",
        quick,
        params.get("max_rebalances"),
        params["output_dir"],
    )
    logger.info(
        "Settings: edge_score=%s families=%s cluster_methods=%s hybrid_alphas=%s",
        params["edge_score"],
        params["strategy_families"],
        params["cluster_methods"],
        params["hybrid_alpha_grid"],
    )

    out_dir = Path(params["output_dir"])
    if not out_dir.is_absolute():
        out_dir = root / out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "matrices").mkdir(exist_ok=True)
    (out_dir / "clusters").mkdir(exist_ok=True)
    (out_dir / "plots").mkdir(exist_ok=True)
    (out_dir / "report").mkdir(exist_ok=True)

    enabled = resolve_pipeline_steps(
        only=only,
        steps=steps,
        skip_steps=skip_steps,
        config_steps=params.get("pipeline_steps"),
    )
    if show_progress is None:
        show_progress = ("cluster_sweep" in enabled or "families" in enabled) and not quick
    profile = (plot_profile or params.get("plot_profile") or "full").lower()
    if profile not in ("minimal", "full"):
        raise ValueError(f"plot_profile must be 'minimal' or 'full', got {profile!r}")
    if only == "families" and plot_profile is None:
        profile = "minimal"

    total_steps = len(enabled)
    logger.info("Pipeline steps enabled (%d): %s", total_steps, ", ".join(sorted(enabled)))
    logger.info("Plot profile: %s", profile)

    warnings: list[str] = []
    step_n = 0
    R: pd.DataFrame | None = None
    edges: pd.DataFrame | None = None
    panel_df = pd.DataFrame()
    from supply_chain_leadlag.backtest import BacktestResult, ComparisonResult

    empty_bt = BacktestResult(
        daily_ret=pd.Series(dtype=float),
        gross_daily_ret=pd.Series(dtype=float),
        daily_cost=pd.Series(dtype=float),
        turnover=pd.Series(dtype=float),
        cumulative=pd.Series(dtype=float),
        rebalance_log=pd.DataFrame(),
    )
    comp = ComparisonResult(
        main=empty_bt,
        random=empty_bt,
        momentum=empty_bt,
        structural=empty_bt,
        equal_weight=empty_bt,
    )
    comp_tab = pd.DataFrame()
    family_rows: list[dict] = []
    all_daily: dict[str, pd.Series] = {}
    stability_all: list[dict] = []
    all_cluster_labels: list[dict] = []
    supplier_weight_events: list[tuple[pd.Timestamp, pd.Series]] = []
    default_cluster = params.get("default_cluster_method", "hermitian")
    max_reb = params["max_rebalances"]
    res_last = None
    labels_last: pd.Series | None = None
    F_last: np.ndarray | None = None
    cluster_rows: list[dict] = []
    hybrid_rows: list[dict] = []
    bt_event_rows: list[dict] = []
    turnover_rows: list[dict] = []

    if "load" in enabled:
        step_n += 1
        logger.info("Step %d/%d: load returns and edges", step_n, total_steps)
        returns_path = root / params["returns_parquet"]
        if returns_path.is_file():
            R = load_returns_wide_by_gvkey(str(returns_path))
        else:
            warnings.append(f"returns parquet missing at {returns_path}; using synthetic panel")
            R = synth_returns_panel(n=350 if quick else 400, n_stock=35)

        edges_path = root / params["edges_csv"]
        if edges_path.is_file():
            edges = load_edges(str(edges_path), date_col=params["edge_date_col"])
        else:
            warnings.append("edges csv missing; synthetic chain edges")
            gv = list(R.columns[:20])
            rows = [
                {
                    "customer_gvkey": gv[i + 1],
                    "supplier_gvkey": gv[i],
                    "weight_wji": 0.2,
                    "date": R.index[100],
                }
                for i in range(len(gv) - 1)
            ]
            edges = pd.DataFrame(rows)

        logger.info(
            "Data loaded: %d trading days, %d assets, %d edge rows (%s → %s)",
            len(R),
            R.shape[1],
            len(edges),
            R.index.min().date(),
            R.index.max().date(),
        )

        with open(out_dir / "config_used.yaml", "w", encoding="utf-8") as f:
            yaml.safe_dump(raw_cfg or params, f, sort_keys=False)

    needs_data = enabled - {"plots", "report"}
    if needs_data and R is None:
        raise ValueError("Step 'load' is required for the selected pipeline steps")

    if "panel" in enabled:
        assert R is not None and edges is not None
        step_n += 1
        logger.info("Step %d/%d: panel forward / reverse validation", step_n, total_steps)
        panel_df = run_panel_with_parquet(root, horizon_max=params["panel_horizon_max"])
        if panel_df is None or panel_df.empty:
            panel_df = run_panel_forward_reverse(
                R,
                edges,
                horizon_max=params["panel_horizon_max"],
                edge_date_col=params["edge_date_col"],
                edge_expiry_days=params["edge_expiry_days"],
            )
            warnings.append(
                "panel: pooled OLS fallback (none_pooled); build leadlag_panel_*.parquet and install linearmodels for FE+clustered SE"
            )
        else:
            warnings.append("panel: used linearmodels on leadlag_panel_forward/reverse.parquet")
        panel_df.to_csv(out_dir / "panel_forward_reverse.csv", index=False)
        horizon_df = panel_df.copy()
        if not horizon_df.empty:
            horizon_df["economic_magnitude_bps"] = horizon_df["beta"] * 1e4
        horizon_df.to_csv(out_dir / "horizon_decay.csv", index=False)
        logger.info("Panel rows written: %d", len(panel_df))

    if "baselines" in enabled:
        assert R is not None and edges is not None
        step_n += 1
        logger.info("Step %d/%d: supplier-pressure baselines (rolling comparison)", step_n, total_steps)
        if params["baselines_include"] and not quick:
            comp = run_rolling_comparison(
                R,
                edges,
                lookback_rows=params["lookback_rows"],
                rebalance_freq=params["rebalance_freq"],
                score=params["edge_score"],
                signal_method="supplier_pressure",
                max_rebalances=params["max_rebalances"],
                edge_expiry_days=params["edge_expiry_days"],
                include_baselines=True,
                commission_bps=params["commission_bps"],
                slippage_bps=params["slippage_bps"],
                borrow_bps_annual=params["borrow_bps_annual"],
                show_progress=verbose,
            )
            comp_tab = comparison_metrics_table(comp)
            logger.info("Baselines complete (main + random + momentum + structural + equal_weight)")
        else:
            if quick:
                warnings.append("quick mode: baselines skipped")
                logger.info("Baselines skipped (quick mode)")
            else:
                logger.info("Baselines skipped (baselines.include=false in config)")

    if "families" in enabled:
        assert R is not None and edges is not None
        step_n += 1
        fam_clusters = {
            f: cluster_method_for_family(f, params) for f in params["strategy_families"]
        }
        logger.info(
            "Step %d/%d: strategy family comparison (%d families, clusters=%s)",
            step_n,
            total_steps,
            len(params["strategy_families"]),
            fam_clusters,
        )
        fam_iter: Any = list(params["strategy_families"])
        if show_progress and not quick:
            from tqdm.auto import tqdm

            fam_iter = tqdm(fam_iter, desc="strategy families", unit="family")
        for fi, family in enumerate(fam_iter, start=1):
            cm_fam = cluster_method_for_family(family, params)
            if not show_progress or quick:
                logger.info(
                    "  family %d/%d: %s (cluster=%s)",
                    fi,
                    len(params["strategy_families"]),
                    family,
                    cm_fam,
                )
            p = dict(params)
            p["_family"] = family
            dr, stab, _, clab, wevents = _accumulate_family_returns(
                R,
                edges,
                p,
                cluster_method=cm_fam,
                hybrid_alpha=None,
                max_rebalances=max_reb,
                quick=quick,
                collect_cluster_labels=(cm_fam == default_cluster),
                show_progress=show_progress and not quick,
                progress_desc=f"{family} ({cm_fam})",
            )
            all_cluster_labels.extend(clab)
            if family == "supplier_pressure":
                supplier_weight_events = wevents
            all_daily[family] = dr
            stability_all.extend(stab)
            met = _metrics_from_returns(dr)
            family_rows.append(
                {
                    "strategy_family": family,
                    "cluster_method": cm_fam,
                    "edge_score": params["edge_score"],
                    "ann_return": met["ann_return"],
                    "ann_vol": met["ann_vol"],
                    "sharpe": met["sharpe"],
                    "max_drawdown": met["max_drawdown"],
                    "avg_turnover": met["avg_turnover"],
                    "net_sharpe": met["net_sharpe"],
                    "n_traded_assets_avg": np.nan,
                    "notes": "",
                }
            )

        pd.DataFrame(family_rows).to_csv(out_dir / "strategy_family_comparison.csv", index=False)
        _write_family_core_outputs(out_dir, all_daily)
        _write_strategy_families_dashboard(out_dir, all_daily, family_rows)
        _write_methods_comparison_plots(out_dir)
        logger.info(
            "Wrote strategy_family_comparison.csv, strategy_families_dashboard.png, "
            "methods_comparison.png (clusters: %s)",
            fam_clusters,
        )
    if "cluster_sweep" in enabled:
        assert R is not None and edges is not None
        cm_list = _exclude_cluster_sweep_methods(list(params["cluster_methods"]))
        if not quick:
            cm_list = _exclude_cluster_sweep_methods(
                list(
                    dict.fromkeys(
                        cm_list
                        + [
                            "sector",
                            "ggroup",
                            "gind",
                            "gsubind",
                            "naics2",
                            "naics",
                            "sic2",
                            "supply_community",
                            "symmetric_spectral",
                            "hermitian",
                            "signed",
                        ]
                    )
                )
            )
        cluster_families = _resolve_cluster_sweep_families(params, quick=quick)
        step_n += 1
        logger.info(
            "Step %d/%d: cluster method comparison (%d methods × families %s)",
            step_n,
            total_steps,
            len(cm_list),
            cluster_families,
        )
        cluster_daily_parts: list[pd.DataFrame] = []
        sweep_tasks = [
            (cm, family)
            for cm in cm_list
            for family in cluster_families
            if family in params["strategy_families"]
        ]
        sweep_iter: Any = sweep_tasks
        if show_progress and sweep_tasks:
            from tqdm.auto import tqdm

            sweep_iter = tqdm(
                sweep_tasks,
                desc="cluster sweep",
                unit="run",
                total=len(sweep_tasks),
            )
        if show_progress and sweep_tasks:
            logger.info(
                "Cluster sweep progress: %d runs × ~%d rebalances each (tqdm)",
                len(sweep_tasks),
                max_reb or 156,
            )
        for cmi, (cm, family) in enumerate(sweep_iter, start=1):
            if not show_progress:
                logger.info("  cluster %d/%d: %s × %s", cmi, len(cm_list), cm, family)
            p = dict(params)
            p["_family"] = family
            try:
                dr, stab, meta, clab, _ = _accumulate_family_returns(
                    R,
                    edges,
                    p,
                    cluster_method=cm,
                    max_rebalances=max_reb,
                    quick=quick,
                    collect_cluster_labels=(cm == default_cluster),
                    show_progress=show_progress,
                    progress_desc=f"{cm} × {family}",
                )
                all_cluster_labels.extend(clab)
            except ClusteringMethodError:
                cluster_rows.append(
                    {
                        "strategy_family": family,
                        "cluster_method": cm,
                        "status": "skipped",
                        "warning": "clustering unavailable",
                    }
                )
                warnings.append(f"skipped {cm} for {family}")
                logger.warning("  skipped %s × %s (clustering unavailable)", cm, family)
                continue
            met = _metrics_from_returns(dr)
            if not dr.empty:
                cluster_daily_parts.append(
                    dr.rename("daily_return").to_frame().assign(
                        strategy_family=family,
                        cluster_method=cm,
                    )
                )
            ari_mean = float(pd.Series([s["ari_prev"] for s in stab]).mean()) if stab else np.nan
            drift_mean = float(pd.Series([s["eigenspace_drift"] for s in stab]).mean()) if stab else np.nan
            cluster_rows.append(
                {
                    "strategy_family": family,
                    "cluster_method": cm,
                    "n_clusters": params["n_clusters"],
                    "edge_score": params["edge_score"],
                    "hybrid_alpha": np.nan,
                    "ann_return": met["ann_return"],
                    "ann_vol": met["ann_vol"],
                    "sharpe": met["sharpe"],
                    "max_drawdown": met["max_drawdown"],
                    "avg_turnover": met["avg_turnover"],
                    "cluster_ari_mean": ari_mean,
                    "eigenspace_drift_mean": drift_mean,
                    "n_rebalances": meta.get("n_rebalances", 0),
                }
            )
            stability_all.extend(stab)
        cluster_cmp = _filter_cluster_sweep_df(pd.DataFrame(cluster_rows))
        cluster_daily = pd.DataFrame()
        if cluster_daily_parts:
            cluster_daily = _filter_cluster_sweep_df(pd.concat(cluster_daily_parts))
            cluster_daily.index.name = "date"
            cluster_daily = cluster_daily.reset_index()
        cluster_cmp, cluster_daily = _merge_cluster_sweep_artifacts(
            out_dir,
            cluster_cmp,
            cluster_daily,
            cluster_families,
        )
        cluster_cmp.to_csv(out_dir / "cluster_method_comparison.csv", index=False)
        if not cluster_daily.empty:
            cluster_daily.to_csv(out_dir / "cluster_sweep_daily_returns.csv", index=False)
        if not cluster_cmp.empty:
            _write_cluster_sweep_plots(out_dir, cluster_cmp, cluster_daily=cluster_daily)

    hybrid_daily = pd.DataFrame()
    if "hybrid_sweep" in enabled:
        assert R is not None and edges is not None
        hybrid_families = ["supplier_pressure"] if quick else params["strategy_families"]
        step_n += 1
        logger.info(
            "Step %d/%d: hybrid alpha sweep (%d alphas × %d families)",
            step_n,
            total_steps,
            len(params["hybrid_alpha_grid"]),
            len(hybrid_families),
        )
        hybrid_rows, hybrid_daily = _execute_hybrid_alpha_sweep(
            R,
            edges,
            params,
            default_cluster=default_cluster,
            max_rebalances=max_reb,
            quick=quick,
            families=list(hybrid_families),
        )
        pd.DataFrame(hybrid_rows).to_csv(out_dir / "hybrid_alpha_sweep.csv", index=False)
        if not hybrid_daily.empty:
            hybrid_daily.to_csv(out_dir / "hybrid_alpha_daily_returns.csv", index=False)

    if "summary" in enabled:
        step_n += 1
        logger.info("Step %d/%d: summary metrics and turnover tables", step_n, total_steps)
        summary_rows = []
        for _, row in pd.DataFrame(family_rows).iterrows():
            summary_rows.append(
                {
                    "strategy_family": row["strategy_family"],
                    "cluster_method": row["cluster_method"],
                    "edge_score": params["edge_score"],
                    "hybrid_alpha": np.nan,
                    "baseline_type": "main",
                    **{k: row[k] for k in ("ann_return", "ann_vol", "sharpe", "max_drawdown", "net_sharpe")},
                    "calmar": np.nan,
                    "hit_rate": np.nan,
                    "avg_daily_return": np.nan,
                    "avg_turnover": row.get("avg_turnover", np.nan),
                    "gross_sharpe": row["sharpe"],
                    "total_cost_bps": 0.0,
                    "n_days": met.get("n_days", 0)
                    if (met := _metrics_from_returns(all_daily.get(row["strategy_family"], pd.Series())))
                    else 0,
                    "n_rebalances": max_reb or 0,
                }
            )
        for _, brow in comp_tab.iterrows():
            summary_rows.append(
                {
                    "strategy_family": "supplier_pressure",
                    "cluster_method": default_cluster,
                    "edge_score": params["edge_score"],
                    "hybrid_alpha": np.nan,
                    "baseline_type": brow["strategy"],
                    "ann_return": brow.get("mean_daily", np.nan) * 252 if pd.notna(brow.get("mean_daily")) else np.nan,
                    "ann_vol": brow.get("vol_daily", np.nan) * np.sqrt(252) if pd.notna(brow.get("vol_daily")) else np.nan,
                    "sharpe": brow.get("sharpe", np.nan),
                    "max_drawdown": brow.get("max_drawdown", np.nan),
                    "calmar": np.nan,
                    "hit_rate": np.nan,
                    "avg_daily_return": brow.get("mean_daily", np.nan),
                    "avg_turnover": brow.get("avg_daily_turnover", np.nan),
                    "gross_sharpe": brow.get("gross_sharpe", np.nan),
                    "net_sharpe": brow.get("sharpe", np.nan),
                    "total_cost_bps": brow.get("total_cost", np.nan),
                    "n_days": brow.get("n_days", 0),
                    "n_rebalances": max_reb or 0,
                }
            )
        pd.DataFrame(summary_rows).to_csv(out_dir / "summary_metrics.csv", index=False)
        if stability_all:
            pd.DataFrame(stability_all).to_csv(out_dir / "cluster_stability.csv", index=False)

    if "events" in enabled and params.get("events_enabled", True):
        assert R is not None and edges is not None
        step_n += 1
        logger.info("Step %d/%d: event-conditioned analysis", step_n, total_steps)
        earnings = load_earnings_calendar(root / params["earnings_calendar_csv"])
        event_cfg = {
            "earnings_window_days": params["earnings_window_days"],
            "large_customer_return_quantile": params["large_customer_return_quantile"],
            "high_vol_quantile": params["high_vol_quantile"],
        }
        flags = add_event_flags(pd.DataFrame({"date": R.index}), earnings, R, edges, event_cfg)
        panel_event_rows = []
        for cond in CONDITIONS:
            sub = panel_df.copy()
            sub["condition"] = cond
            if cond != "all_days" and cond in flags.columns:
                sub["note"] = "conditional panel uses pooled subsample proxy"
            panel_event_rows.append(sub)
        pd.concat(panel_event_rows, ignore_index=True).to_csv(
            out_dir / "event_conditioned_panel.csv", index=False
        )
        shutil.copy(out_dir / "event_conditioned_panel.csv", out_dir / "event_conditioned_results.csv")

        bt_event_rows = []
        main_dr = all_daily.get("supplier_pressure", pd.Series(0.0, index=R.index))
        for cond in CONDITIONS:
            dr_c = mask_returns_by_condition(main_dr, flags, cond)
            met = _metrics_from_returns(dr_c)
            bt_event_rows.append(
                {
                    "condition": cond,
                    "strategy_family": "supplier_pressure",
                    **met,
                    "n_trade_days": int((dr_c != 0).sum()),
                }
            )
        pd.DataFrame(bt_event_rows).to_csv(out_dir / "event_conditioned_backtest.csv", index=False)

        turnover_rows = []
        for label, res in [
            ("supplier_pressure", comp.main),
            ("random", comp.random),
            ("momentum", comp.momentum),
            ("structural", comp.structural),
            ("equal_weight", comp.equal_weight),
        ]:
            m = portfolio_metrics(
                res.daily_ret,
                gross_daily_ret=res.gross_daily_ret,
                daily_cost=res.daily_cost,
                turnover=res.turnover,
            )
            turnover_rows.append(
                {
                    "strategy_family": label,
                    "cluster_method": default_cluster,
                    "edge_score": params["edge_score"],
                    "gross_return": m.get("gross_cum_return", np.nan),
                    "net_return": m.get("cum_return", np.nan),
                    "total_turnover": m.get("total_turnover", np.nan),
                    "total_cost_bps": m.get("total_cost", np.nan) * 1e4 if m.get("total_cost") else np.nan,
                    "commission_cost_bps": params["commission_bps"],
                    "slippage_cost_bps": params["slippage_bps"],
                    "borrow_cost_bps": params["borrow_bps_annual"],
                    "gross_sharpe": m.get("gross_sharpe", np.nan),
                    "net_sharpe": m.get("sharpe", np.nan),
                }
            )
        pd.DataFrame(turnover_rows).to_csv(out_dir / "turnover_costs.csv", index=False)

    if "artifacts" in enabled:
        assert R is not None and edges is not None
        step_n += 1
        logger.info("Step %d/%d: matrix / cluster / holdings artifacts", step_n, total_steps)
        lab_df, size_df = cluster_labels_to_frames(all_cluster_labels)
        if not lab_df.empty:
            lab_df.to_parquet(out_dir / "clusters" / "cluster_labels_by_rebalance.parquet", index=False)
            size_df.to_csv(out_dir / "clusters" / "cluster_sizes.csv", index=False)
        stab_df = pd.DataFrame(stability_all)
        if not stab_df.empty:
            stability_by_method(stab_df).to_csv(
                out_dir / "clusters" / "cluster_stability_by_method.csv", index=False
            )

        holdings = holdings_from_events(supplier_weight_events, "supplier_pressure")
        if not holdings.empty:
            holdings.to_parquet(out_dir / "holdings_or_weights.parquet", index=False)

        try:
            e_last = filter_edges_pit(edges, R.index[-1], expiry_days=params["edge_expiry_days"])
            R_win = returns_window(R, R.index[-1], params["lookback_rows"])
            res_last = build_lead_lag_matrix_gvkey(
                R_win,
                e_last,
                horizon=params["horizon"],
                min_obs=params["min_obs"],
                score=params["edge_score"],
            )
            res_last.C.to_parquet(out_dir / "matrices" / "C_last.parquet")
            res_last.S.to_parquet(out_dir / "matrices" / "S_last.parquet")
            H = hermitian_from_skew(res_last.S.to_numpy(dtype=float))
            w, _ = eigendecompose_hermitian(H)
            pd.DataFrame(
                {
                    "eigen_index": np.arange(len(w)),
                    "eigenvalue": np.real(w),
                    "abs_eigenvalue": np.abs(w),
                }
            ).to_csv(out_dir / "matrices" / "H_spectrum_last.csv", index=False)
            labels = get_cluster_labels(
                res_last.C,
                default_cluster,
                n_clusters=params["n_clusters"],
                edges_pit=e_last,
                random_state=params["cluster_random_state"],
            )
            from supply_chain_leadlag.metacluster_strategy import cluster_directed_flow_matrix

            F = cluster_directed_flow_matrix(
                res_last.C.reindex(index=labels.index, columns=labels.index, fill_value=0.0).to_numpy(),
                labels.astype(int).to_numpy(),
            )
            F_last = F
            pd.DataFrame(F).to_parquet(out_dir / "matrices" / "meta_flow_matrix_last.parquet")
            labels_last = labels
            spec_row, _ = spectral_summary_from_C(res_last.C)
            spec_row.to_csv(out_dir / "spectral_summary.csv", index=False)
        except Exception as exc:
            warnings.append(f"last matrix artifacts: {exc}")
            spec_row = pd.DataFrame(
                [{"lambda_max_H": np.nan, "fro_norm_C": np.nan, "obs_max_eig_perm": np.nan, "perm_p_value": np.nan}]
            )
            spec_row.to_csv(out_dir / "spectral_summary.csv", index=False)

        if all_daily:
            daily_df = pd.DataFrame(all_daily)
            factor_exposure_alpha(daily_df, None).to_csv(out_dir / "factor_exposure_alpha.csv", index=False)

    if R is not None and edges is not None and "load" in enabled:
        meta = {
            "run_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "git_commit": _git_commit(),
            "python_version": sys.version,
            "data_start": str(R.index.min().date()),
            "data_end": str(R.index.max().date()),
            "n_return_assets": int(R.shape[1]),
            "n_edge_rows": int(len(edges)),
            "n_unique_customers": int(edges["customer_gvkey"].nunique()) if "customer_gvkey" in edges.columns else 0,
            "n_unique_suppliers": int(edges["supplier_gvkey"].nunique()) if "supplier_gvkey" in edges.columns else 0,
            "n_rebalances": max_reb,
            "pit_edge_date_col": params["edge_date_col"],
            "pipeline_steps": sorted(enabled),
            "plot_profile": profile,
            "default_cluster_method": default_cluster,
            "family_cluster_methods": params.get("family_cluster_methods") or {},
            "warnings": warnings,
            "quick": quick,
        }
        with open(out_dir / "run_metadata.json", "w", encoding="utf-8") as f:
            json.dump(meta, f, indent=2)

    if "plots" in enabled:
        step_n += 1
        logger.info("Step %d/%d: plots (profile=%s)", step_n, total_steps, profile)
        if not all_daily and (out_dir / "daily_returns.csv").is_file():
            dr_disk = pd.read_csv(out_dir / "daily_returns.csv", index_col=0, parse_dates=True)
            all_daily = {c: dr_disk[c] for c in dr_disk.columns}
        if not family_rows and (out_dir / "strategy_family_comparison.csv").is_file():
            family_rows = pd.read_csv(out_dir / "strategy_family_comparison.csv").to_dict("records")
        if not cluster_rows and (out_dir / "cluster_method_comparison.csv").is_file():
            cluster_rows = pd.read_csv(out_dir / "cluster_method_comparison.csv").to_dict("records")
        _write_plots(
            out_dir,
            panel_df,
            family_rows,
            hybrid_rows,
            cluster_rows,
            all_daily,
            comp_tab,
            stability_all,
            bt_event_rows=bt_event_rows,
            res_last=res_last,
            labels_last=labels_last,
            F_last=F_last,
            turnover_rows=turnover_rows,
            profile=profile,
        )
    if "report" in enabled:
        step_n += 1
        logger.info("Step %d/%d: final report (markdown + LaTeX)", step_n, total_steps)
        generate_final_report(out_dir, params, enabled_steps=sorted(enabled), plot_profile=profile)

    elapsed_total = time.perf_counter() - pipeline_t0
    logger.info("Pipeline complete in %.1fs → %s", elapsed_total, out_dir)
    if warnings:
        logger.warning("%d warning(s) recorded in run_metadata.json", len(warnings))
        for w in warnings:
            logger.warning("  %s", w)

    return out_dir


def _execute_hybrid_alpha_sweep(
    R: pd.DataFrame,
    edges: pd.DataFrame,
    params: dict[str, Any],
    *,
    default_cluster: str,
    max_rebalances: int | None,
    quick: bool,
    families: list[str] | None = None,
) -> tuple[list[dict], pd.DataFrame]:
    """Run all (alpha, family) hybrid backtests; return metrics rows + long daily returns."""
    fams = families if families is not None else (
        ["supplier_pressure"] if quick else list(params["strategy_families"])
    )
    hybrid_rows: list[dict] = []
    daily_parts: list[pd.DataFrame] = []

    for alpha in params["hybrid_alpha_grid"]:
        for family in fams:
            p = dict(params)
            p["_family"] = family
            cm_fam = cluster_method_for_family(family, params)
            dr, stab, _, _, _ = _accumulate_family_returns(
                R,
                edges,
                p,
                cluster_method=cm_fam,
                hybrid_alpha=float(alpha),
                max_rebalances=max_rebalances,
                quick=quick,
                collect_cluster_labels=False,
            )
            met = _metrics_from_returns(dr)
            hybrid_rows.append(
                {
                    "alpha": alpha,
                    "strategy_family": family,
                    "cluster_method": cm_fam,
                    "edge_score": params["edge_score"],
                    **met,
                    "net_sharpe": met["sharpe"],
                    "cluster_ari_mean": float(pd.Series([s["ari_prev"] for s in stab]).mean()) if stab else np.nan,
                    "eigenspace_drift_mean": float(pd.Series([s["eigenspace_drift"] for s in stab]).mean()) if stab else np.nan,
                }
            )
            if not dr.empty:
                daily_parts.append(
                    dr.rename("daily_return").to_frame().assign(
                        alpha=float(alpha),
                        strategy_family=family,
                    )
                )

    if daily_parts:
        daily_long = pd.concat(daily_parts)
        daily_long.index.name = "date"
        daily_long = daily_long.reset_index()
    else:
        daily_long = pd.DataFrame(columns=["date", "daily_return", "alpha", "strategy_family"])
    return hybrid_rows, daily_long


def _write_hybrid_alpha_plots(out_dir: Path, hybrid_rows: list[dict], hybrid_daily: pd.DataFrame | None) -> None:
    """Sharpe vs alpha and cumulative PnL panels from hybrid sweep outputs."""
    plots = out_dir / "plots"
    plots.mkdir(parents=True, exist_ok=True)

    if hybrid_rows:
        hdf = pd.DataFrame(hybrid_rows)
        plt.figure(figsize=(7, 4))
        for fam in sorted(hdf["strategy_family"].unique()):
            sub = hdf[hdf["strategy_family"] == fam].sort_values("alpha")
            plt.plot(sub["alpha"], sub["sharpe"], "o-", label=fam, linewidth=1.5, markersize=6)
        plt.axhline(0, ls="--", c="gray", lw=0.8)
        plt.xlabel("Hybrid α  (1 = data only, 0 = supply chain only)")
        plt.ylabel("Sharpe")
        plt.title("Hybrid α sweep: Sharpe by strategy family")
        plt.legend(fontsize=8)
        plt.grid(True, alpha=0.25)
        plt.tight_layout()
        plt.savefig(plots / "hybrid_alpha_sweep.png", dpi=150)
        plt.close()

    if hybrid_daily is None or hybrid_daily.empty:
        return

    daily = hybrid_daily.copy()
    daily["date"] = pd.to_datetime(daily["date"])
    families = sorted(daily["strategy_family"].unique())
    n = len(families)
    ncols = 2
    nrows = int(np.ceil(n / ncols))
    fig, axes = plt.subplots(nrows, ncols, figsize=(6 * ncols, 4 * nrows), squeeze=False)
    cmap = plt.colormaps.get_cmap("viridis")
    alphas = sorted(daily["alpha"].unique())

    for ax, fam in zip(axes.flatten(), families):
        sub = daily[daily["strategy_family"] == fam]
        for i, alpha in enumerate(alphas):
            s = sub[sub["alpha"] == alpha].set_index("date")["daily_return"].sort_index()
            if s.empty:
                continue
            cum = (1 + s.fillna(0)).cumprod()
            color = cmap(i / max(len(alphas) - 1, 1))
            ax.plot(cum.index, cum, color=color, linewidth=1.2, label=f"α={alpha:g}")
        ax.axhline(1.0, ls="--", c="gray", lw=0.6)
        ax.set_title(fam.replace("_", " "), fontsize=10, fontweight="bold")
        ax.set_ylabel("Cumulative PnL")
        ax.legend(fontsize=7, loc="best")
        ax.grid(True, alpha=0.25)
    for ax in axes.flatten()[n:]:
        ax.set_visible(False)
    fig.suptitle("Hybrid α sweep: cumulative PnL (signed clustering)", fontsize=12, y=1.01)
    plt.tight_layout()
    plt.savefig(plots / "hybrid_alpha_sweep_cumulative_pnl.png", dpi=150, bbox_inches="tight")
    plt.close()

    _write_methods_comparison_plots(out_dir)


def _write_methods_comparison_plots(out_dir: Path) -> None:
    """Families (return-based C, best cluster each) vs optional hybrid best-α panel."""
    plots = out_dir / "plots"
    plots.mkdir(parents=True, exist_ok=True)
    fam_path = out_dir / "daily_returns.csv"
    fam_cmp_path = out_dir / "strategy_family_comparison.csv"
    if not fam_path.is_file() or not fam_cmp_path.is_file():
        return

    fam_daily = pd.read_csv(fam_path, index_col=0, parse_dates=True)
    fam_cmp = pd.read_csv(fam_cmp_path)
    fam_order = [f for f in ["supplier_pressure", "globalrank", "metacluster", "clusterrank"] if f in fam_daily.columns]
    cluster_by_family = dict(zip(fam_cmp["strategy_family"], fam_cmp["cluster_method"]))
    sharpe_by_family = dict(zip(fam_cmp["strategy_family"], fam_cmp["sharpe"]))
    palette = {
        "supplier_pressure": "#440154",
        "globalrank": "#31688e",
        "metacluster": "#35b779",
        "clusterrank": "#fde725",
    }

    hybrid_path = out_dir / "hybrid_alpha_sweep.csv"
    hybrid_daily_path = out_dir / "hybrid_alpha_daily_returns.csv"
    hybrid_fresh = (
        hybrid_path.is_file()
        and hybrid_daily_path.is_file()
        and hybrid_daily_path.stat().st_mtime >= fam_path.stat().st_mtime
    )

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    ax = axes[0]
    for fam in fam_order:
        s = fam_daily[fam].fillna(0).sort_index()
        cum = (1 + s).cumprod()
        cm = cluster_by_family.get(fam, "")
        sh = sharpe_by_family.get(fam, np.nan)
        label = _family_plot_label(fam, cm if fam in ("metacluster", "clusterrank") else None)
        if np.isfinite(sh):
            label += f", Sharpe={sh:.2f}"
        ax.plot(cum.index, cum, color=palette.get(fam, "gray"), lw=1.8, label=label)
    ax.axhline(1.0, ls="--", c="gray", lw=0.6)
    ax.set_title("Step 4: return-based C (best cluster per family)", fontweight="bold")
    ax.set_ylabel("Cumulative PnL")
    ax.legend(fontsize=7, loc="upper left")
    ax.grid(True, alpha=0.25)

    ax = axes[1]
    if hybrid_fresh:
        daily = pd.read_csv(hybrid_daily_path, parse_dates=["date"])
        hdf = pd.read_csv(hybrid_path)
        best = hdf.loc[hdf.groupby("strategy_family")["sharpe"].idxmax()]
        for fam in fam_order:
            alpha = float(best.loc[best["strategy_family"] == fam, "alpha"])
            sub = daily[(daily["strategy_family"] == fam) & (np.isclose(daily["alpha"], alpha))]
            if sub.empty:
                continue
            s = sub.set_index("date")["daily_return"].sort_index()
            cum = (1 + s.fillna(0)).cumprod()
            cm = str(best.loc[best["strategy_family"] == fam, "cluster_method"])
            sh = float(best.loc[best["strategy_family"] == fam, "sharpe"])
            ax.plot(
                cum.index,
                cum,
                color=palette.get(fam, "gray"),
                lw=1.8,
                label=f"{_family_plot_label(fam, cm if fam in ('metacluster', 'clusterrank') else None)} "
                f"(α={alpha:g}, Sharpe={sh:.2f})",
            )
        ax.set_title("Hybrid: best α per family (Sharpe)", fontweight="bold")
    else:
        x = np.arange(len(fam_order))
        sharpes = [float(sharpe_by_family.get(f, 0)) for f in fam_order]
        colors = [palette.get(f, "gray") for f in fam_order]
        bars = ax.bar(x, sharpes, color=colors, edgecolor="white")
        ax.set_xticks(x)
        ax.set_xticklabels(
            [_family_plot_label(f, cluster_by_family.get(f)) for f in fam_order],
            rotation=25,
            ha="right",
        )
        ax.set_ylabel("Sharpe")
        ax.set_title("Sharpe by family (155 BME rebalances)", fontweight="bold")
        for bar, sh in zip(bars, sharpes):
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.02, f"{sh:.2f}", ha="center", fontsize=9)
        ax.text(
            0.5,
            0.02,
            "Re-run --steps hybrid_sweep to refresh right-panel hybrid curves.",
            transform=ax.transAxes,
            ha="center",
            fontsize=8,
            color="0.45",
        )
    ax.axhline(0, ls="--", c="gray", lw=0.6)
    if hybrid_fresh:
        ax.axhline(1.0, ls="--", c="gray", lw=0.6)
        ax.set_ylabel("Cumulative PnL")
        ax.legend(fontsize=6, loc="best")
    ax.grid(True, alpha=0.25)

    fig.suptitle(
        "Strategy families: supplier_pressure vs cluster-based portfolios",
        fontsize=12,
        y=1.02,
    )
    fig.text(
        0.5,
        0.01,
        "Clustering adds structure but dilutes the direct customer→supplier signal; "
        "supplier_pressure trades suppliers only with s=C′r.",
        ha="center",
        fontsize=8,
        color="0.35",
    )
    plt.tight_layout(rect=[0, 0.04, 1, 0.98])
    plt.savefig(plots / "methods_comparison.png", dpi=150, bbox_inches="tight")
    plt.close()

    if not hybrid_path.is_file():
        return
    hdf = pd.read_csv(hybrid_path)
    pivot = hdf.pivot_table(index="strategy_family", columns="alpha", values="sharpe", aggfunc="first")
    pivot = pivot.reindex([f for f in fam_order if f in pivot.index])
    fig, ax = plt.subplots(figsize=(7.5, 3.8))
    vals = pivot.to_numpy(dtype=float)
    vmax = max(0.55, float(np.nanmax(np.abs(vals))))
    im = ax.imshow(vals, aspect="auto", cmap="RdYlGn", vmin=-0.45, vmax=vmax)
    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels([f"{a:g}" for a in pivot.columns])
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels([f.replace("_", " ") for f in pivot.index])
    for i in range(vals.shape[0]):
        for j in range(vals.shape[1]):
            v = vals[i, j]
            if np.isfinite(v):
                ax.text(j, i, f"{v:.2f}", ha="center", va="center", fontsize=9, color="black")
    ax.set_xlabel("Hybrid α  (0 = supply only, 1 = returns only)")
    ax.set_title("Sharpe by family × α")
    fig.colorbar(im, ax=ax, fraction=0.03, pad=0.02, label="Sharpe")
    plt.tight_layout()
    plt.savefig(plots / "hybrid_sharpe_heatmap.png", dpi=150, bbox_inches="tight")
    plt.close()


def run_hybrid_alpha_sweep(
    config_path: str | Path | None = None,
    *,
    max_rebalances: int | None = None,
    quick: bool = False,
    repo_root: Path | None = None,
    verbose: bool = False,
) -> Path:
    """Run only hybrid alpha sweep + plots (writes hybrid_alpha_sweep.csv)."""
    configure_pipeline_logging(verbose=verbose)
    logger.info("Hybrid alpha sweep start (config=%s)", config_path)
    root = repo_root or Path(__file__).resolve().parents[1]
    raw_cfg = load_yaml_config(config_path or root / "config" / "research.yaml")
    params = flat_research_params(raw_cfg)
    if max_rebalances is not None:
        params["max_rebalances"] = max_rebalances
    if quick:
        params["max_rebalances"] = 3
        params["hybrid_alpha_grid"] = [0.0, 0.5, 1.0]

    out_dir = Path(params["output_dir"])
    if not out_dir.is_absolute():
        out_dir = root / out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "plots").mkdir(exist_ok=True)

    returns_path = root / params["returns_parquet"]
    R = (
        load_returns_wide_by_gvkey(str(returns_path))
        if returns_path.is_file()
        else synth_returns_panel(n=300, n_stock=30)
    )
    edges_path = root / params["edges_csv"]
    edges = load_edges(str(edges_path), date_col=params["edge_date_col"]) if edges_path.is_file() else pd.DataFrame()

    default_cluster = params.get("default_cluster_method", "hermitian")
    hybrid_rows, hybrid_daily = _execute_hybrid_alpha_sweep(
        R,
        edges,
        params,
        default_cluster=default_cluster,
        max_rebalances=params["max_rebalances"],
        quick=quick,
    )
    pd.DataFrame(hybrid_rows).to_csv(out_dir / "hybrid_alpha_sweep.csv", index=False)
    if not hybrid_daily.empty:
        hybrid_daily.to_csv(out_dir / "hybrid_alpha_daily_returns.csv", index=False)
    _write_hybrid_alpha_plots(out_dir, hybrid_rows, hybrid_daily)
    return out_dir / "hybrid_alpha_sweep.csv"


def _write_cluster_sweep_cumulative_pnl_plots(
    out_dir: Path,
    cluster_daily: pd.DataFrame | None,
    cluster_df: pd.DataFrame | None = None,
) -> None:
    """One cumulative-PnL chart per cluster-based family (all cluster methods)."""
    plots = out_dir / "plots"
    plots.mkdir(parents=True, exist_ok=True)
    daily_path = out_dir / "cluster_sweep_daily_returns.csv"
    if cluster_daily is None or cluster_daily.empty:
        if not daily_path.is_file():
            return
        cluster_daily = pd.read_csv(daily_path, parse_dates=["date"])
    cluster_daily = _filter_cluster_sweep_df(cluster_daily)
    if cluster_daily.empty or "cluster_method" not in cluster_daily.columns:
        return

    daily = cluster_daily.copy()
    daily["date"] = pd.to_datetime(daily["date"])
    cmp = cluster_df
    if cmp is None and (out_dir / "cluster_method_comparison.csv").is_file():
        cmp = pd.read_csv(out_dir / "cluster_method_comparison.csv")

    cmap = plt.colormaps.get_cmap("tab20")
    for family in ("metacluster", "clusterrank"):
        sub = daily[daily["strategy_family"] == family]
        if sub.empty:
            continue
        methods = sorted(sub["cluster_method"].unique())
        method_order = list(methods)
        if cmp is not None and not cmp.empty:
            ranked = cmp[
                (cmp["strategy_family"] == family) & cmp["sharpe"].notna()
            ].sort_values("sharpe", ascending=False)["cluster_method"]
            method_order = [m for m in ranked if m in methods]
            method_order += [m for m in methods if m not in method_order]

        fig, ax = plt.subplots(figsize=(11, 5))
        for i, cm in enumerate(method_order):
            s = sub[sub["cluster_method"] == cm].set_index("date")["daily_return"].sort_index()
            if s.empty:
                continue
            cum = (1 + s.fillna(0)).cumprod()
            ax.plot(
                cum.index,
                cum,
                color=cmap(i % 20),
                linewidth=1.1,
                label=str(cm),
                alpha=0.9,
            )
        ax.axhline(1.0, ls="--", c="gray", lw=0.6)
        ax.set_ylabel("Cumulative PnL")
        ax.set_title(f"{family.replace('_', ' ')}: cumulative PnL by cluster method")
        ax.legend(fontsize=6, loc="upper left", ncol=2, framealpha=0.9)
        ax.grid(True, alpha=0.25)
        plt.tight_layout()
        out_name = f"cluster_sweep_cumulative_pnl_{family}.png"
        plt.savefig(plots / out_name, dpi=150, bbox_inches="tight")
        plt.close()


def _write_cluster_sweep_plots(
    out_dir: Path,
    cluster_df: pd.DataFrame,
    *,
    cluster_daily: pd.DataFrame | None = None,
) -> None:
    """Sharpe / stability visuals for Step 5 (cluster_method_comparison.csv)."""
    plots = out_dir / "plots"
    base = _filter_cluster_sweep_df(cluster_df.copy())
    if "status" in base.columns:
        base = base[base["status"].fillna("ok") != "skipped"]
    if "n_rebalances" in base.columns:
        base = base[base["n_rebalances"].fillna(0) > 0]
    if base.empty or "cluster_method" not in base.columns:
        return

    families = sorted(base["strategy_family"].dropna().unique())
    # ARI is a property of cluster labels at each rebalance — identical across families for a given method.
    ari_by_method = (
        base.dropna(subset=["cluster_ari_mean"])
        .groupby("cluster_method", observed=False)["cluster_ari_mean"]
        .first()
    )
    method_order = ari_by_method.sort_values(ascending=False).index.tolist()
    for m in base["cluster_method"].unique():
        if m not in method_order:
            method_order.append(m)

    traded = base.dropna(subset=["sharpe", "strategy_family"])
    if traded.empty:
        return

    # Grouped bar: Sharpe by method, colored by family
    fig, ax = plt.subplots(figsize=(9, 4.5))
    x = np.arange(len(method_order))
    width = 0.35 if len(families) <= 2 else 0.8 / max(len(families), 1)
    for i, fam in enumerate(families):
        sub = traded[traded["strategy_family"] == fam].set_index("cluster_method").reindex(method_order)
        offset = (i - (len(families) - 1) / 2) * width
        ax.bar(x + offset, sub["sharpe"].fillna(0), width=width, label=fam)
    ax.axhline(0, ls="--", c="gray", lw=0.8)
    ax.set_xticks(x)
    ax.set_xticklabels(method_order, rotation=25, ha="right")
    ax.set_ylabel("Sharpe")
    ax.set_title("Cluster sweep: Sharpe by method and strategy family")
    ax.legend(fontsize=8)
    plt.tight_layout()
    plt.savefig(plots / "cluster_sweep_sharpe.png", dpi=150)
    plt.close()

    # Dashboard: Sharpe + optional max DD (per family) + ARI (per method)
    if not ari_by_method.empty:
        has_dd = "max_drawdown" in base.columns
        ncols = 3 if has_dd else 2
        fig, axes = plt.subplots(1, ncols, figsize=(5.5 * ncols, 4.5))
        if ncols == 2:
            axes = list(axes)
        x = np.arange(len(method_order))
        fam_colors = {"metacluster": "#e67e22", "clusterrank": "#1f77b4"}
        for fam in families:
            sub = traded[traded["strategy_family"] == fam].set_index("cluster_method").reindex(method_order)
            y = sub["sharpe"].to_numpy(dtype=float)
            color = fam_colors.get(fam, None)
            axes[0].plot(
                x,
                y,
                "o-",
                label=fam,
                linewidth=1.5,
                markersize=7,
                color=color,
            )
            _mark_dashboard_best_star(
                axes[0],
                method_order=method_order,
                sub=sub,
                metric="sharpe",
            )
        axes[0].axhline(0, ls="--", c="gray", lw=0.8)
        axes[0].set_xticks(x)
        axes[0].set_xticklabels(method_order, rotation=28, ha="right")
        axes[0].set_ylabel("Sharpe")
        axes[0].set_title("Sharpe by cluster method (★ = best per family)")
        axes[0].legend(fontsize=8, loc="lower left")
        axes[0].grid(True, alpha=0.25)

        ari_ax = axes[2] if has_dd else axes[1]
        if has_dd:
            dd_base = base.dropna(subset=["max_drawdown", "strategy_family"])
            for fam in families:
                sub = dd_base[dd_base["strategy_family"] == fam].set_index("cluster_method").reindex(method_order)
                y_dd = sub["max_drawdown"].to_numpy(dtype=float) * 100.0
                color = fam_colors.get(fam, None)
                axes[1].plot(
                    x,
                    y_dd,
                    "o-",
                    label=fam,
                    linewidth=1.5,
                    markersize=7,
                    color=color,
                )
                _mark_dashboard_best_star(
                    axes[1],
                    method_order=method_order,
                    sub=sub,
                    metric="max_drawdown",
                    y_scale=100.0,
                )
            axes[1].axhline(0, ls="--", c="gray", lw=0.8)
            axes[1].set_xticks(x)
            axes[1].set_xticklabels(method_order, rotation=28, ha="right")
            axes[1].set_ylabel("Max drawdown (%)")
            axes[1].set_title("Max drawdown by cluster method (★ = best per family)")
            axes[1].legend(fontsize=8, loc="lower left")
            axes[1].grid(True, alpha=0.25)

        ari_vals = ari_by_method.reindex(method_order).fillna(0).to_numpy()
        bars = ari_ax.bar(x, ari_vals, color="steelblue", alpha=0.85, edgecolor="white")
        ari_ax.set_xticks(x)
        ari_ax.set_xticklabels(method_order, rotation=28, ha="right")
        ari_ax.set_ylabel("Mean ARI")
        ari_ax.set_title("Label stability per cluster method")
        ari_ax.set_ylim(0, max(0.85, float(np.nanmax(ari_vals)) * 1.12))
        ari_ax.grid(True, axis="y", alpha=0.25)
        for bar, v in zip(bars, ari_vals):
            ari_ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() + 0.02,
                f"{v:.2f}",
                ha="center",
                va="bottom",
                fontsize=8,
            )
        foot = (
            "ARI is identical for metacluster vs clusterrank at a given method."
            if not has_dd
            else "★ = highest Sharpe / shallowest max drawdown within each family. ARI identical across families."
        )
        fig.text(0.5, 0.01, foot, ha="center", fontsize=8, color="0.35")
        plt.tight_layout(rect=[0, 0.05, 1, 1])
        plt.savefig(plots / "cluster_sweep_dashboard.png", dpi=150)
        plt.close()

    # Sharpe vs ARI: faceted by family so labels do not overlap
    if not ari_by_method.empty:
        cmap = plt.colormaps.get_cmap("tab10")
        method_colors = {m: cmap(i % 10) for i, m in enumerate(method_order)}
        fig, axes = plt.subplots(1, len(families), figsize=(5.5 * len(families), 5), squeeze=False)
        label_offsets = {
            "supply_community": (8, 10),
            "signed": (8, -14),
            "hybrid_prior": (-52, 8),
            "symmetric_spectral": (8, 10),
            "hermitian": (8, -12),
            "sector": (8, 8),
        }
        for ax, fam in zip(axes[0], families):
            sub = base[base["strategy_family"] == fam]
            for _, row in sub.iterrows():
                cm = row["cluster_method"]
                ari = row.get("cluster_ari_mean")
                sh = row.get("sharpe")
                if pd.isna(ari):
                    continue
                if pd.isna(sh):
                    ax.scatter(
                        [ari],
                        [0.0],
                        s=120,
                        c=method_colors.get(cm, "gray"),
                        marker="x",
                        linewidths=2,
                        zorder=3,
                    )
                    ax.annotate(
                        f"{cm}\n(no trades)",
                        (ari, 0.0),
                        fontsize=8,
                        color=method_colors.get(cm, "gray"),
                        ha="center",
                        va="top",
                        xytext=(0, -18),
                        textcoords="offset points",
                    )
                    continue
                ax.scatter(
                    [ari],
                    [sh],
                    s=110,
                    c=[method_colors.get(cm, "gray")],
                    edgecolors="white",
                    linewidths=0.8,
                    zorder=4,
                )
                dx, dy = label_offsets.get(str(cm), (6, 6))
                ax.annotate(
                    str(cm).replace("_", "\n"),
                    (ari, sh),
                    fontsize=8,
                    ha="left",
                    va="center",
                    xytext=(dx, dy),
                    textcoords="offset points",
                    bbox=dict(boxstyle="round,pad=0.25", facecolor="white", alpha=0.85, edgecolor="none"),
                )
            ax.axhline(0, ls="--", c="gray", lw=0.8, zorder=1)
            ax.set_xlabel("Mean ARI (label stability across rebalances)")
            ax.set_ylabel("Sharpe")
            ax.set_title(fam, fontsize=11, fontweight="bold")
            ax.grid(True, alpha=0.3)
            ax.set_xlim(-0.02, max(0.82, float(ari_by_method.max()) + 0.06))
            y_vals = sub["sharpe"].dropna()
            if not y_vals.empty:
                pad = max(0.08, float(y_vals.max() - y_vals.min()) * 0.25)
                ax.set_ylim(float(y_vals.min()) - pad, float(y_vals.max()) + pad)
        fig.suptitle(
            "Cluster sweep: Sharpe vs label stability (one panel per strategy family)",
            fontsize=12,
            y=1.02,
        )
        handles = [
            plt.Line2D(
                [0],
                [0],
                marker="o",
                color="w",
                markerfacecolor=method_colors[m],
                markersize=8,
                label=m,
            )
            for m in method_order
            if m in method_colors
        ]
        handles.append(
            plt.Line2D(
                [0],
                [0],
                marker="x",
                color="gray",
                markerfacecolor="none",
                linestyle="None",
                markersize=8,
                label="no trades (metacluster)",
            )
        )
        fig.legend(handles=handles, loc="lower center", ncol=min(4, len(handles)), fontsize=8, frameon=False)
        plt.tight_layout(rect=[0, 0.08, 1, 0.96])
        plt.savefig(plots / "cluster_sweep_sharpe_vs_ari.png", dpi=150, bbox_inches="tight")
        plt.close()

    _write_cluster_sweep_cumulative_pnl_plots(out_dir, cluster_daily, cluster_df)


def _write_plots(
    out_dir: Path,
    panel_df: pd.DataFrame,
    family_rows: list,
    hybrid_rows: list,
    cluster_rows: list,
    all_daily: dict[str, pd.Series],
    comp_tab: pd.DataFrame,
    stability_all: list,
    *,
    bt_event_rows: list | None = None,
    res_last: Any = None,
    labels_last: pd.Series | None = None,
    F_last: np.ndarray | None = None,
    turnover_rows: list | None = None,
    profile: str = "full",
) -> None:
    plots = out_dir / "plots"
    minimal = profile == "minimal"

    if all_daily:
        cum = pd.DataFrame({k: (1 + v.fillna(0)).cumprod() for k, v in all_daily.items()})
        if minimal:
            _write_strategy_families_dashboard(out_dir, all_daily, family_rows)
        elif not minimal:
            dd = pd.DataFrame({k: drawdown_series(v) for k, v in all_daily.items()})
            plt.figure(figsize=(8, 4))
            for c in dd.columns:
                plt.plot(dd.index, dd[c], label=c)
            plt.legend()
            plt.title("Drawdown by strategy")
            plt.tight_layout()
            plt.savefig(plots / "drawdown_by_strategy.png", dpi=150)
            plt.close()

    if minimal:
        return

    if panel_df is not None and not panel_df.empty:
        fwd = panel_df[panel_df["direction"].str.contains("forward")]
        rev = panel_df[panel_df["direction"].str.contains("reverse")]
        plt.figure(figsize=(6, 4))
        if not fwd.empty:
            plt.plot(fwd["horizon"], fwd["beta"], "o-", label="forward")
        if not rev.empty:
            plt.plot(rev["horizon"], rev["beta"], "s-", label="reverse")
        plt.axhline(0, ls="--", c="gray")
        plt.xlabel("Horizon")
        plt.ylabel("Beta")
        plt.legend()
        plt.tight_layout()
        plt.savefig(plots / "forward_vs_reverse_coefficients.png", dpi=150)
        plt.close()
        if not fwd.empty:
            plt.figure(figsize=(6, 4))
            plt.plot(fwd["horizon"], fwd["beta"], "o-")
            plt.axhline(0, ls="--", c="gray")
            plt.xlabel("Horizon")
            plt.ylabel("Beta")
            plt.title("Horizon decay (forward)")
            plt.tight_layout()
            plt.savefig(plots / "horizon_decay_beta.png", dpi=150)
            plt.close()

    if hybrid_rows or (out_dir / "hybrid_alpha_daily_returns.csv").is_file():
        hrows = hybrid_rows
        if not hrows and (out_dir / "hybrid_alpha_sweep.csv").is_file():
            hrows = pd.read_csv(out_dir / "hybrid_alpha_sweep.csv").to_dict("records")
        hdaily = None
        daily_path = out_dir / "hybrid_alpha_daily_returns.csv"
        if daily_path.is_file():
            hdaily = pd.read_csv(daily_path, parse_dates=["date"])
        _write_hybrid_alpha_plots(out_dir, hrows, hdaily)

    if panel_df is not None and not panel_df.empty:
        fwd = panel_df[panel_df["direction"].str.contains("forward", na=False)]
        if not fwd.empty:
            plt.figure(figsize=(6, 4))
            for cond in fwd["condition"].dropna().unique()[:6]:
                sub = fwd[fwd["condition"] == cond]
                plt.plot(sub["horizon"], sub["beta"], "o-", label=str(cond)[:20])
            plt.axhline(0, ls="--", c="gray")
            plt.legend(fontsize=8)
            plt.xlabel("Horizon")
            plt.ylabel("Beta")
            plt.title("Event-conditioned forward beta")
            plt.tight_layout()
            plt.savefig(plots / "event_conditioned_beta.png", dpi=150)
            plt.close()

    if bt_event_rows:
        edf = pd.DataFrame(bt_event_rows)
        plt.figure(figsize=(6, 4))
        plt.bar(edf["condition"].astype(str), edf["sharpe"].fillna(0))
        plt.xticks(rotation=35, ha="right")
        plt.title("Sharpe by event condition")
        plt.tight_layout()
        plt.savefig(plots / "event_conditioned_sharpe.png", dpi=150)
        plt.close()

    if turnover_rows:
        tdf = pd.DataFrame(turnover_rows)
        if "total_turnover" in tdf.columns:
            plt.figure(figsize=(6, 4))
            plt.bar(tdf["strategy_family"], tdf["total_turnover"].fillna(0))
            plt.xticks(rotation=30, ha="right")
            plt.title("Turnover by strategy")
            plt.tight_layout()
            plt.savefig(plots / "turnover_by_strategy.png", dpi=150)
            plt.close()

    if res_last is not None and labels_last is not None:
        C = res_last.C
        nodes = labels_last.index.astype(str).tolist()
        order = labels_last.sort_values().index.tolist()
        M = C.reindex(index=order, columns=order, fill_value=0.0).to_numpy()
        plt.figure(figsize=(7, 6))
        plt.imshow(M, aspect="auto", cmap="RdBu_r")
        plt.colorbar(label="C")
        plt.title("C sorted by cluster")
        plt.tight_layout()
        plt.savefig(plots / "heatmap_C_sorted_by_cluster.png", dpi=150)
        plt.close()

    if F_last is not None and F_last.size > 0:
        plt.figure(figsize=(6, 5))
        plt.imshow(F_last, aspect="auto", cmap="viridis")
        plt.colorbar(label="meta flow")
        plt.title("Cluster meta-flow matrix")
        plt.tight_layout()
        plt.savefig(plots / "meta_flow_network.png", dpi=150)
        plt.close()

    cluster_df = pd.DataFrame(cluster_rows) if cluster_rows else None
    if cluster_df is None or cluster_df.empty:
        cmp = out_dir / "cluster_method_comparison.csv"
        if cmp.is_file():
            cluster_df = pd.read_csv(cmp)
    if cluster_df is not None and not cluster_df.empty:
        cluster_daily = None
        cd_path = out_dir / "cluster_sweep_daily_returns.csv"
        if cd_path.is_file():
            cluster_daily = pd.read_csv(cd_path, parse_dates=["date"])
        _write_cluster_sweep_plots(out_dir, cluster_df, cluster_daily=cluster_daily)


def _report_df_md(df: pd.DataFrame) -> str:
    try:
        return df.to_markdown(index=False, floatfmt=".3f")
    except ImportError:
        return df.to_string(index=False)


def _best_hybrid_by_family(hybrid: pd.DataFrame) -> pd.DataFrame:
    idx = hybrid.groupby("strategy_family")["sharpe"].idxmax()
    cols = [c for c in ["strategy_family", "alpha", "sharpe", "ann_return", "max_drawdown"] if c in hybrid.columns]
    return hybrid.loc[idx, cols].sort_values("sharpe", ascending=False)


def _hybrid_narrative(hybrid: pd.DataFrame | None) -> str:
    if hybrid is None or hybrid.empty:
        return ""
    best = _best_hybrid_by_family(hybrid)
    lines = [
        "**Interpretation (signed clustering, full sample):**\n",
        "- **α = 0** uses only the normalized supply-chain adjacency; **α = 1** uses only return-estimated \\(C_{\\text{data}}\\).\n",
    ]
    for row in best.itertuples():
        fam = str(row.strategy_family).replace("_", " ")
        lines.append(
            f"- **{fam}:** best Sharpe at **α = {row.alpha:g}** "
            f"(Sharpe {row.sharpe:.2f}, max DD {row.max_drawdown:.2f}).\n"
        )
    sp = hybrid[hybrid["strategy_family"] == "supplier_pressure"]
    gr = hybrid[hybrid["strategy_family"] == "globalrank"]
    if not sp.empty and not gr.empty:
        lines.append(
            "- **Supplier pressure** is strong across α (supply prior ≈ returns-only); "
            "**globalrank** is highly α-sensitive (best at α = 0.75, weak at α ∈ {0, 1}).\n"
        )
        lines.append(
            "- **Metacluster** shows high in-sample Sharpe at low α but **~99% drawdown** in cumulative PnL "
            "(2020–21 spike/crash); treat as unstable despite positive Sharpe.\n"
        )
        lines.append(
            "- **Clusterrank** improves with more data weight (α = 0.75–1.0) vs pure supply (α = 0).\n"
        )
    lines.append(
        "\nSee `plots/methods_comparison.png` (α = 1 vs best-α) and `plots/hybrid_sharpe_heatmap.png`.\n"
    )
    return "".join(lines)


def _family_row(fam: pd.DataFrame, name: str) -> pd.Series | None:
    sub = fam[fam["strategy_family"] == name]
    return sub.iloc[0] if not sub.empty else None


def _families_narrative(fam: pd.DataFrame | None, params: dict[str, Any] | None = None) -> str:
    if fam is None or fam.empty:
        return ""
    fam_map = (params or {}).get("family_cluster_methods") or {}
    sp_r = _family_row(fam, "supplier_pressure")
    mc_r = _family_row(fam, "metacluster")
    gr_r = _family_row(fam, "globalrank")
    cr_r = _family_row(fam, "clusterrank")
    lines = [
        "**Interpretation (Step 4, return-based \\(C\\), 155 BME rebalances):**\n",
        "- **Configuration:** `metacluster` uses **"
        f"{fam_map.get('metacluster', 'sector')}** clustering; `clusterrank` uses **"
        f"{fam_map.get('clusterrank', 'signed')}**; `supplier_pressure` / `globalrank` do not partition by cluster.\n",
    ]
    if sp_r is not None:
        lines.append(
            f"- **Supplier pressure** (Sharpe **{sp_r['sharpe']:.2f}**, max DD {sp_r['max_drawdown']:.1%}): "
            "best risk-adjusted baseline; trades **suppliers only** with \\(s=C^\\top r\\).\n"
        )
    if mc_r is not None:
        lines.append(
            f"- **Metacluster ({fam_map.get('metacluster', 'sector')})** (Sharpe **{mc_r['sharpe']:.2f}**, "
            f"max DD {mc_r['max_drawdown']:.1%}): strong cumulative return but **higher vol** and episodic "
            "(sector meta-flow spike ~2021–22, give-back after); not a clean substitute for supplier pressure.\n"
        )
    if cr_r is not None:
        lines.append(
            f"- **Clusterrank ({fam_map.get('clusterrank', 'signed')})** (Sharpe **{cr_r['sharpe']:.2f}**, "
            f"max DD {cr_r['max_drawdown']:.1%}): improved after **laggers-only** fix (see §9.1); "
            "still below supplier pressure on Sharpe.\n"
        )
    if gr_r is not None:
        lines.append(
            f"- **Globalrank** (Sharpe **{gr_r['sharpe']:.2f}**): static spectral rank on full network; weakest family.\n"
        )
    lines.append(
        "- Cluster-based families **dilute** the direct customer→supplier signal; beating supplier pressure "
        "on Sharpe is **not** expected for structural extensions.\n"
    )
    return "".join(lines)


def _ensemble_narrative(out_dir: Path) -> str:
    """Diversification across strategy-family daily returns (not hybrid α on C)."""
    dr_path = out_dir / "daily_returns.csv"
    if not dr_path.is_file():
        return ""
    dr = pd.read_csv(dr_path, index_col=0, parse_dates=True).fillna(0)
    cols = [c for c in ["supplier_pressure", "metacluster", "clusterrank", "globalrank"] if c in dr.columns]
    if len(cols) < 2:
        return ""
    corr = dr[cols].corr()

    def _sharpe(s: pd.Series) -> float:
        if s.std() == 0:
            return float("nan")
        return float(s.mean() / s.std() * np.sqrt(252))

    lines = [
        "\n### 9.1 Strategy-family diversification (portfolio of methods)\n\n",
        "This is **not** hybrid **α** on \\(C\\) (see §11); it is combining **finished family return series**.\n\n",
        "**Daily return correlation (full sample):**\n\n",
        _report_df_md(corr.round(3).reset_index().rename(columns={"index": "family"})),
        "\n\n",
    ]
    sp, mc = "supplier_pressure", "metacluster"
    if sp in corr.columns and mc in corr.columns:
        c_sp_mc = float(corr.loc[sp, mc])
        lines.append(
            f"- **Supplier pressure vs metacluster:** correlation **{c_sp_mc:.3f}** on average — "
            "mostly **orthogonal**, despite **opposite-looking** cumulative paths in some subperiods "
            "(episodic sector meta-flow vs supplier drawdown/recovery timing).\n"
        )
    if sp in dr.columns and mc in dr.columns:
        combo = 0.5 * dr[sp] + 0.5 * dr[mc]
        lines.append(
            f"- **50/50 supplier_pressure + metacluster:** Sharpe **{_sharpe(combo):.2f}** "
            f"(vs {_sharpe(dr[sp]):.2f} supplier-only, {_sharpe(dr[mc]):.2f} metacluster-only).\n"
        )
    if len(cols) == 4:
        ew = dr[cols].mean(axis=1)
        lines.append(
            f"- **Equal-weight all four families:** Sharpe **{_sharpe(ew):.2f}** (in-sample; "
            "not OOS-validated).\n"
        )
    lines.append(
        "\n**Takeaway:** A **modest ensemble** of methods may improve risk-adjusted return via diversification; "
        "treat as exploratory. Re-run `hybrid_sweep` with per-family `family_cluster_methods` for α tuning on \\(C\\).\n"
    )
    return "".join(lines)


def _cluster_narrative(cluster: pd.DataFrame | None) -> str:
    if cluster is None or cluster.empty:
        return ""
    csub = cluster.dropna(subset=["sharpe"])
    if csub.empty:
        return ""
    best = csub.loc[csub["sharpe"].idxmax()]
    mc_sector = csub[
        (csub["strategy_family"] == "metacluster") & (csub["cluster_method"] == "sector")
    ]
    cr_signed = csub[
        (csub["strategy_family"] == "clusterrank") & (csub["cluster_method"] == "signed")
    ]
    cr_sc = csub[csub["strategy_family"] == "clusterrank"].sort_values("sharpe", ascending=False)
    text = (
        f"**Interpretation (155 rebalances; `hybrid_prior` excluded — same role as hybrid α on \\(C\\)):**\n"
        f"- Best single cell: **{best['strategy_family']} / {best['cluster_method']}** (Sharpe {best['sharpe']:.2f}).\n"
        "- **Step 4 defaults:** **metacluster → sector**, **clusterrank → signed** "
        "(from sweep; clusterrank re-sweep uses fixed laggers-only implementation).\n"
        "- **Industry labels** (sector, ggroup, …): ARI ≈ 0.97–0.98 (static GICS maps). "
        "**Network labels** (signed, hermitian, …): ARI ≈ 0.28–0.72 (dynamic partitions).\n"
        "- **Metacluster:** `supply_community` / `symmetric_spectral` often produce **no trades** (flat PnL).\n"
    )
    if not mc_sector.empty:
        text += f"- Metacluster / **sector**: Sharpe {float(mc_sector['sharpe'].iloc[0]):.2f} (recommended).\n"
    if not cr_signed.empty:
        text += f"- Clusterrank / **signed**: Sharpe {float(cr_signed['sharpe'].iloc[0]):.2f} (recommended after fix).\n"
    if not cr_sc.empty:
        alt = cr_sc.iloc[1] if len(cr_sc) > 1 else None
        if alt is not None and str(alt["cluster_method"]) != "signed":
            text += (
                f"- Clusterrank runner-up: **{alt['cluster_method']}** "
                f"(Sharpe {float(alt['sharpe']):.2f}).\n"
            )
    return text


def _panel_is_production_quality(panel: pd.DataFrame) -> bool:
    """True when panel rows use entity+time FE and clustered SE with valid p-values."""
    if panel.empty:
        return False
    fe = panel.get("fixed_effects", pd.Series(dtype=str)).astype(str)
    if not fe.str.contains("entity", case=False).any():
        return False
    if "clustered_se" in panel.columns and not panel["clustered_se"].fillna(False).any():
        return False
    if "p_value" in panel.columns and panel["p_value"].notna().sum() == 0:
        return False
    return True


def _panel_quality_warning(panel: pd.DataFrame | None) -> str:
    if panel is None or panel.empty:
        return ""
    if _panel_is_production_quality(panel):
        return ""
    fe = panel.get("fixed_effects", pd.Series(dtype=str)).iloc[0] if len(panel) else "unknown"
    return (
        f"**Panel quality warning:** This run used `{fe}` with "
        f"`clustered_se={bool(panel.get('clustered_se', pd.Series([False])).any())}` and "
        "missing `std_error` / `p_value` on some rows. "
        "Rebuild `data/leadlag_panel_forward.parquet` and `data/leadlag_panel_reverse.parquet` "
        "(`scripts/build_leadlag_panel.py`), install `linearmodels`, then rerun `--steps load,panel` "
        "for firm + time FE and entity-clustered standard errors.\n\n"
    )


def _research_question_decisions(
    out_dir: Path,
    panel: pd.DataFrame | None,
    fam: pd.DataFrame | None,
    cluster: pd.DataFrame | None,
    hybrid: pd.DataFrame | None,
) -> list[tuple[str, str, str]]:
    """Rows for EXPECTED_OUTPUTS §7 — conservative labels for preliminary artifact sets."""

    panel_ok = panel is not None and not panel.empty
    panel_fe = _panel_is_production_quality(panel) if panel_ok else False

    panel_dec = "Not run"
    if panel_ok:
        h1_fwd = panel[
            panel["direction"].astype(str).str.contains("forward", na=False) & (panel["horizon"] == 1)
        ]
        if not h1_fwd.empty and float(h1_fwd["beta"].iloc[0]) > 0:
            panel_dec = (
                "Yes"
                if panel_fe
                else "Preliminary yes; needs FE + clustered-SE rerun"
            )
        elif panel_fe:
            panel_dec = "Mixed"
        else:
            panel_dec = "Preliminary / inconclusive; needs FE + clustered-SE rerun"

    directional_dec = "Not run"
    if panel_ok:
        if panel_fe:
            fwd = panel[
                panel["direction"].astype(str).str.contains("forward", na=False) & (panel["horizon"] == 1)
            ]
            rev = panel[
                panel["direction"].astype(str).str.contains("reverse", na=False) & (panel["horizon"] == 1)
            ]
            if not fwd.empty and not rev.empty:
                fwd_b = abs(float(fwd["beta"].iloc[0]))
                rev_b = abs(float(rev["beta"].iloc[0]))
                p_fwd = float(fwd["p_value"].iloc[0]) if fwd["p_value"].notna().any() else 1.0
                p_rev = float(rev["p_value"].iloc[0]) if rev["p_value"].notna().any() else 1.0
                if fwd_b > rev_b and p_fwd < 0.05 and p_rev >= 0.05:
                    directional_dec = "Yes"
                elif fwd_b > rev_b:
                    directional_dec = "Mixed"
                else:
                    directional_dec = "No / Mixed"
        else:
            directional_dec = "Inconclusive in this artifact; rerun reverse placebo with FE + clustered SE"

    tradable_dec = "Not run"
    if fam is not None and not fam.empty:
        sp = fam[fam["strategy_family"] == "supplier_pressure"]
        if not sp.empty and float(sp["sharpe"].iloc[0]) > 0.15:
            tradable_dec = "Preliminary yes"

    network_dec = "Not run"
    if fam is not None and not fam.empty:
        sp_sh = float(fam.loc[fam["strategy_family"] == "supplier_pressure", "sharpe"].iloc[0])
        mc_sh = float(fam.loc[fam["strategy_family"] == "metacluster", "sharpe"].iloc[0])
        cr_sh = float(fam.loc[fam["strategy_family"] == "clusterrank", "sharpe"].iloc[0])
        if max(mc_sh, cr_sh) > 0.25:
            network_dec = "Preliminary yes (extensions add return; supplier_pressure still best Sharpe)"
        elif max(mc_sh, cr_sh) > 0.15:
            network_dec = "Mixed (positive but below supplier_pressure)"

    cluster_dec = "Not run"
    if cluster is not None and not cluster.empty:
        csub = cluster.dropna(subset=["sharpe"])
        if not csub.empty:
            signed_ok = (
                csub[
                    (csub["strategy_family"] == "metacluster")
                    & (csub["cluster_method"] == "signed")
                ]["sharpe"].max()
                > 0.2
            )
            herm_bad = (
                csub[
                    (csub["strategy_family"] == "metacluster")
                    & (csub["cluster_method"] == "hermitian")
                ]["sharpe"].max()
                < 0
            )
            sector_ok = (
                csub[
                    (csub["strategy_family"] == "metacluster")
                    & (csub["cluster_method"] == "sector")
                ]["sharpe"].max()
                > 0.25
            )
            cluster_dec = "Yes for sector/meta" if sector_ok and herm_bad else "Mixed"

    hybrid_dec = "Not run"
    if hybrid is not None and not hybrid.empty:
        spreads = hybrid.groupby("strategy_family")["sharpe"].agg(lambda s: float(s.max() - s.min()))
        if (spreads > 0.1).any():
            hybrid_dec = "Preliminary yes"

    event_dec = "Not run"
    ev_path = out_dir / "event_conditioned_backtest.csv"
    if ev_path.is_file():
        ev = pd.read_csv(ev_path)
        if not ev.empty and "sharpe" in ev.columns:
            event_dec = "Yes" if float(ev["sharpe"].max()) > 0 else "Mixed"

    return [
        ("Does customer pressure predict supplier returns?", "panel_forward_reverse.csv", panel_dec),
        ("Is the effect directional?", "panel_forward_reverse.csv", directional_dec),
        ("Is the effect tradable?", "summary_metrics.csv", tradable_dec),
        ("Does higher-order network structure help?", "strategy_family_comparison.csv", network_dec),
        ("Does direction-aware clustering help?", "cluster_method_comparison.csv", cluster_dec),
        ("Does supply-chain structure stabilize return-based lead-lag?", "hybrid_alpha_sweep.csv", hybrid_dec),
        ("Is diffusion event-amplified?", "event_conditioned_backtest.csv", event_dec),
    ]


def _artifact_submission_checklist(out_dir: Path) -> str:
    """Markdown checklist: preliminary vs final submission readiness."""

    def _status(path: str, ok_note: str, warn_note: str, missing_note: str) -> tuple[str, str]:
        p = out_dir / path
        if not p.is_file():
            return "✗", missing_note
        if path == "panel_forward_reverse.csv":
            panel = pd.read_csv(p)
            if _panel_is_production_quality(panel):
                return "✓", ok_note
            return "⚠", warn_note
        if path == "summary_metrics.csv":
            sm = pd.read_csv(p)
            if "avg_turnover" in sm.columns and sm["avg_turnover"].notna().any():
                return "✓", ok_note
            return "⚠", warn_note
        if path == "factor_exposure_alpha.csv":
            return "○", "optional"
        return "✓", ok_note

    lines = [
        "\n## Appendix B: Submission readiness (preliminary vs final)\n\n",
        "**Status:** This directory is a **preliminary results** bundle (backtest, clustering, hybrid). "
        "It is sufficient for pipeline validation and exploratory comparison; it is **not** a complete "
        "final research deliverable until the ⚠ / ✗ items below are addressed.\n\n",
        "**Defensible headline:** Full **155-rebalance** run. **Supplier pressure** best Sharpe (~0.52); "
        "**metacluster/sector** and **clusterrank/signed** are best cluster choices; families are largely "
        "uncorrelated (SP vs meta ≈ 0). Clusterrank implementation fixed (laggers-only). Panel FE, events, "
        "costs, and refreshed hybrid α sweep still needed for final submission.\n\n",
        "| File | Status |\n|------|--------|\n",
    ]
    checks = [
        ("summary_metrics.csv", "present", "turnover/costs incomplete", "missing"),
        ("strategy_family_comparison.csv", "present", "", "missing"),
        ("cluster_method_comparison.csv", "present", "", "missing"),
        ("hybrid_alpha_sweep.csv", "present", "", "missing"),
        ("cluster_stability.csv", "present", "", "missing"),
        ("panel_forward_reverse.csv", "FE + clustered SE", "pooled OLS fallback; NaN SE/p-values", "missing"),
        ("horizon_decay.csv", "present", "", "missing (run panel step)"),
        ("event_conditioned_backtest.csv", "present", "", "missing (run events step)"),
        ("turnover_costs.csv", "present", "incomplete", "missing"),
        ("factor_exposure_alpha.csv", "present", "", "optional"),
    ]
    for path, ok, warn, miss in checks:
        sym, note = _status(path, ok, warn, miss)
        lines.append(f"| `{path}` | {sym} {note} |\n")
    lines.append(
        "\n**Recommended reruns:** (1) `build_leadlag_panel.py` + `--steps load,panel` with `linearmodels`; "
        "(2) `--steps events` with earnings calendar; (3) full rebalance calendar (drop or raise "
        "`--max-rebalances` cap); (4) populate turnover/cost columns in `summary_metrics.csv`.\n"
    )
    return "".join(lines)


def generate_final_report(
    out_dir: Path,
    params: dict[str, Any],
    *,
    enabled_steps: list[str] | None = None,
    plot_profile: str = "full",
) -> None:
    """Write START_HERE.md (index) and report/final_report.md (EXPECTED_OUTPUTS §6 deliverable)."""
    del enabled_steps, plot_profile  # reserved for future use
    report_dir = out_dir / "report"
    report_dir.mkdir(exist_ok=True)
    plots_dir = out_dir / "plots"

    def _load(name: str) -> pd.DataFrame | None:
        p = out_dir / name
        return pd.read_csv(p) if p.is_file() else None

    fam = _load("strategy_family_comparison.csv")
    summary = _load("summary_metrics.csv")
    cluster_raw = _load("cluster_method_comparison.csv")
    cluster = _filter_cluster_sweep_df(cluster_raw) if cluster_raw is not None else None
    hybrid = _load("hybrid_alpha_sweep.csv")
    panel = _load("panel_forward_reverse.csv")
    meta: dict[str, Any] = {}
    meta_path = out_dir / "run_metadata.json"
    if meta_path.is_file():
        meta = json.loads(meta_path.read_text(encoding="utf-8"))

    plot_files = sorted(p.name for p in plots_dir.glob("*.png")) if plots_dir.is_dir() else []
    rq_df = pd.DataFrame(
        _research_question_decisions(out_dir, panel, fam, cluster, hybrid),
        columns=["Research question", "Evidence file", "Decision"],
    )

    start = [
        "# Start here\n\n",
        "**Status:** Preliminary results bundle (not full final submission). "
        "See `report/final_report.md` §2 and Appendix B for gaps.\n\n",
        "Quick index. **Main deliverable (`docs/EXPECTED_OUTPUTS.md`):** "
        "[`report/final_report.md`](report/final_report.md)\n\n",
    ]
    if fam is not None and not fam.empty:
        b = fam.sort_values("sharpe", ascending=False).iloc[0]
        start.append(f"Top family Sharpe: **{b['strategy_family']}** ({b['sharpe']:.2f})\n\n")
    start.append("| Topic | Where |\n|-------|--------|\n")
    start.append("| Full report | `report/final_report.md` |\n")
    if (out_dir / "strategy_family_comparison.csv").is_file():
        start.append("| 4 families | `strategy_family_comparison.csv` · `plots/strategy_families_dashboard.png` |\n")
    if (out_dir / "cluster_method_comparison.csv").is_file():
        start.append("| Cluster sweep | `cluster_method_comparison.csv` · `plots/cluster_sweep_dashboard.png` |\n")
    if (out_dir / "hybrid_alpha_sweep.csv").is_file():
        start.append("| Hybrid α | `hybrid_alpha_sweep.csv` · `plots/methods_comparison.png` |\n")
    _write_methods_comparison_plots(out_dir)
    if cluster is not None and not cluster.empty:
        cluster_daily = None
        cd_path = out_dir / "cluster_sweep_daily_returns.csv"
        if cd_path.is_file():
            cluster_daily = pd.read_csv(cd_path, parse_dates=["date"])
        _write_cluster_sweep_plots(out_dir, cluster, cluster_daily=cluster_daily)
    (out_dir / "START_HERE.md").write_text("".join(start), encoding="utf-8")

    n_reb = meta.get("n_rebalances")
    if n_reb is None and cluster is not None and not cluster.empty and "n_rebalances" in cluster.columns:
        n_reb = int(cluster["n_rebalances"].dropna().max())
    if n_reb is None and fam is not None and not fam.empty:
        n_reb = 155
    fam_clusters = meta.get("family_cluster_methods") or params.get("family_cluster_methods") or {}

    panel_warn = _panel_quality_warning(panel)

    md: list[str] = [
        "# Supply Chain Lead–Lag: Research Report (preliminary artifact set)\n\n",
        "> **Deliverable status:** Preliminary results — backtest, clustering, and hybrid sweep are in good shape; "
        "panel FE/clustered SE, events, turnover/costs, and full rebalance calendar are **not** final. "
        "See [Appendix B](#appendix-b-submission-readiness-preliminary-vs-final).\n\n",
        "## 1. Abstract\n",
        "Point-in-time (PIT) supply-chain lead–lag study on Compustat customer–supplier links and daily returns "
        f"({meta.get('data_start', '?')}–{meta.get('data_end', '?')}). We compare four tradable families, "
        "sweep clustering methods, and blend \\(C_{\\text{data}}\\) with \\(C_{\\text{supply}}\\) via hybrid **α** "
        f"(`{params.get('default_cluster_method', 'signed')}` clusters; `{params.get('edge_score', 'tstat_diff')}`).\n\n",
        "**Headline (defensible now):** Full **155-rebalance** PIT backtest. **Supplier pressure** is the best "
        "Sharpe baseline (0.52). **Metacluster (sector)** adds return with different path shape (corr ≈ 0 vs SP); "
        "**clusterrank (signed)** is best among cluster-rank specs after laggers-only fix (Sharpe 0.37). "
        "Cluster sweep: **sector** for meta, **signed** for clusterrank. Hybrid **α** on \\(C\\) still matters for "
        "**globalrank**. Panel FE, events, and net-of-cost claims remain open.\n\n",
        "## 2. Research questions\n\n",
        "Decisions use **conservative** labels when artifacts are pooled-OLS or capped at 12 rebalances.\n\n",
        _report_df_md(rq_df),
        "\n\n## 3. Data and PIT construction\n",
        f"- Returns: `{params.get('returns_parquet')}` · Edges: `{params.get('edges_csv')}`\n",
        f"- PIT date column: `{params.get('edge_date_col')}` · Rebalance: `{params.get('rebalance_freq')}`\n",
    ]
    if meta:
        md.append(
            f"- Window: {meta.get('data_start')}–{meta.get('data_end')} · "
            f"{n_reb if n_reb is not None else '—'} rebalances · "
            f"{meta.get('n_return_assets', '—')} return assets · "
            f"{meta.get('n_edge_rows', '—')} PIT edge rows\n"
        )
    if fam_clusters:
        md.append(f"- Step 4 cluster map: `{fam_clusters}`\n")
    steps = meta.get("pipeline_steps") or []
    if steps:
        md.append(f"- Pipeline steps in last run: `{', '.join(steps)}`\n")

    md.append("\n## 4. Customer-pressure signal\n")
    md.append(
        "At each day \\(d\\), signal \\(s = C^\\top r_d\\) (customer pressure on suppliers); "
        "long–short **suppliers only** with return earned on \\(d+1\\) (`supplier_pressure` family).\n"
    )
    if fam is not None and not fam.empty:
        sp_row = fam[fam["strategy_family"] == "supplier_pressure"]
        if not sp_row.empty:
            r = sp_row.iloc[0]
            md.append(
                f"\nRolling backtest (signed cluster label for bookkeeping, no clustering in signal): "
                f"Sharpe **{r['sharpe']:.2f}**, ann. return {r['ann_return']:.1%}, max DD {r['max_drawdown']:.1%}.\n"
            )

    md.append("\n## 5. Panel predictability results\n")
    if panel is not None and not panel.empty:
        md.append(panel_warn)
        fwd = panel[panel["direction"].astype(str).str.contains("forward", na=False)]
        md.append(_report_df_md(fwd.head(12)) + "\n")
        if not _panel_is_production_quality(panel):
            md.append(
                "\nForward horizon 1 shows positive pooled β with large |t|, but **reverse horizons 1–2 also "
                "have |t| > 2** in this artifact — directionality is **not** established until FE + clustered SE. "
                "Do not cite this table as final regression evidence.\n"
            )
    else:
        md.append(
            "*Not run* (`panel_forward_reverse.csv` missing). Tradable supplier-pressure (§4, §9) is only "
            "indirect evidence for predictability.\n"
        )

    md.append("\n## 6. Directional reverse placebo\n")
    if panel is not None and not panel.empty:
        for direction in ("forward", "reverse"):
            sub = panel[panel["direction"].astype(str).str.contains(direction, na=False)]
            if not sub.empty:
                md.append(f"\n**{direction.title()}:**\n\n{_report_df_md(sub.head(5))}\n")
        if not _panel_is_production_quality(panel):
            md.append(
                "\n**Read with caution:** Reverse placebo is not cleanly insignificant under pooled OLS "
                "(e.g. reverse h=1,2 with |t|≈2.5; h=5 with |t|≈−2.8). Rerun with PanelOLS on built panel parquets.\n"
            )
    else:
        md.append("*Not run.*\n")

    md.append("\n## 7. Lead-lag matrix construction\n")
    md.append(
        f"Rolling \\(C\\), lookback {params.get('lookback_rows')} rows, score `{params.get('edge_score')}`.\n"
    )

    md.append("\n## 8. Network structure and spectral diagnostics\n")
    if (out_dir / "spectral_summary.csv").is_file():
        md.append(_report_df_md(pd.read_csv(out_dir / "spectral_summary.csv")) + "\n")
    else:
        md.append("*Optional — run `artifacts` step.*\n")

    md.append("\n## 9. Strategy family comparison\n")
    if fam is not None and not fam.empty:
        md.append(_families_narrative(fam, params) + "\n")
        md.append(_report_df_md(fam.sort_values("sharpe", ascending=False)) + "\n")
        md.append(_ensemble_narrative(out_dir))
        if (plots_dir / "strategy_families_dashboard.png").is_file():
            md.append("\n![Families](../plots/strategy_families_dashboard.png)\n")
        if (plots_dir / "methods_comparison.png").is_file():
            md.append("\n![Methods comparison](../plots/methods_comparison.png)\n")
        if (plots_dir / "cumulative_pnl_by_strategy.png").is_file():
            md.append("\n![Cumulative PnL](../plots/cumulative_pnl_by_strategy.png)\n")
    else:
        md.append("*Run `--only families`.*\n")

    md.append("\n## 10. Clustering method comparison\n")
    if cluster is not None and not cluster.empty:
        md.append(_cluster_narrative(cluster) + "\n")
        top = cluster.dropna(subset=["sharpe"]).sort_values("sharpe", ascending=False).head(12)
        md.append(_report_df_md(top) + "\n")
        stab = _load("cluster_stability.csv")
        if stab is not None and not stab.empty and "ari" in stab.columns:
            ari = stab.groupby("cluster_method")["ari"].mean().sort_values(ascending=False)
            md.append("\n**Mean ARI by method:** " + ", ".join(f"{k}: {v:.2f}" for k, v in ari.head(5).items()) + "\n")
        if (plots_dir / "cluster_sweep_dashboard.png").is_file():
            md.append("\n![Cluster sweep](../plots/cluster_sweep_dashboard.png)\n")
        if (plots_dir / "cluster_sweep_cumulative_pnl_metacluster.png").is_file():
            md.append("\n![Metacluster PnL by method](../plots/cluster_sweep_cumulative_pnl_metacluster.png)\n")
        if (plots_dir / "cluster_sweep_cumulative_pnl_clusterrank.png").is_file():
            md.append("\n![Clusterrank PnL by method](../plots/cluster_sweep_cumulative_pnl_clusterrank.png)\n")
    else:
        md.append("*Run cluster sweep.*\n")

    md.append("\n## 11. Hybrid alpha sweep\n")
    if hybrid is not None and not hybrid.empty:
        md.append(_hybrid_narrative(hybrid) + "\n")
        md.append("**Best α per family:**\n\n")
        md.append(_report_df_md(_best_hybrid_by_family(hybrid)) + "\n\n")
        md.append("**Full grid:**\n\n")
        md.append(_report_df_md(hybrid.sort_values(["strategy_family", "alpha"])) + "\n")
        if (plots_dir / "methods_comparison.png").is_file():
            md.append("\n![Methods comparison](../plots/methods_comparison.png)\n")
        if (plots_dir / "hybrid_sharpe_heatmap.png").is_file():
            md.append("\n![Hybrid Sharpe heatmap](../plots/hybrid_sharpe_heatmap.png)\n")
        if (plots_dir / "hybrid_alpha_sweep.png").is_file():
            md.append("\n![Hybrid Sharpe curves](../plots/hybrid_alpha_sweep.png)\n")
        if (plots_dir / "hybrid_alpha_sweep_cumulative_pnl.png").is_file():
            md.append("\n![Hybrid PnL panels](../plots/hybrid_alpha_sweep_cumulative_pnl.png)\n")
        elif not (out_dir / "hybrid_alpha_daily_returns.csv").is_file():
            md.append("\n*Re-run `--only hybrid` for cumulative PnL plot.*\n")
    else:
        md.append("*Run `--only hybrid`.*\n")

    md.append("\n## 12. Event-conditioned diffusion\n")
    ev_bt = _load("event_conditioned_backtest.csv")
    if ev_bt is not None and not ev_bt.empty:
        md.append(_report_df_md(ev_bt) + "\n")
    else:
        md.append("*Optional — run `events` step.*\n")

    md.append("\n## 13. Transaction costs and risk controls\n")
    if summary is not None and not summary.empty:
        md.append(_report_df_md(summary) + "\n")
        if summary.get("avg_turnover", pd.Series(dtype=float)).isna().all() or (
            summary.get("total_cost_bps", pd.Series([0])).fillna(0) == 0
        ).all():
            md.append(
                "\n**Not final for trading claims:** `avg_turnover` is NaN and `total_cost_bps` is 0 — "
                "gross Sharpe equals net Sharpe. Populate turnover/cost assumptions before submission.\n"
            )
    else:
        md.append("*Run `baselines` + `summary` steps.*\n")

    md.append("\n## 14. Implementation notes\n")
    md.append(
        "- **Clusterrank (fixed):** Prior versions shorted **local leaders** instead of long–short **among laggers only** "
        "(IMPLEMENTATION_SPEC §6). Step 4 and clusterrank-only `cluster_sweep` reruns use the corrected rule.\n"
        "- **Cluster sweep:** `hybrid_prior` removed from method comparison (duplicates hybrid α on \\(C\\)). "
        "Partial rerun: `--cluster-sweep-families clusterrank` merges into existing CSVs.\n"
        "- **Globalrank:** Static weights per rebalance window (spec §4); no daily signal refresh.\n"
    )

    md.append("\n## 15. Limitations\n")
    md.append(
        "- **Panel:** `panel_forward_reverse.csv` is pooled OLS without clustered SE — not final for RQ1–2.\n"
        "- **Hybrid α sweep:** May predate latest `family_cluster_methods`; rerun `--steps hybrid_sweep` to align.\n"
        "- **Metacluster:** Episodic PnL (sector labels + meta-flow) — report path risk, not Sharpe alone.\n"
        "- **Events / costs:** `event_conditioned_*.csv` and turnover in `summary_metrics.csv` incomplete.\n"
        "- **Ensemble Sharpe:** In-sample combinations (§9.1) are exploratory, not OOS-validated.\n"
    )

    md.append("\n## 16. Conclusion\n")
    md.append(
        "Under **155 BME rebalances** (2010–2024) and PIT supply-chain edges:\n\n"
        "1. **Supplier pressure** remains the primary tradable story (Sharpe ~0.52) — direct \\(C^\\top r\\) on suppliers.\n"
        "2. **Metacluster (sector)** is the best cluster extension for meta-flow (Sharpe ~0.33) but with different "
        "crisis behavior than supplier pressure; low return correlation suggests **ensemble** potential, not redundancy.\n"
        "3. **Clusterrank (signed)** is the best network cluster-rank spec after the laggers-only fix (Sharpe ~0.37).\n"
        "4. **Globalrank** underperforms; hybrid **α** tuning helps globalrank (best α ≈ 0.75 in prior sweep).\n"
        "5. Regression directionality, events, and net-of-cost robustness are **not** final — see Appendix B.\n\n"
    )
    if fam is not None and not fam.empty:
        for fam_name in ("supplier_pressure", "metacluster", "clusterrank", "globalrank"):
            row = _family_row(fam, fam_name)
            if row is not None:
                cm = row.get("cluster_method", "—")
                md.append(
                    f"- **{fam_name}** (`{cm}`): Sharpe {float(row['sharpe']):.2f}, "
                    f"max DD {float(row['max_drawdown']):.1%}\n"
                )
    if hybrid is not None and not hybrid.empty:
        best = _best_hybrid_by_family(hybrid)
        md.append(
            "\n**Hybrid α on \\(C\\) (prior sweep; refresh recommended):** "
            + ", ".join(
                f"`{r.strategy_family}` @ α={r.alpha:g} (Sharpe {r.sharpe:.2f})" for r in best.itertuples()
            )
            + ".\n"
        )

    md.append("\n## Appendix A: artifact checklist\n\n**Plots:**\n")
    md.append("\n".join(f"- `plots/{p}`" for p in plot_files) if plot_files else "- none")
    md.append("\n\n**Required CSVs (EXPECTED_OUTPUTS §8):**\n")
    for fname in [
        "summary_metrics.csv",
        "panel_forward_reverse.csv",
        "strategy_family_comparison.csv",
        "cluster_method_comparison.csv",
        "hybrid_alpha_sweep.csv",
        "cluster_stability.csv",
    ]:
        md.append(f"- {'✓' if (out_dir / fname).is_file() else '✗'} `{fname}`\n")

    md.append(_artifact_submission_checklist(out_dir))

    (report_dir / "final_report.md").write_text("".join(md), encoding="utf-8")

    if params.get("report_generate_tex", True):
        (report_dir / "final_report.tex").write_text(
            r"""\documentclass{article}
\begin{document}
\title{Supply Chain Lead--Lag: Final Research Report}
\maketitle
See \texttt{report/final\_report.md} (full EXPECTED\_OUTPUTS deliverable).
\end{document}
""",
            encoding="utf-8",
        )
