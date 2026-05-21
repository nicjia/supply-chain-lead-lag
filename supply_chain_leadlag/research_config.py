"""Flatten ``config/research.yaml`` with safe defaults."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from supply_chain_leadlag.yaml_config import load_yaml_config


def default_research_config_path(repo_root: Path | None = None) -> Path | None:
    root = repo_root or Path(__file__).resolve().parents[1]
    p = root / "config" / "research.yaml"
    return p if p.is_file() else None


def flat_research_params(cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    """Resolved research pipeline parameters with defaults for omitted keys."""
    cfg = cfg or {}
    paths = cfg.get("paths", {})
    pit = cfg.get("pit", {})
    matrix = cfg.get("matrix", {})
    cl = cfg.get("clustering", {})
    strat = cfg.get("strategies", {})
    base = cfg.get("baselines", {})
    costs = cfg.get("costs", {})
    risk = cfg.get("risk", {})
    events = cfg.get("events", {})
    report = cfg.get("report", {})
    panel = cfg.get("panel", {})

    edge_expiry = pit.get("edge_expiry_days")
    max_reb = strat.get("max_rebalances")

    hybrid_grid = matrix.get("hybrid_alpha_grid", [0.0, 0.25, 0.5, 0.75, 1.0])
    if not isinstance(hybrid_grid, list):
        hybrid_grid = [0.0, 0.25, 0.5, 0.75, 1.0]

    cl_methods = cl.get("methods", [
        "sector",
        "supply_community",
        "symmetric_spectral",
        "hermitian",
        "signed",
        "hybrid_prior",
    ])
    if isinstance(cl_methods, str):
        cl_methods = [x.strip() for x in cl_methods.split(",") if x.strip()]

    families = strat.get("families", [
        "supplier_pressure",
        "globalrank",
        "metacluster",
        "clusterrank",
    ])
    if isinstance(families, str):
        families = [x.strip() for x in families.split(",") if x.strip()]

    return {
        "edges_csv": paths.get("edges_csv", "data/merged_edges.csv"),
        "returns_parquet": paths.get("returns_parquet", "data/returns_with_gvkey.parquet"),
        "earnings_calendar_csv": paths.get("earnings_calendar_csv", "data/output_earnings_calendar.csv"),
        "sector_map_csv": paths.get("sector_map_csv", "data/sector_map.csv"),
        "output_dir": paths.get("output_dir", "results/final_research"),
        "edge_date_col": pit.get("edge_date_col", "filing_date"),
        "edge_expiry_days": int(edge_expiry) if edge_expiry is not None else 550,
        "prefer_filing_date": bool(pit.get("prefer_filing_date", True)),
        "lookback_rows": int(matrix.get("lookback_rows", 504)),
        "horizon": int(matrix.get("horizon", 1)),
        "min_obs": int(matrix.get("min_obs", 80)),
        "max_lag": int(matrix.get("max_lag", 5)),
        "winsor_q": matrix.get("winsor_q", 0.001),
        "edge_score": matrix.get("edge_score", "tstat_diff"),
        "edge_scores": matrix.get("edge_scores", ["tstat_diff"]),
        "hybrid_alpha_grid": [float(x) for x in hybrid_grid],
        "n_clusters": int(cl.get("n_clusters", 10)),
        "cluster_random_state": int(cl.get("random_state", 42)),
        "cluster_methods": list(cl_methods),
        "hybrid_prior_alpha": float(cl.get("hybrid_prior_alpha", 0.5)),
        "strategy_families": list(families),
        "q": float(strat.get("q", 0.2)),
        "rebalance_freq": str(strat.get("rebalance_freq", "BME")),
        "max_rebalances": int(max_reb) if max_reb is not None else None,
        "apply_next_day": bool(strat.get("apply_next_day", True)),
        "globalrank_method": strat.get("globalrank_method", "spectral"),
        "baselines_include": bool(base.get("include", True)),
        "baseline_methods": base.get("methods", ["random", "momentum", "structural", "equal_weight"]),
        "momentum_window": int(base.get("momentum_window", 20)),
        "commission_bps": float(costs.get("commission_bps", 0.0)),
        "slippage_bps": float(costs.get("slippage_bps", 0.0)),
        "borrow_bps_annual": float(costs.get("borrow_bps_annual", 0.0)),
        "max_abs_weight": (
            float(risk["max_abs_weight"]) if risk.get("max_abs_weight") is not None else None
        ),
        "beta_neutralize": bool(risk.get("beta_neutralize", False)),
        "market_gvkey": risk.get("market_gvkey"),
        "beta_lookback_rows": int(risk.get("beta_lookback_rows", 252)),
        "sector_neutralize": bool(risk.get("sector_neutralize", False)),
        "events_enabled": bool(events.get("enabled", True)),
        "earnings_window_days": events.get("earnings_window_days", [-1, 0, 1]),
        "large_customer_return_quantile": float(events.get("large_customer_return_quantile", 0.95)),
        "high_vol_quantile": float(events.get("high_vol_quantile", 0.80)),
        "report_generate_md": bool(report.get("generate_md", True)),
        "report_generate_tex": bool(report.get("generate_tex", True)),
        "panel_horizon_max": int(panel.get("horizon_max", 5)),
        "panel_build_if_missing": bool(panel.get("build_if_missing", True)),
    }


def load_research_config(path: str | Path | None = None) -> dict[str, Any]:
    if path is None:
        dp = default_research_config_path()
        path = dp if dp is not None else None
    raw = load_yaml_config(path)
    return flat_research_params(raw)
