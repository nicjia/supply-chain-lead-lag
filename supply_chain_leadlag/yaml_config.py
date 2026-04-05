"""Load `config/backtest.yaml` and merge with CLI overrides."""

from __future__ import annotations

from pathlib import Path
from typing import Any

try:
    import yaml
except ModuleNotFoundError as e:  # pragma: no cover
    raise ModuleNotFoundError(
        "PyYAML is required for config/backtest.yaml. Install the PyPI package: pip install pyyaml "
        "(import name is `yaml`, not the nonexistent `yaml` package on PyPI)."
    ) from e


def load_yaml_config(path: str | Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    path = Path(path)
    if not path.is_file():
        return {}
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def default_config_path(repo_root: Path | None = None) -> Path | None:
    """`config/backtest.yaml` under repo root, if present."""
    root = repo_root or Path(__file__).resolve().parents[1]
    p = root / "config" / "backtest.yaml"
    return p if p.is_file() else None


def flat_backtest_run_params(cfg: dict[str, Any]) -> dict[str, Any]:
    """Flatten YAML sections to kwargs for `run_rolling_comparison` + paths + outputs."""
    paths = cfg.get("paths", {})
    matrix = cfg.get("matrix", {})
    ranking = cfg.get("ranking", {})
    bt = cfg.get("backtest", {})
    out = cfg.get("outputs", {})
    return {
        "returns_parquet": paths.get("returns_parquet", "data/returns_with_gvkey.parquet"),
        "edges_csv": paths.get("edges_csv", "data/merged_edges.csv"),
        "score": matrix.get("score", "tstat_diff"),
        "horizon": int(matrix.get("horizon", 1)),
        "max_lag": int(matrix.get("max_lag", 5)),
        "min_obs": int(matrix.get("min_obs", 80)),
        "rank_method": ranking.get("rank_method", "leadingness"),
        "n_clusters": int(ranking.get("n_clusters", 4)),
        "cluster_random_state": int(ranking.get("cluster_random_state", 0)),
        "lookback_rows": int(bt.get("lookback_rows", 504)),
        "rebalance_freq": str(bt.get("rebalance_freq", "BME")),
        "q": float(bt.get("q", 0.2)),
        "max_rebalances": bt.get("max_rebalances"),
        "momentum_window": int(bt.get("momentum_window", 20)),
        "baseline_seed": int(bt.get("baseline_seed", 0)),
        "compare_baselines": bool(bt.get("compare_baselines", True)),
        "out_csv": out.get("daily_csv", "results/backtest/daily_strategy.csv"),
        "out_summary": out.get("summary_json", "results/backtest/summary.json"),
        "out_comparison": out.get("comparison_csv", "results/backtest/summary_comparison.csv"),
    }


def flat_grid_params(cfg: dict[str, Any]) -> dict[str, Any]:
    g = cfg.get("grid", {})
    scores = g.get("scores")
    if scores is not None and not isinstance(scores, list):
        scores = None
    rm = g.get("rank_methods")
    if rm is not None and not isinstance(rm, list):
        rm = None
    return {
        "scores": scores,
        "rank_methods": rm,
        "out_csv": g.get("out_csv", "results/backtest/grid_main.csv"),
    }


def merge_backtest_cli(flat: dict[str, Any], args: Any) -> dict[str, Any]:
    """CLI attributes override `flat` when the attribute is not None (or True for flags)."""
    m = dict(flat)
    for key in (
        "returns_parquet",
        "edges_csv",
        "score",
        "horizon",
        "max_lag",
        "min_obs",
        "rank_method",
        "n_clusters",
        "cluster_random_state",
        "lookback_rows",
        "rebalance_freq",
        "q",
        "max_rebalances",
        "momentum_window",
        "baseline_seed",
        "out_csv",
        "out_summary",
        "out_comparison",
    ):
        v = getattr(args, key, None)
        if v is not None:
            m[key] = v
    if getattr(args, "no_compare", False):
        m["compare_baselines"] = False
    return m


def merge_grid_cli(flat_grid: dict[str, Any], args: Any) -> dict[str, Any]:
    m = dict(flat_grid)
    if getattr(args, "scores", None):
        m["scores"] = [x.strip() for x in args.scores.split(",") if x.strip()]
    if getattr(args, "rank_methods", None):
        m["rank_methods"] = [x.strip() for x in args.rank_methods.split(",") if x.strip()]
    if getattr(args, "out_csv", None):
        m["out_csv"] = args.out_csv
    return m


def grid_search_bundle(cfg: dict[str, Any]) -> dict[str, Any]:
    """Single dict for `grid_search_main_backtest` from merged YAML."""
    run = flat_backtest_run_params(cfg)
    g = cfg.get("grid", {})
    scores = g.get("scores")
    if isinstance(scores, str):
        scores = [s.strip() for s in scores.split(",") if s.strip()]
    rank_methods = g.get("rank_methods")
    if isinstance(rank_methods, str):
        rank_methods = [x.strip() for x in rank_methods.split(",") if x.strip()]
    out_csv = g.get("out_csv", "results/backtest/grid_main.csv")
    return {
        "returns_parquet": run["returns_parquet"],
        "edges_csv": run["edges_csv"],
        "scores": scores,
        "rank_methods": rank_methods,
        "out_csv": out_csv,
        "lookback_rows": run["lookback_rows"],
        "rebalance_freq": run["rebalance_freq"],
        "q": run["q"],
        "min_obs": run["min_obs"],
        "horizon": run["horizon"],
        "max_lag": run["max_lag"],
        "max_rebalances": run["max_rebalances"],
        "n_clusters": run["n_clusters"],
        "cluster_random_state": run["cluster_random_state"],
    }
