#!/usr/bin/env python3
"""
Grid search over score × rank_method, optionally × n_clusters × max_rebalances.
Shared defaults with `config/backtest.yaml`.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from supply_chain_leadlag.backtest import DEFAULT_SCORE_GRID, grid_search_main_backtest
from supply_chain_leadlag.matrix import load_edges, load_returns_wide_by_gvkey
from supply_chain_leadlag.yaml_config import (
    default_config_path,
    flat_backtest_run_params,
    grid_search_bundle,
    load_yaml_config,
    merge_backtest_cli,
)


def main():
    ap = argparse.ArgumentParser(
        description="Grid: edge score × rank_method. CLI overrides config/backtest.yaml.",
    )
    ap.add_argument("--config", default=None)
    ap.add_argument("--returns_parquet", default=None)
    ap.add_argument("--edges_csv", default=None)
    ap.add_argument("--scores", default=None, help="Comma-separated; default from YAML or all six.")
    ap.add_argument("--rank_methods", default=None, help="Comma-separated; default from YAML.")
    ap.add_argument("--lookback_rows", type=int, default=None)
    ap.add_argument("--rebalance_freq", default=None)
    ap.add_argument("--q", type=float, default=None)
    ap.add_argument("--min_obs", type=int, default=None)
    ap.add_argument("--horizon", type=int, default=None)
    ap.add_argument("--max_lag", type=int, default=None)
    ap.add_argument("--n_clusters", type=int, default=None)
    ap.add_argument("--cluster_random_state", type=int, default=None)
    ap.add_argument("--max_rebalances", type=int, default=None)
    ap.add_argument(
        "--n_clusters_grid",
        default=None,
        help="Comma-separated ints to sweep (overrides grid YAML), e.g. 4,6,8.",
    )
    ap.add_argument(
        "--max_rebalances_grid",
        default=None,
        help="Comma-separated caps per cell; use 'null' for no cap, e.g. 8,12,null.",
    )
    ap.add_argument(
        "--hybrid_alpha",
        type=float,
        default=None,
        help="If set, main leg uses α·C_data+(1−α)·C_supply before ranking (same for all grid cells).",
    )
    ap.add_argument("--out_csv", default=None)
    ap.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable tqdm progress bar.",
    )
    args = ap.parse_args()

    cfg_path = args.config or default_config_path(_ROOT)
    cfg = load_yaml_config(cfg_path) if cfg_path else {}
    run = flat_backtest_run_params(cfg)
    run_m = merge_backtest_cli(run, args)
    bundle = grid_search_bundle(cfg)
    bundle["returns_parquet"] = run_m["returns_parquet"]
    bundle["edges_csv"] = run_m["edges_csv"]

    for k in (
        "lookback_rows",
        "rebalance_freq",
        "q",
        "min_obs",
        "horizon",
        "max_lag",
        "max_rebalances",
        "n_clusters",
        "cluster_random_state",
    ):
        bundle[k] = run_m[k]

    if args.scores:
        bundle["scores"] = [x.strip() for x in args.scores.split(",") if x.strip()]
    if args.rank_methods:
        bundle["rank_methods"] = [x.strip() for x in args.rank_methods.split(",") if x.strip()]
    if args.out_csv:
        bundle["out_csv"] = args.out_csv
    if args.n_clusters_grid:
        bundle["n_clusters_grid"] = [int(x.strip()) for x in args.n_clusters_grid.split(",") if x.strip()]
    if args.max_rebalances_grid:
        mrb: list[int | None] = []
        for x in args.max_rebalances_grid.split(","):
            s = x.strip().lower()
            if s in ("", "null", "none", "full", "all"):
                mrb.append(None)
            else:
                mrb.append(int(s))
        bundle["max_rebalances_grid"] = mrb

    R = load_returns_wide_by_gvkey(bundle["returns_parquet"])
    edges = load_edges(bundle["edges_csv"])

    scores = bundle["scores"]
    rank_methods = bundle["rank_methods"] or ["leadingness", "spectral"]

    print(f"[returns] {R.shape}  [edges] {len(edges):,} rows")
    if cfg_path:
        print(f"[config] {cfg_path}")
    n_s = len(scores) if scores is not None else len(DEFAULT_SCORE_GRID)
    nc_g = bundle.get("n_clusters_grid")
    mr_g = bundle.get("max_rebalances_grid")
    nc_n = len(nc_g) if nc_g else 1
    mr_n = len(mr_g) if mr_g else 1
    n_cells = n_s * len(rank_methods) * nc_n * mr_n
    print(
        f"[grid] {n_s} scores × {len(rank_methods)} rank_methods × "
        f"{nc_n} n_clusters × {mr_n} max_rebalances ({n_cells} cells, main leg only)"
    )

    df = grid_search_main_backtest(
        R,
        edges,
        scores=scores,
        rank_methods=rank_methods,
        n_clusters_grid=bundle.get("n_clusters_grid"),
        max_rebalances_grid=bundle.get("max_rebalances_grid"),
        show_progress=not args.no_progress,
        lookback_rows=int(bundle["lookback_rows"]),
        rebalance_freq=str(bundle["rebalance_freq"]),
        q=float(bundle["q"]),
        min_obs=int(bundle["min_obs"]),
        horizon=int(bundle["horizon"]),
        max_lag=int(bundle["max_lag"]),
        max_rebalances=bundle["max_rebalances"],
        n_clusters=int(bundle["n_clusters"]),
        cluster_random_state=int(bundle["cluster_random_state"]),
        hybrid_alpha=bundle.get("hybrid_alpha"),
    )

    out = Path(bundle["out_csv"])
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)
    print(df.to_string(index=False))
    print(f"[saved] {out}")


if __name__ == "__main__":
    main()
