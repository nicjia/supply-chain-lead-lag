#!/usr/bin/env python3
"""
Grid search over (score × rank_method) for the main rolling long–short strategy.

Example:
  python scripts/grid_backtest.py --max_rebalances 6 --out_csv results/backtest/grid_main.csv
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


def main():
    ap = argparse.ArgumentParser(
        description="Grid: edge score for C × global rank method (main leg Sharpe).",
    )
    ap.add_argument("--returns_parquet", default="data/returns_with_gvkey.parquet")
    ap.add_argument("--edges_csv", default="data/merged_edges.csv")
    ap.add_argument(
        "--scores",
        default=None,
        help="Comma-separated: tstat_diff,beta_diff,cross_corr,... Default: all six.",
    )
    ap.add_argument(
        "--rank_methods",
        default="leadingness,spectral",
        help="Comma-separated: leadingness, spectral.",
    )
    ap.add_argument("--lookback_rows", type=int, default=504)
    ap.add_argument("--rebalance_freq", default="BME")
    ap.add_argument("--q", type=float, default=0.2)
    ap.add_argument("--min_obs", type=int, default=80)
    ap.add_argument("--horizon", type=int, default=1)
    ap.add_argument("--max_lag", type=int, default=5)
    ap.add_argument("--max_rebalances", type=int, default=None)
    ap.add_argument("--out_csv", default="results/backtest/grid_main.csv")
    args = ap.parse_args()

    scores = args.scores.split(",") if args.scores else None
    rank_methods = [x.strip() for x in args.rank_methods.split(",") if x.strip()]

    R = load_returns_wide_by_gvkey(args.returns_parquet)
    edges = load_edges(args.edges_csv)
    print(f"[returns] {R.shape}  [edges] {len(edges):,} rows")
    n_s = len(scores) if scores is not None else len(DEFAULT_SCORE_GRID)
    print(f"[grid] {n_s} scores × {len(rank_methods)} rank_methods (main leg only)")

    df = grid_search_main_backtest(
        R,
        edges,
        scores=scores,
        rank_methods=rank_methods,  # type: ignore[arg-type]
        lookback_rows=args.lookback_rows,
        rebalance_freq=args.rebalance_freq,
        q=args.q,
        min_obs=args.min_obs,
        horizon=args.horizon,
        max_lag=args.max_lag,
        max_rebalances=args.max_rebalances,
    )

    out = Path(args.out_csv)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)
    print(df.to_string(index=False))
    print(f"[saved] {out}")


if __name__ == "__main__":
    main()
