#!/usr/bin/env python3
"""Rolling long–short backtest on real returns + merged edges (see supply_chain_leadlag.backtest)."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
import pandas as pd

# Allow `python scripts/backtest_leadlag.py` without `pip install -e .`
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from supply_chain_leadlag.backtest import comparison_metrics_table, run_rolling_comparison
from supply_chain_leadlag.matrix import load_edges, load_returns_wide_by_gvkey
from supply_chain_leadlag.signals import portfolio_metrics


def main():
    ap = argparse.ArgumentParser(description="Rolling lead–lag long–short backtest + baselines.")
    ap.add_argument("--returns_parquet", default="data/returns_with_gvkey.parquet")
    ap.add_argument("--edges_csv", default="data/merged_edges.csv")
    ap.add_argument("--score", default="tstat_diff")
    ap.add_argument("--rank_method", choices=["leadingness", "spectral"], default="leadingness")
    ap.add_argument("--lookback_rows", type=int, default=504)
    ap.add_argument("--rebalance_freq", default="BME", help="Pandas offset (BME = business month-end).")
    ap.add_argument("--q", type=float, default=0.2, help="Top/bottom fraction for long–short legs.")
    ap.add_argument("--min_obs", type=int, default=80)
    ap.add_argument("--horizon", type=int, default=1)
    ap.add_argument("--max_lag", type=int, default=5)
    ap.add_argument("--momentum_window", type=int, default=20, help="Days for momentum baseline sum.")
    ap.add_argument("--baseline_seed", type=int, default=0, help="RNG seed for random-rank baseline.")
    ap.add_argument(
        "--no_compare",
        action="store_true",
        help="Only run main strategy (skip baseline portfolios).",
    )
    ap.add_argument("--out_csv", default="results/backtest/daily_strategy.csv")
    ap.add_argument("--out_summary", default="results/backtest/summary.json")
    ap.add_argument(
        "--out_comparison",
        default="results/backtest/summary_comparison.csv",
        help="Written when baselines are enabled.",
    )
    ap.add_argument(
        "--max_rebalances",
        type=int,
        default=None,
        help="Cap number of monthly rebalances (faster; full sample can be very slow on large edge lists).",
    )
    args = ap.parse_args()

    R = load_returns_wide_by_gvkey(args.returns_parquet)
    edges = load_edges(args.edges_csv)

    print(f"[returns] {R.shape}  [edges] {len(edges):,} rows")

    comp = run_rolling_comparison(
        R,
        edges,
        lookback_rows=args.lookback_rows,
        rebalance_freq=args.rebalance_freq,
        score=args.score,  # type: ignore[arg-type]
        rank_method=args.rank_method,
        q=args.q,
        min_obs=args.min_obs,
        horizon=args.horizon,
        max_lag=args.max_lag,
        max_rebalances=args.max_rebalances,
        momentum_window=args.momentum_window,
        baseline_seed=args.baseline_seed,
        include_baselines=not args.no_compare,
    )

    out_csv = Path(args.out_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    if args.no_compare:
        res = comp.main
        out = res.daily_ret.to_frame(name="main")
        out["cumulative"] = res.cumulative
        out.to_csv(out_csv)
        if not res.rebalance_log.empty:
            res.rebalance_log.to_csv(out_csv.parent / "rebalances.csv", index=False)
        summ = portfolio_metrics(res.daily_ret)
        summ["n_rebalances"] = int(len(res.rebalance_log))
        out_summary = Path(args.out_summary)
        out_summary.parent.mkdir(parents=True, exist_ok=True)

        def _json_val(v):
            if hasattr(v, "item"):
                return v.item()
            if isinstance(v, (float, int, str)) or v is None:
                return v
            return str(v)

        with open(out_summary, "w") as f:
            json.dump({k: _json_val(v) for k, v in summ.items()}, f, indent=2)
        print(json.dumps(summ, indent=2))
        print(f"[saved] {out_csv}  {out_summary}")
        return

    daily = pd.DataFrame(
        {
            "main": comp.main.daily_ret,
            "random": comp.random.daily_ret,
            "momentum": comp.momentum.daily_ret,
            "structural": comp.structural.daily_ret,
            "equal_weight": comp.equal_weight.daily_ret,
        }
    )
    daily.to_csv(out_csv)
    if not comp.main.rebalance_log.empty:
        comp.main.rebalance_log.to_csv(out_csv.parent / "rebalances.csv", index=False)

    table = comparison_metrics_table(comp)
    out_cmp = Path(args.out_comparison)
    out_cmp.parent.mkdir(parents=True, exist_ok=True)
    table.to_csv(out_cmp, index=False)

    summ_main = portfolio_metrics(comp.main.daily_ret)
    summ_main["n_rebalances"] = int(len(comp.main.rebalance_log))

    def _json_val(v):
        if hasattr(v, "item"):
            return v.item()
        if isinstance(v, (float, int, str)) or v is None:
            return v
        return str(v)

    out_summary = Path(args.out_summary)
    out_summary.parent.mkdir(parents=True, exist_ok=True)
    with open(out_summary, "w") as f:
        json.dump({k: _json_val(v) for k, v in summ_main.items()}, f, indent=2)

    print(table.to_string(index=False))
    print(f"[saved] {out_csv}  {out_cmp}  {out_summary}")


if __name__ == "__main__":
    main()
