#!/usr/bin/env python3
"""Rolling long–short backtest. Defaults: `config/backtest.yaml` (override with CLI flags)."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from supply_chain_leadlag.backtest import comparison_metrics_table, run_rolling_comparison
from supply_chain_leadlag.matrix import load_edges, load_returns_wide_by_gvkey
from supply_chain_leadlag.signals import portfolio_metrics
from supply_chain_leadlag.yaml_config import (
    default_config_path,
    flat_backtest_run_params,
    load_yaml_config,
    merge_backtest_cli,
)


def main():
    ap = argparse.ArgumentParser(
        description="Rolling lead–lag long–short backtest + baselines. "
        "See config/backtest.yaml for defaults; CLI overrides YAML.",
    )
    ap.add_argument(
        "--config",
        default=None,
        help="YAML file. If omitted, uses config/backtest.yaml when that file exists.",
    )
    ap.add_argument("--returns_parquet", default=None)
    ap.add_argument("--edges_csv", default=None)
    ap.add_argument(
        "--signal_method",
        default=None,
        choices=["supplier_pressure", "rank_factor"],
    )
    ap.add_argument(
        "--edge_date_col",
        default=None,
        choices=["filing_date", "srcdate"],
    )
    ap.add_argument("--edge_expiry_days", type=int, default=None)
    ap.add_argument(
        "--score",
        default=None,
        choices=["tstat_diff", "beta_diff", "cross_corr", "regression_r2", "granger", "levy"],
    )
    ap.add_argument(
        "--rank_method",
        default=None,
        choices=["leadingness", "spectral", "cluster", "cluster_eigen"],
    )
    ap.add_argument("--n_clusters", type=int, default=None)
    ap.add_argument("--cluster_random_state", type=int, default=None)
    ap.add_argument("--lookback_rows", type=int, default=None)
    ap.add_argument("--rebalance_freq", default=None)
    ap.add_argument("--q", type=float, default=None)
    ap.add_argument("--min_obs", type=int, default=None)
    ap.add_argument(
        "--hybrid_alpha",
        type=float,
        default=None,
        help="Blend main leg C with supply-only C: α·C_data+(1−α)·C_supply; omit for data-only.",
    )
    ap.add_argument("--horizon", type=int, default=None)
    ap.add_argument("--max_lag", type=int, default=None)
    ap.add_argument("--momentum_window", type=int, default=None)
    ap.add_argument("--baseline_seed", type=int, default=None)
    ap.add_argument("--commission_bps", type=float, default=None)
    ap.add_argument("--slippage_bps", type=float, default=None)
    ap.add_argument("--borrow_bps_annual", type=float, default=None)
    ap.add_argument("--max_abs_weight", type=float, default=None)
    ap.add_argument("--beta_neutralize", action="store_true")
    ap.add_argument("--beta_lookback_rows", type=int, default=None)
    ap.add_argument("--market_gvkey", default=None)
    ap.add_argument("--sector_neutralize", action="store_true")
    ap.add_argument("--sector_map_csv", default=None)
    ap.add_argument("--no_compare", action="store_true")
    ap.add_argument("--out_csv", default=None)
    ap.add_argument("--out_summary", default=None)
    ap.add_argument("--out_comparison", default=None)
    ap.add_argument("--max_rebalances", type=int, default=None)
    ap.add_argument(
        "--progress",
        action="store_true",
        help="Show tqdm progress bar over rebalance dates.",
    )
    args = ap.parse_args()

    cfg_path = args.config or default_config_path(_ROOT)
    cfg = load_yaml_config(cfg_path) if cfg_path else {}
    flat = flat_backtest_run_params(cfg)
    p = merge_backtest_cli(flat, args)

    R = load_returns_wide_by_gvkey(p["returns_parquet"])
    edges = load_edges(p["edges_csv"], date_col=str(p.get("edge_date_col", "srcdate")))
    market_ret = None
    if p.get("market_gvkey"):
        mk = str(p["market_gvkey"])
        if mk in R.columns:
            market_ret = R[mk]
        else:
            raise ValueError(f"--market_gvkey {mk!r} not found in returns columns")

    sector_map = None
    if p.get("sector_map_csv"):
        sm = pd.read_csv(str(p["sector_map_csv"]))
        if not {"gvkey", "sector"}.issubset(sm.columns):
            raise ValueError("sector_map_csv must include columns: gvkey, sector")
        sector_map = (
            sm.assign(gvkey=lambda d: d["gvkey"].astype(str).str.zfill(6))
            .dropna(subset=["sector"])
            .drop_duplicates("gvkey")
            .set_index("gvkey")["sector"]
        )

    print(f"[returns] {R.shape}  [edges] {len(edges):,} rows")
    if cfg_path:
        print(f"[config] {cfg_path}")
    print(
        f"[config] signal_method={p['signal_method']!r} score={p['score']!r} "
        f"rank_method={p['rank_method']!r} hybrid_alpha={p.get('hybrid_alpha')!r}"
    )

    comp = run_rolling_comparison(
        R,
        edges,
        lookback_rows=int(p["lookback_rows"]),
        rebalance_freq=str(p["rebalance_freq"]),
        score=p["score"],  # type: ignore[arg-type]
        signal_method=p["signal_method"],  # type: ignore[arg-type]
        rank_method=p["rank_method"],  # type: ignore[arg-type]
        n_clusters=int(p["n_clusters"]),
        cluster_random_state=int(p["cluster_random_state"]),
        q=float(p["q"]),
        min_obs=int(p["min_obs"]),
        horizon=int(p["horizon"]),
        max_lag=int(p["max_lag"]),
        max_rebalances=p["max_rebalances"],
        hybrid_alpha=p.get("hybrid_alpha"),
        momentum_window=int(p["momentum_window"]),
        baseline_seed=int(p["baseline_seed"]),
        include_baselines=bool(p.get("compare_baselines", True)),
        commission_bps=float(p.get("commission_bps", 0.0)),
        slippage_bps=float(p.get("slippage_bps", 0.0)),
        borrow_bps_annual=float(p.get("borrow_bps_annual", 0.0)),
        max_abs_weight=p.get("max_abs_weight"),
        beta_neutralize=bool(p.get("beta_neutralize", False)),
        beta_lookback_rows=int(p.get("beta_lookback_rows", 252)),
        market_ret=market_ret,
        sector_neutralize=bool(p.get("sector_neutralize", False)),
        sector_map=sector_map,
        edge_expiry_days=p.get("edge_expiry_days"),
        show_progress=bool(args.progress),
    )

    out_csv = Path(p["out_csv"])
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    if not p.get("compare_baselines", True):
        res = comp.main
        out = res.daily_ret.to_frame(name="main")
        out["main_gross"] = res.gross_daily_ret
        out["main_cost"] = res.daily_cost
        out["main_turnover"] = res.turnover
        out["cumulative"] = res.cumulative
        out.to_csv(out_csv)
        if not res.rebalance_log.empty:
            res.rebalance_log.to_csv(out_csv.parent / "rebalances.csv", index=False)
        summ = portfolio_metrics(
            res.daily_ret,
            gross_daily_ret=res.gross_daily_ret,
            daily_cost=res.daily_cost,
            turnover=res.turnover,
        )
        summ["n_rebalances"] = int(len(res.rebalance_log))
        out_summary = Path(p["out_summary"])

        def _json_val(v):
            if hasattr(v, "item"):
                return v.item()
            if isinstance(v, (float, int, str)) or v is None:
                return v
            return str(v)

        out_summary.parent.mkdir(parents=True, exist_ok=True)
        with open(out_summary, "w") as f:
            json.dump({k: _json_val(v) for k, v in summ.items()}, f, indent=2)
        print(json.dumps(summ, indent=2))
        print(f"[saved] {out_csv}  {out_summary}")
        return

    daily = pd.DataFrame(
        {
            "main": comp.main.daily_ret,
            "main_gross": comp.main.gross_daily_ret,
            "main_cost": comp.main.daily_cost,
            "main_turnover": comp.main.turnover,
            "random": comp.random.daily_ret,
            "random_gross": comp.random.gross_daily_ret,
            "random_cost": comp.random.daily_cost,
            "random_turnover": comp.random.turnover,
            "momentum": comp.momentum.daily_ret,
            "momentum_gross": comp.momentum.gross_daily_ret,
            "momentum_cost": comp.momentum.daily_cost,
            "momentum_turnover": comp.momentum.turnover,
            "structural": comp.structural.daily_ret,
            "structural_gross": comp.structural.gross_daily_ret,
            "structural_cost": comp.structural.daily_cost,
            "structural_turnover": comp.structural.turnover,
            "equal_weight": comp.equal_weight.daily_ret,
            "equal_weight_gross": comp.equal_weight.gross_daily_ret,
            "equal_weight_cost": comp.equal_weight.daily_cost,
            "equal_weight_turnover": comp.equal_weight.turnover,
        }
    )
    daily.to_csv(out_csv)
    if not comp.main.rebalance_log.empty:
        comp.main.rebalance_log.to_csv(out_csv.parent / "rebalances.csv", index=False)

    table = comparison_metrics_table(comp)
    out_cmp = Path(p["out_comparison"])
    out_cmp.parent.mkdir(parents=True, exist_ok=True)
    table.to_csv(out_cmp, index=False)

    summ_main = portfolio_metrics(
        comp.main.daily_ret,
        gross_daily_ret=comp.main.gross_daily_ret,
        daily_cost=comp.main.daily_cost,
        turnover=comp.main.turnover,
    )
    summ_main["n_rebalances"] = int(len(comp.main.rebalance_log))

    def _json_val(v):
        if hasattr(v, "item"):
            return v.item()
        if isinstance(v, (float, int, str)) or v is None:
            return v
        return str(v)

    out_summary = Path(p["out_summary"])
    out_summary.parent.mkdir(parents=True, exist_ok=True)
    with open(out_summary, "w") as f:
        json.dump({k: _json_val(v) for k, v in summ_main.items()}, f, indent=2)

    print(table.to_string(index=False))
    print(f"[saved] {out_csv}  {out_cmp}  {out_summary}")


if __name__ == "__main__":
    main()
