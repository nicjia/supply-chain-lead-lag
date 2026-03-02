# scripts/run_leadlag_tests.py

'''
Usage:

# Baseline
python scripts/run_leadlag_tests.py --panel data/leadlag_panel.parquet --horizon_max 5

# Winsorized y
python scripts/run_leadlag_tests.py --winsorize

# Only days where signal is nonzero
python scripts/run_leadlag_tests.py --nonzero_y

# Both
python scripts/run_leadlag_tests.py --winsorize --nonzero_y
'''
from __future__ import annotations

import argparse
import numpy as np
import pandas as pd
from linearmodels.panel import PanelOLS

def winsorize(s: pd.Series, lo=0.01, hi=0.99) -> pd.Series:
    ql = s.quantile(lo)
    qh = s.quantile(hi)
    return s.clip(ql, qh)

def run_horizon_regs(panel: pd.DataFrame, horizon_max: int, y_col: str) -> pd.DataFrame:
    df = panel.copy()
    df = df.set_index(["supplier_gvkey", "date"]).sort_index()

    results = []
    for h in range(1, horizon_max + 1):
        yvar = df[f"sup_r_fwd_{h}"]
        X = df[[y_col, "sup_r_lag_1", "sup_r_lag_2", "sup_r_lag_3", "sup_r_lag_4", "sup_r_lag_5"]].copy()
        X["const"] = 1.0

        mod = PanelOLS(yvar, X, entity_effects=True, time_effects=True)
        res = mod.fit(cov_type="clustered", cluster_entity=True)

        results.append({
            "h": h,
            "beta_y": float(res.params[y_col]),
            "se_y": float(res.std_errors[y_col]),
            "t_y": float(res.tstats[y_col]),
            "p_y": float(res.pvalues[y_col]),
            "nobs": int(res.nobs),
        })
        print(f"h={h} beta={res.params[y_col]:.6g} t={res.tstats[y_col]:.3g} p={res.pvalues[y_col]:.3g} n={res.nobs}")

    return pd.DataFrame(results)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--panel", type=str, default="data/leadlag_panel.parquet")
    ap.add_argument("--horizon_max", type=int, default=5)
    ap.add_argument("--winsorize", action="store_true")
    ap.add_argument("--nonzero_y", action="store_true")
    ap.add_argument("--out_csv", type=str, default="data/leadlag_horizon_results.csv")
    args = ap.parse_args()

    panel = pd.read_parquet(args.panel)
    panel["date"] = pd.to_datetime(panel["date"]).dt.normalize()

    # Prepare y variants
    if args.winsorize:
        panel["y_w"] = winsorize(panel["y"], 0.01, 0.99)
        y_col = "y_w"
    else:
        y_col = "y"

    if args.nonzero_y:
        panel = panel.loc[panel[y_col] != 0.0].copy()

    print(f"[load] {args.panel} shape={panel.shape:,} suppliers={panel['supplier_gvkey'].nunique():,} dates={panel['date'].nunique():,}")
    print(f"[y] col={y_col} std={panel[y_col].std():.6g} mean_abs={panel[y_col].abs().mean():.6g} nonzero={(panel[y_col]!=0).mean():.3%}")

    out = run_horizon_regs(panel, args.horizon_max, y_col=y_col)
    out.to_csv(args.out_csv, index=False)
    print(f"[saved] {args.out_csv}")

if __name__ == "__main__":
    main()