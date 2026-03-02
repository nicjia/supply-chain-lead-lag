# scripts/run_leadlag_tests.py

from __future__ import annotations
import argparse
import numpy as np
import pandas as pd
from linearmodels.panel import PanelOLS


'''
Forward baseline:
python scripts/run_leadlag_tests.py \
  --panel data/leadlag_panel_forward.parquet \
  --direction forward

Reverse baseline:
python scripts/run_leadlag_tests.py \
  --panel data/leadlag_panel_reverse.parquet \
  --direction reverse

Forward winsorized & nonzero:
python scripts/run_leadlag_tests.py \
  --panel data/leadlag_panel_forward.parquet \
  --direction forward --winsorize --nonzero

Reverse winsorized & nonzero:
python scripts/run_leadlag_tests.py \
  --panel data/leadlag_panel_reverse.parquet \
  --direction reverse --winsorize --nonzero

'''
# --------------------------------------------------
# Utils
# --------------------------------------------------

def winsorize(s: pd.Series, lo=0.01, hi=0.99) -> pd.Series:
    ql = s.quantile(lo)
    qh = s.quantile(hi)
    return s.clip(ql, qh)

def run_panel_reg(
    panel: pd.DataFrame,
    entity_col: str,
    ret_prefix: str,
    signal_col: str,
    horizon_max: int,
) -> pd.DataFrame:

    df = panel.set_index([entity_col, "date"]).sort_index()
    results = []

    for h in range(1, horizon_max + 1):

        yvar = df[f"{ret_prefix}_fwd_{h}"]
        X = df[[signal_col,
                f"{ret_prefix}_lag_1",
                f"{ret_prefix}_lag_2",
                f"{ret_prefix}_lag_3",
                f"{ret_prefix}_lag_4",
                f"{ret_prefix}_lag_5"]].copy()

        X["const"] = 1.0

        mod = PanelOLS(yvar, X, entity_effects=True, time_effects=True)
        res = mod.fit(cov_type="clustered", cluster_entity=True)

        results.append({
            "h": h,
            "beta": float(res.params[signal_col]),
            "se": float(res.std_errors[signal_col]),
            "t": float(res.tstats[signal_col]),
            "p": float(res.pvalues[signal_col]),
            "nobs": int(res.nobs),
        })

        print(
            f"h={h} beta={res.params[signal_col]:.6g} "
            f"t={res.tstats[signal_col]:.3g} "
            f"p={res.pvalues[signal_col]:.3g}"
        )

    return pd.DataFrame(results)

# --------------------------------------------------
# Main
# --------------------------------------------------

def main():

    ap = argparse.ArgumentParser()
    ap.add_argument("--panel", type=str, default="data/leadlag_panel.parquet")
    ap.add_argument("--direction", type=str, default="forward",
                    choices=["forward", "reverse"])
    ap.add_argument("--horizon_max", type=int, default=5)
    ap.add_argument("--winsorize", action="store_true")
    ap.add_argument("--nonzero", action="store_true")
    args = ap.parse_args()

    panel = pd.read_parquet(args.panel)
    panel["date"] = pd.to_datetime(panel["date"]).dt.normalize()

    # --------------------------------------------------
    # Forward or Reverse Setup
    # --------------------------------------------------

    if args.direction == "forward":
        entity_col = "supplier_gvkey"
        signal_col = "y"
        ret_prefix = "sup_r"

    else:  # reverse
        entity_col = "customer_gvkey"
        signal_col = "z"
        ret_prefix = "cust_r"

    # Winsorize if requested
    if args.winsorize:
        panel[f"{signal_col}_w"] = winsorize(panel[signal_col])
        signal_col = f"{signal_col}_w"

    # Nonzero filter
    if args.nonzero:
        panel = panel.loc[panel[signal_col] != 0.0].copy()

    # Diagnostics
    print(f"\n[Direction] {args.direction.upper()}")
    print(f"[Signal] {signal_col}")
    print(f"[Std] {panel[signal_col].std():.6g}")
    print(f"[Nonzero] {(panel[signal_col]!=0).mean():.3%}")

    # Run regression
    results = run_panel_reg(
        panel,
        entity_col=entity_col,
        ret_prefix=ret_prefix,
        signal_col=signal_col,
        horizon_max=args.horizon_max,
    )

    # --------------------------------------------------
    # Save clean filename
    # --------------------------------------------------

    fname = f"data/leadlag_{args.direction}"
    if args.winsorize:
        fname += "_winsor"
    if args.nonzero:
        fname += "_nonzero"
    fname += ".csv"

    results.to_csv(fname, index=False)
    print(f"[saved] {fname}")

if __name__ == "__main__":
    main()