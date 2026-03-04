# scripts/build_leadlag_panel.py
# Usage:
#   python scripts/build_leadlag_panel.py --direction forward --horizon_max 5 --min_edge_weight 0.0 --out data/leadlag_panel_forward.parquet
#   python scripts/build_leadlag_panel.py --direction reverse --horizon_max 5 --min_edge_weight 0.0 --out data/leadlag_panel_reverse.parquet

from __future__ import annotations

import argparse
import numpy as np
import pandas as pd

BASE_DIR = "data"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base_dir", type=str, default=BASE_DIR)
    ap.add_argument("--direction", type=str, default="forward", choices=["forward", "reverse"])
    ap.add_argument("--horizon_max", type=int, default=5)
    ap.add_argument("--min_edge_weight", type=float, default=0.0)
    ap.add_argument("--use_logret", action="store_true")
    ap.add_argument("--out", type=str, required=True)
    args = ap.parse_args()

    HORIZON_MAX = args.horizon_max
    MIN_EDGE_WEIGHT = args.min_edge_weight
    USE_LOGRET = args.use_logret

    # ----------------------------
    # 1) Load returns (gvkey-native)
    # ----------------------------
    ret = pd.read_parquet(f"{args.base_dir}/returns_with_gvkey.parquet")
    ret["date"] = pd.to_datetime(ret["date"]).dt.normalize()
    ret["gvkey"] = ret["gvkey"].astype(str).str.zfill(6)
    ret = ret.dropna(subset=["RET"]).sort_values(["gvkey", "date"])
    ret = ret.drop_duplicates(["gvkey", "date"], keep="last")

    if USE_LOGRET:
        ret["r"] = np.log1p(ret["RET"].astype(float))
    else:
        ret["r"] = ret["RET"].astype(float)

    # ----------------------------
    # 2) Load edges (merged_edges.csv — customer_gvkey already resolved)
    # ----------------------------
    edges = pd.read_csv("merged_edges.csv")
    edges["date"] = pd.to_datetime(edges["srcdate"]).dt.normalize()
    edges["supplier_gvkey"] = edges["supplier_gvkey"].astype(str).str.zfill(6)
    # customer_gvkey stored as float in CSV (e.g. 2285.0); convert float→int→str→zfill
    edges["customer_gvkey"] = (
        pd.to_numeric(edges["customer_gvkey"], errors="coerce")
        .astype("Int64")
        .astype(str)
        .str.zfill(6)
    )
    edges["weight_wji"] = pd.to_numeric(edges["weight_wji"], errors="coerce")

    # Drop relationship-only edges (no dollar weight)
    edges = edges.dropna(subset=["weight_wji"]).copy()

    # Drop rows where customer_gvkey was not resolved
    edges = edges[edges["customer_gvkey"].notna()
                  & (edges["customer_gvkey"] != "nan")
                  & (edges["customer_gvkey"] != "<NA>")
                  & (edges["customer_gvkey"] != "00<NA>")].copy()

    if MIN_EDGE_WEIGHT is not None:
        edges = edges.loc[edges["weight_wji"].abs() >= float(MIN_EDGE_WEIGHT)].copy()

    edges = edges.sort_values(["customer_gvkey", "supplier_gvkey", "date"])
    E0 = edges

    # ----------------------------
    # 3) Returns tables
    # ----------------------------
    cust_ret = ret[["gvkey", "date", "r"]].rename(columns={"gvkey": "customer_gvkey", "r": "cust_r"})
    sup_ret  = ret[["gvkey", "date", "r"]].rename(columns={"gvkey": "supplier_gvkey", "r": "sup_r"})

    # ----------------------------
    # 4) Forward-fill PIT weights to trading dates per (customer_gvkey, supplier_gvkey)
    # ----------------------------
    pairs = E0[["customer_gvkey", "supplier_gvkey", "date", "weight_wji"]].copy()
    pairs = pairs.sort_values(["customer_gvkey", "supplier_gvkey", "date"])

    trading_dates = pd.DataFrame({"date": ret["date"].unique()}).sort_values("date").reset_index(drop=True)

    out_chunks = []
    for (cg, sg), gdf in pairs.groupby(["customer_gvkey", "supplier_gvkey"], sort=False):
        tmp = trading_dates.copy()
        tmp["customer_gvkey"] = cg
        tmp["supplier_gvkey"] = sg
        tmp = pd.merge_asof(
            tmp,
            gdf[["date", "weight_wji"]].sort_values("date"),
            on="date",
            direction="backward",
            allow_exact_matches=True,
        ).dropna(subset=["weight_wji"])
        out_chunks.append(tmp)

    E_daily = pd.concat(out_chunks, ignore_index=True)

    # ----------------------------
    # 5) Build panel: forward or reverse
    # ----------------------------
    if args.direction == "forward":
        # y_{i,t} = sum_j w_{j->i,t} * r_{j,t}
        E_daily2 = E_daily.merge(cust_ret, on=["customer_gvkey", "date"], how="inner")
        E_daily2["term"] = E_daily2["weight_wji"] * E_daily2["cust_r"]

        sig = (
            E_daily2.groupby(["supplier_gvkey", "date"], as_index=False)["term"].sum()
            .rename(columns={"term": "y"})
        )

        panel = sup_ret.merge(sig, on=["supplier_gvkey", "date"], how="left")
        panel["y"] = panel["y"].fillna(0.0)
        panel = panel.sort_values(["supplier_gvkey", "date"])

        for h in range(1, HORIZON_MAX + 1):
            panel[f"sup_r_fwd_{h}"] = panel.groupby("supplier_gvkey")["sup_r"].shift(-h)
        for lag in [1, 2, 3, 4, 5]:
            panel[f"sup_r_lag_{lag}"] = panel.groupby("supplier_gvkey")["sup_r"].shift(lag)

        panel = panel.dropna(subset=[f"sup_r_fwd_{h}" for h in range(1, HORIZON_MAX + 1)])

        # Save
        panel.to_parquet(args.out, index=False)
        print(f"[saved] {args.out}")
        nrows, ncols = panel.shape
        print(f"[panel:forward] shape=({nrows:,}, {ncols:,}) suppliers={panel['supplier_gvkey'].nunique():,} dates={panel['date'].nunique():,}")
        print(panel["y"].describe())
        print("std(y)=", float(panel["y"].std()))
        print("mean(|y|)=", float(panel["y"].abs().mean()))

    else:
        # z_{j,t} = sum_i w_{j->i,t} * r_{i,t}
        E_daily2 = E_daily.merge(sup_ret, on=["supplier_gvkey", "date"], how="inner")
        E_daily2["term_rev"] = E_daily2["weight_wji"] * E_daily2["sup_r"]

        sig = (
            E_daily2.groupby(["customer_gvkey", "date"], as_index=False)["term_rev"].sum()
            .rename(columns={"term_rev": "z"})
        )

        panel = cust_ret.merge(sig, on=["customer_gvkey", "date"], how="left")
        panel["z"] = panel["z"].fillna(0.0)
        panel = panel.sort_values(["customer_gvkey", "date"])

        for h in range(1, HORIZON_MAX + 1):
            panel[f"cust_r_fwd_{h}"] = panel.groupby("customer_gvkey")["cust_r"].shift(-h)
        for lag in [1, 2, 3, 4, 5]:
            panel[f"cust_r_lag_{lag}"] = panel.groupby("customer_gvkey")["cust_r"].shift(lag)

        panel = panel.dropna(subset=[f"cust_r_fwd_{h}" for h in range(1, HORIZON_MAX + 1)])

        # Save
        panel.to_parquet(args.out, index=False)
        print(f"[saved] {args.out}")
        nrows, ncols = panel.shape
        print(f"[panel:reverse] shape=({nrows:,}, {ncols:,}) customers={panel['customer_gvkey'].nunique():,} dates={panel['date'].nunique():,}")
        print(panel["z"].describe())
        print("std(z)=", float(panel["z"].std()))
        print("mean(|z|)=", float(panel["z"].abs().mean()))

if __name__ == "__main__":
    main()