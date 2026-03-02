# scripts/build_leadlag_panel.py
# Usage:
# python scripts/build_leadlag_panel.py --horizon_max 5 --min_edge_weight 0.0 --out data/leadlag_panel.parquet

from __future__ import annotations

import argparse
import numpy as np
import pandas as pd

BASE_DIR = "data"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base_dir", type=str, default=BASE_DIR)
    ap.add_argument("--horizon_max", type=int, default=5)
    ap.add_argument("--min_edge_weight", type=float, default=0.0)
    ap.add_argument("--use_logret", action="store_true")
    ap.add_argument("--out", type=str, default="data/leadlag_panel.parquet")
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
    # 2) Load edges
    # ----------------------------
    edges = pd.read_csv(f"{args.base_dir}/output_edges.csv", parse_dates=["date"])
    edges["date"] = pd.to_datetime(edges["date"]).dt.normalize()
    edges["supplier_gvkey"] = edges["supplier_gvkey"].astype(str).str.zfill(6)
    edges["customer_tic"] = edges["customer_tic"].astype(str).str.strip()
    edges["weight_wji"] = edges["weight_wji"].astype(float)

    if MIN_EDGE_WEIGHT is not None:
        edges = edges.loc[edges["weight_wji"].abs() >= float(MIN_EDGE_WEIGHT)].copy()

    edges = edges.sort_values(["customer_tic", "supplier_gvkey", "date"])

    # ----------------------------
    # 3) Load tic->gvkey translator (validity intervals)
    # ----------------------------
    link = pd.read_csv(f"{args.base_dir}/query3_translator.csv")
    link["gvkey"] = link["gvkey"].astype(str).str.zfill(6)
    link["tic"] = link["tic"].astype(str).str.strip()
    link["LINKDT"] = pd.to_datetime(link["LINKDT"], errors="coerce")
    link["LINKENDDT"] = link["LINKENDDT"].replace("E", "9999-12-31")
    link["LINKENDDT"] = pd.to_datetime(link["LINKENDDT"], errors="coerce")

    if "LINKTYPE" in link.columns:
        link = link[link["LINKTYPE"].isin(["LU", "LC"])].copy()

    # ----------------------------
    # 4) Map customer_tic -> customer_gvkey on edge PIT dates
    # ----------------------------
    edge_dates = edges[["date", "customer_tic"]].drop_duplicates()
    edge_dates = edge_dates.merge(link, left_on="customer_tic", right_on="tic", how="left")
    edge_dates = edge_dates.loc[
        (edge_dates["LINKDT"].notna()) &
        (edge_dates["LINKENDDT"].notna()) &
        (edge_dates["LINKDT"] <= edge_dates["date"]) &
        (edge_dates["date"] <= edge_dates["LINKENDDT"])
    ].copy()

    if "LINKPRIM" in edge_dates.columns:
        edge_dates["pref"] = (edge_dates["LINKPRIM"] == "P").astype(int)
        edge_dates = edge_dates.sort_values(["date", "customer_tic", "pref"], ascending=[True, True, False])

    cust_map = edge_dates.drop_duplicates(["date", "customer_tic"], keep="first")[["date", "customer_tic", "gvkey"]]
    cust_map = cust_map.rename(columns={"gvkey": "customer_gvkey"})

    # ----------------------------
    # 5) Join edges -> customer gvkey (PIT)
    # ----------------------------
    E0 = edges.merge(cust_map, on=["date", "customer_tic"], how="inner")

    # ----------------------------
    # 6) Returns tables
    # ----------------------------
    cust_ret = ret[["gvkey", "date", "r"]].rename(columns={"gvkey": "customer_gvkey", "r": "cust_r"})
    sup_ret  = ret[["gvkey", "date", "r"]].rename(columns={"gvkey": "supplier_gvkey", "r": "sup_r"})

    # ----------------------------
    # 7) Forward-fill PIT weights to trading dates per (customer_gvkey, supplier_gvkey)
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
    # 7b) Aggregate customer pressure to supplier-day
    # ----------------------------
    E_daily = E_daily.merge(cust_ret, on=["customer_gvkey", "date"], how="inner")
    E_daily["term"] = E_daily["weight_wji"] * E_daily["cust_r"]

    y = (
        E_daily.groupby(["supplier_gvkey", "date"], as_index=False)["term"].sum()
        .rename(columns={"term": "y"})
    )

    # ----------------------------
    # 8) Build final panel with horizons + lags
    # ----------------------------
    panel = sup_ret.merge(y, on=["supplier_gvkey", "date"], how="left")
    panel["y"] = panel["y"].fillna(0.0)
    panel = panel.sort_values(["supplier_gvkey", "date"])

    for h in range(1, HORIZON_MAX + 1):
        panel[f"sup_r_fwd_{h}"] = panel.groupby("supplier_gvkey")["sup_r"].shift(-h)

    for lag in [1, 2, 3, 4, 5]:
        panel[f"sup_r_lag_{lag}"] = panel.groupby("supplier_gvkey")["sup_r"].shift(lag)

    panel = panel.dropna(subset=[f"sup_r_fwd_{h}" for h in range(1, HORIZON_MAX + 1)])

    # ----------------------------
    # 9) Save + lightweight summary
    # ----------------------------
    panel.to_parquet(args.out, index=False)

    print(f"[saved] {args.out}")
    nrows, ncols = panel.shape
    print(f"[panel] shape=({nrows:,}, {ncols:,}) "
        f"suppliers={panel['supplier_gvkey'].nunique():,} "
        f"dates={panel['date'].nunique():,}")
    print(panel["y"].describe())
    print("std(y)=", float(panel["y"].std()))
    print("mean(|y|)=", float(panel["y"].abs().mean()))
    print(panel.groupby("supplier_gvkey")["y"].count().describe())

if __name__ == "__main__":
    main()