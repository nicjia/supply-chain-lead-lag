from __future__ import annotations

import argparse
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from supply_chain_leadlag.matrix import build_lead_lag_matrix_gvkey, load_edges, load_returns_wide_by_gvkey


def main():
    ap = argparse.ArgumentParser(description="Build lead–lag matrix C on supply-chain edges.")
    ap.add_argument("--edges_csv", default="merged_edges.csv")
    ap.add_argument("--edge_date_col", default="srcdate", choices=["filing_date", "srcdate"])
    ap.add_argument("--returns_parquet", default="data/returns_with_gvkey.parquet")
    ap.add_argument(
        "--score",
        default="tstat_diff",
        choices=[
            "tstat_diff",
            "beta_diff",
            "cross_corr",
            "regression_r2",
            "granger",
            "levy",
        ],
        help="Edge asymmetry: OLS horizon regressions or pairwise (corr / R² / Granger / Lévy).",
    )
    ap.add_argument("--horizon", type=int, default=1, help="Used for tstat_diff / beta_diff only.")
    ap.add_argument("--max_lag", type=int, default=5, help="Used for cross_corr, regression_r2, granger, levy.")
    ap.add_argument("--granger_n_lags", type=int, default=2)
    ap.add_argument("--min_obs", type=int, default=80)
    ap.add_argument("--outdir", default=None, help="Default: results/leadlag_stage2_gvkey or ..._<score> if score≠tstat_diff.")
    args = ap.parse_args()

    edges_csv = args.edges_csv
    returns_parquet = args.returns_parquet
    if args.outdir:
        outdir = Path(args.outdir)
    elif args.score == "tstat_diff":
        outdir = Path("results/leadlag_stage2_gvkey")
    else:
        outdir = Path(f"results/leadlag_stage2_gvkey_{args.score}")
    outdir.mkdir(parents=True, exist_ok=True)

    edges = load_edges(edges_csv, date_col=args.edge_date_col)
    R = load_returns_wide_by_gvkey(returns_parquet)

    print(f"[edges] {len(edges):,} (with valid weight & customer_gvkey)")
    print(f"[returns wide] {R.shape}")
    print(f"[score] {args.score}")

    res = build_lead_lag_matrix_gvkey(
        returns_wide=R,
        edges_resolved=edges,
        horizon=args.horizon,
        min_obs=args.min_obs,
        winsor_q=0.001,
        score=args.score,  # type: ignore[arg-type]
        max_lag=args.max_lag,
        granger_n_lags=args.granger_n_lags,
    )

    res.edge_scores.to_csv(outdir / "edge_scores_h1.csv", index=False)
    res.C.to_parquet(outdir / "C_h1.parquet")
    res.S.to_parquet(outdir / "S_h1.parquet")
    res.leadingness.to_csv(outdir / "leadingness_h1.csv", header=["leadingness"])

    print("\nTop 20 leaders (gvkey):")
    print(res.leadingness.head(20))
    print("\nBottom 20 laggers (gvkey):")
    print(res.leadingness.tail(20).sort_values())


if __name__ == "__main__":
    main()
