from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

import numpy as np
import pandas as pd


# ----------------------------
# Small OLS helper
# ----------------------------
def _ols_slope_tstat(x: np.ndarray, y: np.ndarray) -> Tuple[float, float, int]:
    """OLS y ~ a + b x. Returns (beta, tstat, nobs), homoskedastic SE."""
    mask = np.isfinite(x) & np.isfinite(y)
    x = x[mask]
    y = y[mask]
    n = int(x.size)
    if n < 10:
        return np.nan, np.nan, n

    x_mean = x.mean()
    y_mean = y.mean()

    x_c = x - x_mean
    y_c = y - y_mean

    sxx = float(np.dot(x_c, x_c))
    if sxx <= 1e-20:
        return np.nan, np.nan, n

    beta = float(np.dot(x_c, y_c) / sxx)

    # residuals
    y_hat = y_mean + beta * (x - x_mean)
    resid = y - y_hat

    dof = n - 2
    if dof <= 0:
        return beta, np.nan, n

    sigma2 = float(np.dot(resid, resid) / dof)
    se_beta = float(np.sqrt(sigma2 / sxx)) if sigma2 >= 0 else np.nan
    tstat = float(beta / se_beta) if (np.isfinite(se_beta) and se_beta > 0) else np.nan

    return beta, tstat, n


@dataclass
class LeadLagResult:
    edge_scores: pd.DataFrame
    C: pd.DataFrame
    S: pd.DataFrame
    leadingness: pd.Series


# ----------------------------
# Loading + ID resolution
# ----------------------------
def load_returns_wide_by_gvkey(returns_parquet: str) -> pd.DataFrame:
    r = pd.read_parquet(returns_parquet)
    r["date"] = pd.to_datetime(r["date"])
    r["RET"] = pd.to_numeric(r["RET"], errors="coerce")
    r = r[np.isfinite(r["RET"])].copy()

    # keep gvkey as string with leading zeros
    r["gvkey"] = r["gvkey"].astype(str).str.zfill(6)

    wide = r.pivot_table(index="date", columns="gvkey", values="RET", aggfunc="last").sort_index()
    return wide


def load_link_table_query3(query3_csv: str) -> pd.DataFrame:
    """
    Expect columns like:
    gvkey, tic, LINKDT, LINKENDDT, ... (others ignored)
    """
    lk = pd.read_csv(query3_csv)
    lk["gvkey"] = lk["gvkey"].astype(str).str.zfill(6)
    lk["tic"] = lk["tic"].astype(str)

    lk["LINKDT"] = pd.to_datetime(lk["LINKDT"], errors="coerce")
    lk["LINKENDDT"] = pd.to_datetime(lk["LINKENDDT"], errors="coerce")

    # Treat missing LINKENDDT as "open-ended"
    lk["LINKENDDT"] = lk["LINKENDDT"].fillna(pd.Timestamp("2099-12-31"))

    # Basic cleanup
    lk = lk.dropna(subset=["tic", "gvkey", "LINKDT", "LINKENDDT"]).copy()
    return lk


def resolve_customer_gvkey_asof(edges: pd.DataFrame, link: pd.DataFrame) -> pd.DataFrame:
    """
    Resolve customer_tic -> customer_gvkey using link intervals [LINKDT, LINKENDDT].
    We do a merge then filter by interval membership. If multiple matches, pick the most recent LINKDT.
    """
    e = edges.copy()
    e["date"] = pd.to_datetime(e["date"])
    e["customer_tic"] = e["customer_tic"].astype(str)
    e["supplier_gvkey"] = e["supplier_gvkey"].astype(str).str.zfill(6)

    # merge on ticker
    m = e.merge(link, left_on="customer_tic", right_on="tic", how="left", suffixes=("", "_lk"))

    # interval filter: LINKDT <= edge_date <= LINKENDDT
    ok = (m["LINKDT"] <= m["date"]) & (m["date"] <= m["LINKENDDT"])
    m = m[ok].copy()

    # if multiple links, choose the one with latest LINKDT (most specific)
    m = m.sort_values(["customer_tic", "supplier_gvkey", "date", "LINKDT"])
    m = m.groupby(["customer_tic", "supplier_gvkey", "date"], as_index=False).tail(1)

    m = m.rename(columns={"gvkey": "customer_gvkey"})
    m["customer_gvkey"] = m["customer_gvkey"].astype(str).str.zfill(6)

    keep_cols = [
        "date",
        "customer_tic",
        "customer_gvkey",
        "supplier_tic",
        "supplier_gvkey",
        "weight_wji",
        "match_tier",
    ]
    return m[keep_cols]


def load_edges(edges_csv: str, tier_max: int = 1) -> pd.DataFrame:
    e = pd.read_csv(edges_csv)
    e["date"] = pd.to_datetime(e["date"])
    if "match_tier" in e.columns and tier_max is not None:
        e = e[e["match_tier"] <= tier_max].copy()

    e["supplier_gvkey"] = e["supplier_gvkey"].astype(str).str.zfill(6)
    e["weight_wji"] = pd.to_numeric(e["weight_wji"], errors="coerce")
    e = e[np.isfinite(e["weight_wji"])].copy()
    return e


# ----------------------------
# Stage 2: build C, S, leadingness
# ----------------------------
def build_lead_lag_matrix_gvkey(
    returns_wide: pd.DataFrame,
    edges_resolved: pd.DataFrame,
    *,
    horizon: int = 1,
    min_obs: int = 80,
    winsor_q: float = 0.001,
    score: str = "tstat_diff",  # {"tstat_diff","beta_diff"}
) -> LeadLagResult:
    """
    For each edge (customer_gvkey -> supplier_gvkey):
      forward:  r_supp(t+h) ~ r_cust(t)
      reverse:  r_cust(t+h) ~ r_supp(t)
    Score = (t_fwd - t_rev) * w.
    Build C[customer, supplier] = score; then S = C - C^T; leadingness = row-sum(S).
    """
    R = returns_wide.sort_index()

    # winsorize cross-sectionally to reduce extreme daily returns
    if winsor_q is not None and 0 < winsor_q < 0.5:
        lo = R.quantile(winsor_q)
        hi = R.quantile(1 - winsor_q)
        R = R.clip(lower=lo, upper=hi, axis=1)

    Y = R.shift(-horizon)

    records = []
    colset = set(R.columns)

    for row in edges_resolved.itertuples(index=False):
        cust = getattr(row, "customer_gvkey")
        supp = getattr(row, "supplier_gvkey")
        w = float(getattr(row, "weight_wji"))

        if cust not in colset or supp not in colset:
            continue

        x_fwd = R[cust].to_numpy()
        y_fwd = Y[supp].to_numpy()
        beta_fwd, t_fwd, n_fwd = _ols_slope_tstat(x_fwd, y_fwd)

        x_rev = R[supp].to_numpy()
        y_rev = Y[cust].to_numpy()
        beta_rev, t_rev, n_rev = _ols_slope_tstat(x_rev, y_rev)

        n_eff = min(n_fwd, n_rev)
        if n_eff < min_obs:
            continue

        if score == "tstat_diff":
            raw = t_fwd - t_rev
        elif score == "beta_diff":
            raw = beta_fwd - beta_rev
        else:
            raise ValueError("score must be {'tstat_diff','beta_diff'}")

        s_ij = w * raw

        records.append(
            dict(
                customer_gvkey=cust,
                supplier_gvkey=supp,
                w=w,
                beta_fwd=beta_fwd,
                t_fwd=t_fwd,
                beta_rev=beta_rev,
                t_rev=t_rev,
                n=n_eff,
                score=s_ij,
            )
        )

    edge_scores = pd.DataFrame.from_records(records)
    if edge_scores.empty:
        raise ValueError("No valid edge scores computed. Check ID coverage and min_obs.")

    # C[customer, supplier] = score
    C = edge_scores.pivot_table(
        index="customer_gvkey",
        columns="supplier_gvkey",
        values="score",
        aggfunc="mean",
        fill_value=0.0,
    )

    nodes = sorted(set(C.index).union(C.columns))
    C = C.reindex(index=nodes, columns=nodes, fill_value=0.0)

    S = C - C.T
    leadingness = S.sum(axis=1).sort_values(ascending=False)

    return LeadLagResult(
        edge_scores=edge_scores.sort_values("score", ascending=False).reset_index(drop=True),
        C=C,
        S=S,
        leadingness=leadingness,
    )


def main():
    # ---- paths (edit if needed) ----
    edges_csv = "src/output_edges.csv"
    query3_csv = "src/query3_translator.csv"
    returns_parquet = "src/returns_with_gvkey.parquet"
    outdir = Path("results/leadlag_stage2_gvkey")
    outdir.mkdir(parents=True, exist_ok=True)

    # ---- load ----
    edges = load_edges(edges_csv, tier_max=1)
    link = load_link_table_query3(query3_csv)
    edges_resolved = resolve_customer_gvkey_asof(edges, link)

    # keep only edges where customer_gvkey resolved
    edges_resolved = edges_resolved.dropna(subset=["customer_gvkey"]).copy()

    R = load_returns_wide_by_gvkey(returns_parquet)

    print(f"[edges raw] {len(edges):,}")
    print(f"[edges resolved] {len(edges_resolved):,} (after customer gvkey as-of resolution)")
    print(f"[returns wide] {R.shape}")

    # ---- build matrix ----
    res = build_lead_lag_matrix_gvkey(
        returns_wide=R,
        edges_resolved=edges_resolved,
        horizon=1,
        min_obs=80,
        winsor_q=0.001,
        score="tstat_diff",
    )

    # ---- save ----
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