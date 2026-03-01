#!/usr/bin/env python3
"""
Resolve customer names (cnms) to tickers using CSV1, CSV2, and optional firm_like list.
Outputs: ../data/processed/edges_path_a.csv, unmatched_cnms.csv, match_summary.txt
"""
import re
import pandas as pd
import numpy as np
from pathlib import Path

BASE = Path(__file__).resolve().parent
DATA = BASE.parent / "data"
OUT_DIR = DATA / "processed"

CSV1_PATH = DATA / "gqqatnce2nuvm7hl.csv"
CSV2_PATH = DATA / "hwihvjjrzgyby1vf.csv"
FIRM_LIKE_PATH = DATA / "firm_like_cnms_list.txt"
if not FIRM_LIKE_PATH.exists():
    FIRM_LIKE_PATH = BASE / "firm_like_cnms_list.txt"

MANUAL_MAP = {
    "APPLE": "AAPL", "APPLE INC": "AAPL", "APPLE COMPUTER": "AAPL",
    "MICROSOFT": "MSFT", "MICROSOFT CORP": "MSFT", "MICROSOFT CORPORATION": "MSFT",
    "AMAZON": "AMZN", "AMAZON.COM": "AMZN", "AMAZON WEB SERVICES": "AMZN",
    "GOOGLE": "GOOGL", "ALPHABET": "GOOGL", "GOOGLE INC": "GOOGL",
    "WALMART": "WMT", "WAL-MART": "WMT", "WAL MART": "WMT",
    "TARGET": "TGT", "TARGET CORP": "TGT",
    "HOME DEPOT": "HD", "THE HOME DEPOT": "HD",
    "COSTCO": "COST", "COSTCO WHOLESALE": "COST",
    "DELL": "DELL", "DELL INC": "DELL", "DELL TECHNOLOGIES": "DELL",
    "HP INC": "HPQ", "HEWLETT PACKARD": "HPQ", "HEWLETT-PACKARD": "HPQ",
    "BOEING": "BA", "THE BOEING COMPANY": "BA",
    "AT&T": "T", "ATT": "T",
    "VERIZON": "VZ", "VERIZON COMMUNICATIONS": "VZ",
    "FORD": "F", "FORD MOTOR": "F", "FORD MOTOR COMPANY": "F",
    "GENERAL MOTORS": "GM", "GM": "GM",
    "CVS": "CVS", "CVS HEALTH": "CVS", "CVS PHARMACY": "CVS",
    "WALGREENS": "WBA", "WALGREENS BOOTS ALLIANCE": "WBA",
    "BEST BUY": "BBY", "LOWES": "LOW", "LOWE'S": "LOW",
    "LOCKHEED MARTIN": "LMT", "LOCKHEED": "LMT",
    "RAYTHEON": "RTX", "NORTHROP GRUMMAN": "NOC",
    "GENERAL DYNAMICS": "GD", "L3HARRIS": "LHX",
    "INTEL": "INTC", "NVIDIA": "NVDA", "QUALCOMM": "QCOM",
    "BROADCOM": "AVGO", "TEXAS INSTRUMENTS": "TXN",
    "META": "META", "FACEBOOK": "META",
    "NETFLIX": "NFLX", "TESLA": "TSLA",
    "JPMORGAN": "JPM", "JP MORGAN": "JPM",
    "BANK OF AMERICA": "BAC", "WELLS FARGO": "WFC",
    "JOHNSON & JOHNSON": "JNJ", "J&J": "JNJ",
    "PFIZER": "PFE", "MERCK": "MRK", "ABBOTT": "ABT",
    "EXXON": "XOM", "EXXONMOBIL": "XOM", "CHEVRON": "CVX",
    "MCKESSON": "MCK", "AMERISOURCEBERGEN": "ABC", "CARDINAL HEALTH": "CAH",
    "UPS": "UPS", "FEDEX": "FDX", "DHL": None,
    "KROGER": "KR", "ALBERTSONS": "ACI",
}

FUZZY_THRESHOLD_ACCEPT = 85
FUZZY_THRESHOLD_REVIEW = 75
HIGH_MIN_SCORE = 95


def norm_gvkey(x):
    if pd.isna(x):
        return None
    if isinstance(x, float) and x == x:
        x = int(x)
    s = str(x).strip().lstrip("0") or "0"
    return s.zfill(6)


def normalize_name(s):
    if pd.isna(s) or not str(s).strip():
        return ""
    s = str(s).upper().strip()
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def main():
    try:
        from rapidfuzz import fuzz
        token_sort_ratio = fuzz.token_sort_ratio
    except ImportError:
        raise SystemExit("Install rapidfuzz: pip install rapidfuzz")

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # ----- STEP 1: Build matching corpus from CSV2 -----
    df2 = pd.read_csv(CSV2_PATH, low_memory=False)
    df2["datadate_dt"] = pd.to_datetime(df2["datadate"], errors="coerce")
    df2["gvkey_norm"] = df2["gvkey"].apply(norm_gvkey)
    latest = df2.sort_values("datadate_dt").groupby("gvkey_norm").last().reset_index()
    corpus = latest[["gvkey_norm", "tic", "conm"]].copy()
    corpus = corpus.rename(columns={"gvkey_norm": "gvkey", "conm": "canonical_name"})
    corpus["canonical_name"] = corpus["canonical_name"].fillna("").astype(str)
    corpus["tic"] = corpus["tic"].fillna("").astype(str)
    corpus = corpus.drop_duplicates(subset=["gvkey"]).reset_index(drop=True)
    # Alias table: (normalized_string, gvkey, tic) for conm and tic
    corpus_conm_norm = corpus.copy()
    corpus_conm_norm["norm"] = corpus_conm_norm["canonical_name"].apply(normalize_name)
    corpus_conm_norm = corpus_conm_norm[corpus_conm_norm["norm"] != ""]
    corpus_tic = corpus[corpus["tic"] != ""].copy()
    corpus_tic["norm"] = corpus_tic["tic"].str.upper().str.strip()
    tic_to_gvkey = df2.sort_values("datadate_dt").groupby("gvkey_norm").last().reset_index()[["gvkey_norm", "tic"]]
    tic_to_gvkey = tic_to_gvkey.rename(columns={"gvkey_norm": "gvkey"})
    tic_to_gvkey["tic"] = tic_to_gvkey["tic"].fillna("").astype(str)
    tic_to_gvkey = tic_to_gvkey[tic_to_gvkey["tic"] != ""]

    # ----- Load CSV1 and firm-like set -----
    df1 = pd.read_csv(CSV1_PATH, low_memory=False)
    df1["gvkey_norm"] = df1["gvkey"].apply(norm_gvkey)
    total_csv1_rows = len(df1)
    firm_like_set = set()
    if FIRM_LIKE_PATH.exists():
        firm_like_set = set(line.strip() for line in open(FIRM_LIKE_PATH, encoding="utf-8") if line.strip())
    df1["cnms_str"] = df1["cnms"].fillna("").astype(str)
    df1["cnms_norm"] = df1["cnms_str"].apply(normalize_name)
    rows_firm_like = df1["cnms_str"].isin(firm_like_set).sum() if firm_like_set else 0

    # Total salecs (for revenue-weighted stats)
    salecs_num = pd.to_numeric(df1["salecs"], errors="coerce")
    total_salecs_all = salecs_num.sum()

    # Unique cnms to resolve
    unique_cnms = df1[["cnms_str", "cnms_norm"]].drop_duplicates("cnms_str").reset_index(drop=True)

    # ----- STEP 2: MANUAL_MAP (exact, case-insensitive) -----
    manual_norm_to_tic = {normalize_name(k): v for k, v in MANUAL_MAP.items()}
    results = []
    manual_matched_cnms = set()
    for _, row in unique_cnms.iterrows():
        cnm_raw = row["cnms_str"]
        cnm_n = row["cnms_norm"]
        if cnm_n in manual_norm_to_tic:
            ticker = manual_norm_to_tic[cnm_n]
            manual_matched_cnms.add(cnm_raw)
            if ticker is None:
                results.append({
                    "cnms": cnm_raw,
                    "matched_ticker": None,
                    "matched_gvkey": None,
                    "match_source": "KNOWN_NON_PUBLIC",
                    "score": 100,
                })
            else:
                gv = tic_to_gvkey[tic_to_gvkey["tic"].str.upper() == ticker.upper()]
                gvkey_match = gv["gvkey"].iloc[0] if len(gv) else None
                results.append({
                    "cnms": cnm_raw,
                    "matched_ticker": ticker,
                    "matched_gvkey": gvkey_match,
                    "match_source": "MANUAL_MAP",
                    "score": 100,
                })

    # Unresolved = cnms not yet matched to a gvkey (exclude MANUAL_MAP with gvkey and KNOWN_NON_PUBLIC)
    has_match = {r["cnms"] for r in results if r.get("matched_gvkey") is not None}
    unresolved = unique_cnms[~unique_cnms["cnms_str"].isin(has_match)].reset_index(drop=True)

    # ----- STEP 3: Fuzzy match remaining -----
    all_norms_conm = list(corpus_conm_norm["norm"].unique())
    all_tic_norms = list(corpus_tic["norm"].unique())
    for _, row in unresolved.iterrows():
        cnm_raw = row["cnms_str"]
        cnm_n = row["cnms_norm"]
        if not cnm_n:
            results.append({"cnms": cnm_raw, "matched_ticker": None, "matched_gvkey": None, "match_source": "FUZZY", "score": 0})
            continue
        best_score = 0
        best_gvkey = None
        best_tic = None
        for cand in all_norms_conm:
            sc = token_sort_ratio(cnm_n, cand)
            if sc > best_score:
                best_score = sc
                rec = corpus_conm_norm[corpus_conm_norm["norm"] == cand].iloc[0]
                best_gvkey = rec["gvkey"]
                best_tic = rec["tic"]
        for cand in all_tic_norms:
            sc = token_sort_ratio(cnm_n, cand)
            if sc > best_score:
                best_score = sc
                rec = corpus_tic[corpus_tic["norm"] == cand].iloc[0]
                best_gvkey = rec["gvkey"]
                best_tic = rec["tic"]
        results.append({
            "cnms": cnm_raw,
            "matched_ticker": best_tic,
            "matched_gvkey": best_gvkey,
            "match_source": "FUZZY",
            "score": best_score,
        })

    # ----- STEP 4: Confidence bucketing -----
    def bucket(r):
        if r["match_source"] == "MANUAL_MAP" and r["matched_gvkey"] is not None:
            return "HIGH_CONFIDENCE"
        if r["match_source"] == "KNOWN_NON_PUBLIC":
            return "KNOWN_NON_PUBLIC"
        if r["match_source"] == "FUZZY":
            s = r["score"]
            if s >= HIGH_MIN_SCORE:
                return "HIGH_CONFIDENCE"
            if s >= FUZZY_THRESHOLD_ACCEPT:
                return "MEDIUM_CONFIDENCE"
            if s >= FUZZY_THRESHOLD_REVIEW:
                return "REVIEW_NEEDED"
            return "UNMATCHED"
        return "UNMATCHED"

    res_df = pd.DataFrame(results)
    res_df["confidence_bucket"] = res_df.apply(bucket, axis=1)
    # Only HIGH/MEDIUM/REVIEW have a usable match; UNMATCHED/KNOWN_NON_PUBLIC have no customer_gvkey for edges
    res_df["has_match"] = res_df["matched_gvkey"].notna() & res_df["confidence_bucket"].isin(["HIGH_CONFIDENCE", "MEDIUM_CONFIDENCE", "REVIEW_NEEDED"])

    # ----- STEP 5: Merge back to CSV1, remove self-loops, dedupe -----
    df1_merged = df1.merge(
        res_df[["cnms", "matched_ticker", "matched_gvkey", "confidence_bucket", "score", "has_match", "match_source"]],
        left_on="cnms_str",
        right_on="cnms",
        how="left",
        suffixes=("", "_cust"),
    )
    df1_merged = df1_merged.rename(columns={"matched_ticker": "customer_tic", "matched_gvkey": "customer_gvkey"})
    # Self-loops: supplier gvkey == customer gvkey
    self_loop = (df1_merged["gvkey_norm"] == df1_merged["customer_gvkey"]) & df1_merged["customer_gvkey"].notna()
    n_self_loops = self_loop.sum()
    df1_merged = df1_merged[~self_loop]
    # Keep only rows that have a match (for edge table)
    edges_raw = df1_merged[df1_merged["has_match"]].copy()
    edges_raw["supplier_gvkey"] = edges_raw["gvkey_norm"]
    edges_raw["supplier_tic"] = edges_raw["tic"]
    # Dedupe: (supplier_gvkey, customer_gvkey, srcdate) -> keep max salecs, else max score
    salecs_col = pd.to_numeric(edges_raw["salecs"], errors="coerce")
    edges_raw["salecs_num"] = salecs_col
    edges_raw["_score_num"] = edges_raw["score"].fillna(0)
    edges_raw = edges_raw.sort_values(["supplier_gvkey", "customer_gvkey", "srcdate", "salecs_num", "_score_num"], ascending=[True, True, True, False, False])
    edges_deduped = edges_raw.groupby(["supplier_gvkey", "customer_gvkey", "srcdate"], as_index=False).first()
    edges_deduped = edges_deduped.drop(columns=["_score_num"], errors="ignore")

    # ----- STEP 6: pct_revenue -----
    total_sales_per_supplier_date = df1.groupby(["gvkey_norm", "srcdate"])["salecs"].apply(
        lambda x: pd.to_numeric(x, errors="coerce").sum()
    ).reset_index()
    total_sales_per_supplier_date = total_sales_per_supplier_date.rename(columns={"salecs": "total_sales"})
    edges_deduped = edges_deduped.merge(
        total_sales_per_supplier_date,
        left_on=["supplier_gvkey", "srcdate"],
        right_on=["gvkey_norm", "srcdate"],
        how="left",
    )
    tot = edges_deduped["total_sales"]
    edges_deduped["pct_revenue"] = np.where(
        tot.gt(0),
        pd.to_numeric(edges_deduped["salecs"], errors="coerce") / tot,
        np.nan,
    )
    edges_deduped.loc[pd.to_numeric(edges_deduped["salecs"], errors="coerce").isna(), "pct_revenue"] = np.nan
    edges_deduped = edges_deduped.drop(columns=[c for c in ["gvkey_norm", "total_sales"] if c in edges_deduped.columns], errors="ignore")

    # signal_date = srcdate + 60 days
    edges_deduped["srcdate_dt"] = pd.to_datetime(edges_deduped["srcdate"], errors="coerce")
    edges_deduped["signal_date"] = (edges_deduped["srcdate_dt"] + pd.Timedelta(days=60)).dt.strftime("%Y-%m-%d")

    # ----- STEP 7: Outputs -----
    out_cols = [
        "supplier_gvkey", "supplier_tic", "customer_gvkey", "customer_tic",
        "cnms", "salecs", "pct_revenue", "srcdate", "signal_date",
        "confidence_bucket", "score",
    ]
    edges_out = edges_deduped[[c for c in out_cols if c in edges_deduped.columns]].copy()
    edges_path = OUT_DIR / "edges_path_a.csv"
    edges_out.to_csv(edges_path, index=False)
    print(f"Wrote {edges_path}")

    # Unmatched cnms: aggregate by cnms -> frequency, total_salecs
    unmatched_cnms_list = res_df[res_df["confidence_bucket"] == "UNMATCHED"]["cnms"].tolist()
    df1_unmatched = df1[df1["cnms_str"].isin(unmatched_cnms_list)]
    salecs_u = pd.to_numeric(df1_unmatched["salecs"], errors="coerce")
    agg = df1_unmatched.assign(salecs_num=salecs_u).groupby("cnms_str").agg(
        frequency=("salecs", "size"),
        total_salecs=("salecs_num", "sum"),
    ).reset_index().rename(columns={"cnms_str": "cnms"})
    agg = agg.sort_values("total_salecs", ascending=False)
    unmatched_path = OUT_DIR / "unmatched_cnms.csv"
    agg.to_csv(unmatched_path, index=False)
    print(f"Wrote {unmatched_path}")

    # ----- match_summary.txt -----
    manual_rows = (df1_merged["match_source"] == "MANUAL_MAP").sum()
    high = (df1_merged["confidence_bucket"] == "HIGH_CONFIDENCE").sum()
    medium = (df1_merged["confidence_bucket"] == "MEDIUM_CONFIDENCE").sum()
    review = (df1_merged["confidence_bucket"] == "REVIEW_NEEDED").sum()
    unmatched_rows = (df1_merged["confidence_bucket"] == "UNMATCHED").sum()
    high_salecs = df1_merged.loc[df1_merged["confidence_bucket"] == "HIGH_CONFIDENCE", "salecs"]
    high_salecs = pd.to_numeric(high_salecs, errors="coerce").sum()
    medium_salecs = df1_merged.loc[df1_merged["confidence_bucket"] == "MEDIUM_CONFIDENCE", "salecs"]
    medium_salecs = pd.to_numeric(medium_salecs, errors="coerce").sum()
    unmatched_salecs = df1_merged.loc[df1_merged["confidence_bucket"] == "UNMATCHED", "salecs"]
    unmatched_salecs = pd.to_numeric(unmatched_salecs, errors="coerce").sum()
    pct_high = (high_salecs / total_salecs_all * 100) if total_salecs_all else 0
    pct_medium = (medium_salecs / total_salecs_all * 100) if total_salecs_all else 0
    pct_unmatched = (unmatched_salecs / total_salecs_all * 100) if total_salecs_all else 0
    edges_high_medium = edges_deduped[edges_deduped["confidence_bucket"].isin(["HIGH_CONFIDENCE", "MEDIUM_CONFIDENCE"])]
    final_edge_count = len(edges_high_medium.drop_duplicates(["supplier_gvkey", "customer_gvkey", "srcdate"]))
    median_pct = edges_out["pct_revenue"].median() * 100 if "pct_revenue" in edges_out.columns else float("nan")
    pct_null_rev = (edges_out["pct_revenue"].isna().sum() / len(edges_out) * 100) if len(edges_out) else 0

    summary_lines = [
        "Match summary",
        "=============",
        f"Total CSV1 rows: {total_csv1_rows:,}",
        f"Rows with firm-like cnms (from firm_like_cnms_list.txt): {rows_firm_like:,}",
        f"MANUAL_MAP hits: {manual_rows:,} rows" + (f" ({manual_rows / rows_firm_like * 100:.2f}% of firm-like)" if rows_firm_like else ""),
        f"HIGH_CONFIDENCE matches: {high:,} rows, revenue-weighted coverage {pct_high:.2f}% of total salecs",
        f"MEDIUM_CONFIDENCE matches: {medium:,} rows, revenue-weighted coverage {pct_medium:.2f}%",
        f"REVIEW_NEEDED: {review:,} rows",
        f"UNMATCHED: {unmatched_rows:,} rows, {pct_unmatched:.2f}% of total salecs unresolved",
        f"Self-loops removed: {n_self_loops:,}",
        f"Final edge count (HIGH + MEDIUM only): {final_edge_count:,} unique (supplier, customer, year) triples",
        f"Median pct_revenue per edge: {median_pct:.2f}%",
        f"% of edges with null pct_revenue (salecs missing): {pct_null_rev:.2f}%",
    ]
    summary_path = OUT_DIR / "match_summary.txt"
    summary_path.write_text("\n".join(summary_lines), encoding="utf-8")
    print(f"Wrote {summary_path}")
    for line in summary_lines:
        print(line)


if __name__ == "__main__":
    main()
