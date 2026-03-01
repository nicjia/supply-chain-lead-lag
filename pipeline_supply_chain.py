#!/usr/bin/env python3
"""
Supply chain data pipeline: RDQ lookup → customer resolution → master edges → returns panel → summary.
All inputs from data/, outputs to data/processed/. Run stages 1–5 in order.

Outputs: rdq_lookup.csv, edges_path_a.csv, unmatched_cnms.csv, master_edges.csv, returns_panel.csv,
         transactions.csv (supplier, customer, amount, date), pipeline_summary.txt

Checkpointing: If an output file exists and data/processed/.pipeline_version matches PIPELINE_VERSION,
that stage is skipped. Bump PIPELINE_VERSION when any stage's output logic changes so old checkpoints
are invalidated and stages rerun.
"""
import re
import sys
import traceback
from pathlib import Path
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from rapidfuzz import fuzz
from tqdm import tqdm

BASE = Path(__file__).resolve().parent
DATA = BASE / "data"
PROCESSED = DATA / "processed"

# Bump this when any stage's output logic changes (so old checkpoints are ignored).
PIPELINE_VERSION = "3"

CSV1_PATH = DATA / "gqqatnce2nuvm7hl.csv"
CSV2_PATH = DATA / "hwihvjjrzgyby1vf.csv"
RDQ_PATH = DATA / "syaj3hxumfqnozvb.csv"
CCM_PATH = DATA / "psci31kycwth7rnj.csv"
FUND_PATH = DATA / "yiay0c9gvy7mzn7n.csv"
CRSP_PATH = DATA / "ucb4dk729hsfln0l.csv"
FIRM_LIKE_PATH = DATA / "firm_like_cnms_list.txt"
if not FIRM_LIKE_PATH.exists():
    FIRM_LIKE_PATH = BASE / "eda" / "firm_like_cnms_list.txt"

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
    "GENERAL MOTORS": "GM",
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
    "UPS": "UPS", "FEDEX": "FDX",
    "KROGER": "KR", "ALBERTSONS": "ACI",
    "DHL": None, "US GOVERNMENT": None, "DEPARTMENT OF DEFENSE": None,
    "US DEPARTMENT OF DEFENSE": None, "DOD": None,
}

LOG_LINES = []


def norm_gvkey(x):
    if pd.isna(x):
        return None
    if isinstance(x, float) and x == x:
        x = int(x)
    s = str(x).strip().lstrip("0") or "0"
    return s.zfill(6)


def plog(s=""):
    print(s)
    LOG_LINES.append(s)


def normalize_name(s):
    if pd.isna(s) or not str(s).strip():
        return ""
    s = str(s).upper().strip()
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def run_stage1():
    plog("\n" + "=" * 60)
    plog("STAGE 1 — RDQ LOOKUP")
    plog("=" * 60)
    df1 = pd.read_csv(CSV1_PATH, low_memory=False)
    df1["gvkey_norm"] = df1["gvkey"].apply(norm_gvkey)
    pairs = df1[["gvkey_norm", "srcdate"]].drop_duplicates()
    pairs = pairs.rename(columns={"gvkey_norm": "gvkey"})
    n_pairs = len(pairs)
    plog(f"Total unique (gvkey, srcdate) pairs: {n_pairs:,}")

    rdq = pd.read_csv(RDQ_PATH, low_memory=False)
    rdq["gvkey"] = rdq["gvkey"].apply(norm_gvkey)
    rdq["datadate_dt"] = pd.to_datetime(rdq["datadate"], errors="coerce")
    rdq["rdq_dt"] = pd.to_datetime(rdq["rdq"], errors="coerce")
    rdq = rdq.dropna(subset=["datadate_dt", "rdq_dt"])

    pairs["srcdate_dt"] = pd.to_datetime(pairs["srcdate"], errors="coerce")
    merged = pairs.merge(
        rdq[["gvkey", "datadate_dt", "rdq", "rdq_dt"]],
        left_on=["gvkey", "srcdate_dt"],
        right_on=["gvkey", "datadate_dt"],
        how="left",
    )
    unmatched_idx = merged["rdq"].isna()
    if unmatched_idx.any():
        unmerged = merged.loc[unmatched_idx, ["gvkey", "srcdate_dt"]].drop_duplicates()
        rdq_sub = rdq[["gvkey", "datadate_dt", "rdq", "rdq_dt"]].copy()
        fill_rdq = []
        for _, row in unmerged.iterrows():
            g, sd = row["gvkey"], row["srcdate_dt"]
            if pd.isna(sd):
                fill_rdq.append((g, sd, None, None))
                continue
            cand = rdq_sub[rdq_sub["gvkey"] == g].copy()
            cand["diff"] = (cand["datadate_dt"] - sd).dt.days.abs()
            cand = cand[cand["diff"] <= 7].sort_values("diff")
            if len(cand) > 0:
                b = cand.iloc[0]
                fill_rdq.append((g, sd, b["rdq"], b["rdq_dt"]))
            else:
                fill_rdq.append((g, sd, None, None))
        fill_df = pd.DataFrame(fill_rdq, columns=["gvkey", "srcdate_dt", "rdq_fill", "rdq_dt_fill"])
        merged = merged.merge(fill_df, on=["gvkey", "srcdate_dt"], how="left")
        merged["rdq"] = merged["rdq"].fillna(merged["rdq_fill"])
        merged["rdq_dt"] = merged["rdq_dt"].fillna(merged["rdq_dt_fill"])
        merged = merged.drop(columns=["rdq_fill", "rdq_dt_fill"], errors="ignore")

    merged["signal_date"] = merged["rdq_dt"]
    fallback = merged["rdq"].isna()
    merged.loc[fallback, "signal_date"] = merged.loc[fallback, "srcdate_dt"] + pd.Timedelta(days=75)
    merged["rdq_source"] = np.where(merged["rdq"].notna(), "RDQ_MATCHED", "FALLBACK")
    merged["signal_date"] = pd.to_datetime(merged["signal_date"]).dt.strftime("%Y-%m-%d")

    rdq_matched = (merged["rdq_source"] == "RDQ_MATCHED").sum()
    fallback_n = (merged["rdq_source"] == "FALLBACK").sum()
    plog(f"Matched via rdq: {rdq_matched:,} ({100 * rdq_matched / n_pairs:.1f}%)")
    plog(f"Using fallback: {fallback_n:,} ({100 * fallback_n / n_pairs:.1f}%)")

    mask = merged["rdq"].notna()
    rdq_dt = pd.to_datetime(merged.loc[mask, "rdq_dt"], errors="coerce")
    src_dt = pd.to_datetime(merged.loc[mask, "srcdate_dt"], errors="coerce")
    lag = (rdq_dt - src_dt).dt.days
    median_lag = lag.median() if len(lag) and lag.notna().any() else 0
    plog(f"Median filing lag (rdq - srcdate) for matched rows: {median_lag:.0f} days")

    err_rdq_lt_src = ((merged["rdq_dt"] < merged["srcdate_dt"]) & merged["rdq"].notna()).sum()
    plog(f"Rows with rdq < srcdate (errors): {err_rdq_lt_src:,}")

    out = merged[["gvkey", "rdq", "signal_date", "rdq_source"]].copy()
    out["srcdate"] = merged["srcdate"] if "srcdate" in merged.columns else merged["srcdate_dt"].astype(str)
    out = out[["gvkey", "srcdate", "rdq", "signal_date", "rdq_source"]]
    PROCESSED.mkdir(parents=True, exist_ok=True)
    out.to_csv(PROCESSED / "rdq_lookup.csv", index=False)
    plog(f"Saved rdq_lookup.csv")
    return out


def run_stage2(rdq_lookup):
    plog("\n" + "=" * 60)
    plog("STAGE 2 — CUSTOMER NAME RESOLUTION")
    plog("=" * 60)

    df1 = pd.read_csv(CSV1_PATH, low_memory=False)
    df1["gvkey"] = df1["gvkey"].apply(norm_gvkey)
    total_csv1_rows = len(df1)
    plog(f"Total CSV1 rows: {total_csv1_rows:,}")

    csv2 = pd.read_csv(CSV2_PATH, low_memory=False)
    csv2["gvkey"] = csv2["gvkey"].apply(norm_gvkey)
    csv2["datadate_dt"] = pd.to_datetime(csv2["datadate"], errors="coerce")
    latest = csv2.sort_values("datadate_dt").groupby("gvkey", as_index=False).last()
    corpus = latest[["gvkey", "tic", "conm"]].copy()
    corpus = corpus.rename(columns={"conm": "canonical_name"})
    corpus["normalized_name"] = corpus["canonical_name"].apply(normalize_name)
    corpus = corpus[corpus["normalized_name"].str.len() > 0].drop_duplicates(subset=["gvkey"])
    tic_aliases = latest[["gvkey", "tic"]].drop_duplicates()
    tic_aliases["normalized_name"] = tic_aliases["tic"].apply(normalize_name)
    tic_aliases["canonical_name"] = tic_aliases["tic"]
    corpus = pd.concat([corpus[["gvkey", "tic", "canonical_name", "normalized_name"]], tic_aliases[["gvkey", "tic", "canonical_name", "normalized_name"]]], ignore_index=True).drop_duplicates(subset=["gvkey", "normalized_name"], keep="first")

    if FIRM_LIKE_PATH.exists():
        firm_like = set()
        with open(FIRM_LIKE_PATH) as f:
            for line in f:
                s = normalize_name(line.strip())
                if s:
                    firm_like.add(s)
    else:
        firm_like = set(corpus["normalized_name"].dropna().unique())

    df1["cnms_norm"] = df1["cnms"].apply(normalize_name)
    firm_like_rows = df1[df1["cnms_norm"].isin(firm_like)]
    plog(f"Firm-like cnms rows: {len(firm_like_rows):,}")

    manual_hits = 0
    known_non_public = 0
    match_results = []
    manual_norm_map = {normalize_name(k): v for k, v in MANUAL_MAP.items()}
    gvkey_by_tic = corpus.drop_duplicates("tic").set_index("tic")["gvkey"].to_dict()
    for cnms_norm in firm_like_rows["cnms_norm"].unique():
        ticker = manual_norm_map.get(cnms_norm)
        if ticker is None:
            continue
        if pd.isna(ticker):
            known_non_public += firm_like_rows["cnms_norm"].eq(cnms_norm).sum()
            match_results.append({"cnms_norm": cnms_norm, "match_source": "KNOWN_NON_PUBLIC", "score": 100, "customer_gvkey": None, "customer_tic": None, "confidence_bucket": "KNOWN_NON_PUBLIC"})
            continue
        gvkey = gvkey_by_tic.get(ticker)
        manual_hits += firm_like_rows["cnms_norm"].eq(cnms_norm).sum()
        match_results.append({"cnms_norm": cnms_norm, "match_source": "MANUAL_MAP", "score": 100, "customer_gvkey": gvkey, "customer_tic": ticker, "confidence_bucket": "HIGH_CONFIDENCE"})

    plog(f"MANUAL_MAP hits: {manual_hits:,} rows")
    plog(f"KNOWN_NON_PUBLIC skipped: {known_non_public:,} rows")

    manual_resolved = {r["cnms_norm"] for r in match_results if r["match_source"] == "MANUAL_MAP"}
    known_non = {r["cnms_norm"] for r in match_results if r["match_source"] == "KNOWN_NON_PUBLIC"}
    to_fuzzy = [c for c in firm_like_rows["cnms_norm"].unique() if c and c not in manual_resolved and c not in known_non]

    corpus_names = corpus["normalized_name"].dropna().unique().tolist()
    corpus_df = corpus.drop_duplicates("gvkey").set_index("gvkey")
    vectorizer = TfidfVectorizer(analyzer="char_wb", ngram_range=(2, 4), max_features=50000)
    X_corpus = vectorizer.fit_transform(corpus["canonical_name"].fillna("").astype(str))
    name_to_gvkey = corpus.set_index("normalized_name")["gvkey"].to_dict()
    name_to_tic = corpus.set_index("normalized_name")["tic"].to_dict()

    chunk_size = 1000
    for i in tqdm(range(0, len(to_fuzzy), chunk_size), desc="Fuzzy match chunks"):
        chunk = to_fuzzy[i : i + chunk_size]
        X_q = vectorizer.transform(chunk)
        sim = cosine_similarity(X_q, X_corpus)
        for j, cnms_norm in enumerate(chunk):
            top_idx = np.argsort(sim[j])[-10:][::-1]
            best_score = 0
            best_gvkey = None
            best_tic = None
            for idx in top_idx:
                cand_name = corpus.iloc[idx]["normalized_name"]
                if pd.isna(cand_name):
                    continue
                sc = fuzz.token_sort_ratio(cnms_norm, str(cand_name))
                if sc > best_score:
                    best_score = sc
                    best_gvkey = corpus.iloc[idx]["gvkey"]
                    best_tic = corpus.iloc[idx]["tic"]
            if best_score >= 85:
                bucket = "HIGH_CONFIDENCE" if best_score >= 95 else "MEDIUM_CONFIDENCE"
                match_results.append({"cnms_norm": cnms_norm, "match_source": "FUZZY", "score": best_score, "customer_gvkey": best_gvkey, "customer_tic": best_tic, "confidence_bucket": bucket})
            elif best_score >= 75:
                match_results.append({"cnms_norm": cnms_norm, "match_source": "FUZZY", "score": best_score, "customer_gvkey": best_gvkey, "customer_tic": best_tic, "confidence_bucket": "REVIEW_NEEDED"})
            else:
                match_results.append({"cnms_norm": cnms_norm, "match_source": "UNMATCHED", "score": best_score or 0, "customer_gvkey": None, "customer_tic": None, "confidence_bucket": "UNMATCHED"})

    fund = pd.read_csv(FUND_PATH, low_memory=False)
    fund["gvkey"] = fund["gvkey"].apply(norm_gvkey)
    fund["datadate_dt"] = pd.to_datetime(fund["datadate"], errors="coerce")
    latest_sich = fund.sort_values("datadate_dt").groupby("gvkey", as_index=False).last()[["gvkey", "sich"]]
    gvkey_to_sich = latest_sich.set_index("gvkey")["sich"].to_dict()

    match_df = pd.DataFrame(match_results)
    df1_with_tic = df1.merge(latest[["gvkey", "tic"]].drop_duplicates("gvkey"), on="gvkey", how="left", suffixes=("", "_latest"))
    if "tic_latest" in df1_with_tic.columns:
        df1_with_tic["supplier_tic"] = df1_with_tic["tic_latest"].fillna(df1_with_tic["tic"]) if "tic" in df1_with_tic.columns else df1_with_tic["tic_latest"]
        df1_with_tic = df1_with_tic.drop(columns=["tic_latest"], errors="ignore")
    else:
        df1_with_tic = df1_with_tic.rename(columns={"tic": "supplier_tic"})
    merged = df1_with_tic.merge(match_df, left_on="cnms_norm", right_on="cnms_norm", how="left")
    merged.loc[merged["cnms_norm"].isin(firm_like) & merged["customer_gvkey"].isna() & (merged["match_source"] != "KNOWN_NON_PUBLIC"), "confidence_bucket"] = merged.loc[merged["cnms_norm"].isin(firm_like) & merged["customer_gvkey"].isna() & (merged["match_source"] != "KNOWN_NON_PUBLIC"), "confidence_bucket"].fillna("UNMATCHED")
    merged.loc[~merged["cnms_norm"].isin(firm_like), "confidence_bucket"] = "GEOGRAPHIC_OR_SEGMENT"
    self_loop_count = (merged["gvkey"] == merged["customer_gvkey"]).sum()
    merged = merged[merged["gvkey"] != merged["customer_gvkey"]]
    merged = merged.rename(columns={"gvkey": "supplier_gvkey"})
    supp_sich = merged["supplier_gvkey"].map(gvkey_to_sich)
    cust_sich = merged["customer_gvkey"].map(gvkey_to_sich)
    sic_ok = (merged["confidence_bucket"] == "MEDIUM_CONFIDENCE") | ((merged["confidence_bucket"] == "REVIEW_NEEDED") & (merged["score"] >= 85))
    low_score = merged["score"] < 95
    try:
        s_sich = pd.to_numeric(supp_sich, errors="coerce")
        c_sich = pd.to_numeric(cust_sich, errors="coerce")
        diff = (s_sich - c_sich).abs()
        sic_flag = diff > 500
        exempt = (s_sich // 1000 == 6) | (c_sich // 1000 == 6)
        downgrade = sic_ok & low_score & sic_flag & ~exempt
        merged.loc[downgrade, "confidence_bucket"] = "REVIEW_NEEDED"
    except Exception:
        pass
    dedupe_cols = ["supplier_gvkey", "customer_gvkey", "srcdate"]
    merged = merged.sort_values(["salecs", "score"], ascending=[False, False]).drop_duplicates(subset=[c for c in dedupe_cols if c in merged.columns], keep="first")

    supplier_total = merged.groupby(["supplier_gvkey", "srcdate"])["salecs"].transform("sum")
    merged["pct_revenue"] = merged["salecs"] / supplier_total.replace(0, np.nan)
    merged["pct_revenue_missing"] = merged["salecs"].isna()
    rl = rdq_lookup[["gvkey", "srcdate", "signal_date", "rdq_source"]].rename(columns={"gvkey": "rg", "srcdate": "rs"})
    merged = merged.merge(rl, left_on=["supplier_gvkey", "srcdate"], right_on=["rg", "rs"], how="left").drop(columns=["rg", "rs"], errors="ignore")
    merged["signal_date"] = merged["signal_date"].fillna(pd.to_datetime(merged["srcdate"], errors="coerce") + pd.Timedelta(days=75)).astype(str)
    merged["rdq_source"] = merged["rdq_source"].fillna("FALLBACK")
    plog(f"Self-loops removed: {self_loop_count:,}")
    edges = merged[merged["confidence_bucket"] != "KNOWN_NON_PUBLIC"].copy()
    out_cols = ["supplier_gvkey", "supplier_tic", "customer_gvkey", "customer_tic", "cnms", "salecs", "pct_revenue", "pct_revenue_missing", "srcdate", "signal_date", "rdq_source", "confidence_bucket", "score", "match_source"]
    edges = edges[[c for c in out_cols if c in edges.columns]]
    PROCESSED.mkdir(parents=True, exist_ok=True)
    edges.to_csv(PROCESSED / "edges_path_a.csv", index=False)
    unmatched_agg = merged[merged["confidence_bucket"].isin(["UNMATCHED", "GEOGRAPHIC_OR_SEGMENT", "REVIEW_NEEDED"])].groupby("cnms").agg(frequency=("cnms", "count"), total_salecs=("salecs", "sum")).reset_index().sort_values("total_salecs", ascending=False)
    unmatched_agg.to_csv(PROCESSED / "unmatched_cnms.csv", index=False)

    high = (edges["confidence_bucket"] == "HIGH_CONFIDENCE").sum()
    med = (edges["confidence_bucket"] == "MEDIUM_CONFIDENCE").sum()
    rev = (edges["confidence_bucket"] == "REVIEW_NEEDED").sum()
    unm = (edges["confidence_bucket"] == "UNMATCHED").sum()
    usable = edges[edges["confidence_bucket"].isin(["HIGH_CONFIDENCE", "MEDIUM_CONFIDENCE"])]
    plog(f"HIGH_CONFIDENCE edges: {high:,} ({100*high/len(firm_like_rows):.1f}% of firm-like rows)")
    plog(f"MEDIUM_CONFIDENCE edges: {med:,}")
    plog(f"REVIEW_NEEDED: {rev:,}")
    plog(f"UNMATCHED: {unm:,}")
    plog(f"Final usable edge count (HIGH + MEDIUM): {len(usable.drop_duplicates(['supplier_gvkey','customer_gvkey', 'srcdate'])):,} unique (supplier, customer, year)")
    plog(f"Revenue-weighted coverage (matched salecs / total salecs): {100 * usable['salecs'].sum() / df1['salecs'].sum():.1f}%")
    plog(f"Edges with null pct_revenue: {edges['pct_revenue'].isna().sum():,} ({100*edges['pct_revenue'].isna().mean():.1f}%)")
    return edges


def run_stage3(edges_path_a_path):
    plog("\n" + "=" * 60)
    plog("STAGE 3 — MASTER EDGE TABLE")
    plog("=" * 60)
    edges = pd.read_csv(edges_path_a_path, low_memory=False)
    edges = edges[edges["confidence_bucket"].isin(["HIGH_CONFIDENCE", "MEDIUM_CONFIDENCE"])]
    edges["supplier_gvkey"] = edges["supplier_gvkey"].apply(norm_gvkey)
    edges["customer_gvkey"] = edges["customer_gvkey"].apply(norm_gvkey)
    if "supplier_tic" not in edges.columns or edges["supplier_tic"].isna().all():
        csv2 = pd.read_csv(CSV2_PATH, low_memory=False)
        csv2["gvkey"] = csv2["gvkey"].apply(norm_gvkey)
        latest_tic = csv2.sort_values("datadate").groupby("gvkey", as_index=False).last()[["gvkey", "tic"]]
        gvkey_to_tic = latest_tic.set_index("gvkey")["tic"].to_dict()
        edges["supplier_tic"] = edges["supplier_gvkey"].map(gvkey_to_tic)
    plog(f"Total edges in: {len(edges):,}")

    ccm = pd.read_csv(CCM_PATH, low_memory=False)
    ccm["gvkey"] = ccm["gvkey"].apply(norm_gvkey)
    ccm = ccm[ccm["LINKTYPE"].isin(["LU", "LC"]) & ccm["LINKPRIM"].isin(["P", "C"])]
    ccm["LINKDT"] = pd.to_datetime(ccm["LINKDT"], errors="coerce")
    ccm["LINKENDDT"] = ccm["LINKENDDT"].astype(str).str.strip().str.upper()
    ccm.loc[ccm["LINKENDDT"].isin(["", "E", "NAN"]), "LINKENDDT"] = "2099-12-31"
    ccm["LINKENDDT"] = pd.to_datetime(ccm["LINKENDDT"], errors="coerce")
    ccm["LINKENDDT"] = ccm["LINKENDDT"].fillna(pd.Timestamp("2099-12-31"))

    def permno_as_of(gvkey_series, signal_date_series):
        out = []
        for gvkey, sd in zip(gvkey_series, signal_date_series):
            sd = pd.to_datetime(sd, errors="coerce")
            links = ccm[ccm["gvkey"] == gvkey]
            links = links[(links["LINKDT"] <= sd) & (links["LINKENDDT"] >= sd)]
            if links.empty:
                out.append(np.nan)
                continue
            prim = links[links["LINKPRIM"] == "P"]
            if len(prim) > 0:
                best = prim.sort_values("LINKDT", ascending=False).iloc[0]
            else:
                best = links.sort_values("LINKDT", ascending=False).iloc[0]
            out.append(int(best["LPERMNO"]) if pd.notna(best["LPERMNO"]) else np.nan)
        return out

    edges["signal_date_dt"] = pd.to_datetime(edges["signal_date"], errors="coerce")
    edges["supplier_permno"] = permno_as_of(edges["supplier_gvkey"], edges["signal_date_dt"])
    edges["customer_permno"] = permno_as_of(edges["customer_gvkey"], edges["signal_date_dt"])
    edges["permno_missing_supplier"] = edges["supplier_permno"].isna()
    edges["permno_missing_customer"] = edges["customer_permno"].isna()

    n_supp = edges["supplier_permno"].notna().sum()
    n_cust = edges["customer_permno"].notna().sum()
    n_both = ((edges["supplier_permno"].notna()) & (edges["customer_permno"].notna())).sum()
    plog(f"Edges with valid supplier permno: {n_supp:,} ({100*n_supp/len(edges):.1f}%)")
    plog(f"Edges with valid customer permno: {n_cust:,} ({100*n_cust/len(edges):.1f}%)")
    plog(f"Edges with BOTH permnos valid: {n_both:,} ({100*n_both/len(edges):.1f}%)")
    plog(f"Edges missing at least one permno: {len(edges) - n_both:,}")

    fund = pd.read_csv(FUND_PATH, low_memory=False)
    fund["gvkey"] = fund["gvkey"].apply(norm_gvkey)
    fund["datadate_dt"] = pd.to_datetime(fund["datadate"], errors="coerce")
    fund = fund.sort_values("datadate_dt")

    def latest_fund_as_of(gvkeys, signal_dates, cols):
        out = {c: [] for c in cols}
        for gvkey, sd in zip(gvkeys, signal_dates):
            sd = pd.to_datetime(sd, errors="coerce")
            f = fund[fund["gvkey"] == gvkey]
            f = f[f["datadate_dt"] <= sd]
            if f.empty:
                for c in cols:
                    out[c].append(np.nan)
                continue
            row = f.iloc[-1]
            for c in cols:
                out[c].append(row.get(c, np.nan))
        return out

    for col in ["sich", "gsector", "sale", "at"]:
        if col not in fund.columns:
            fund[col] = np.nan
    supp_f = latest_fund_as_of(edges["supplier_gvkey"], edges["signal_date_dt"], ["sich", "gsector", "sale", "at"])
    edges["supplier_sich"] = supp_f["sich"]
    edges["supplier_gsector"] = supp_f["gsector"]
    edges["supplier_sale"] = supp_f["sale"]
    edges["supplier_at"] = supp_f["at"]
    cust_f = latest_fund_as_of(edges["customer_gvkey"], edges["signal_date_dt"], ["sich", "gsector"])
    edges["customer_sich"] = cust_f["sich"]
    edges["customer_gsector"] = cust_f["gsector"]

    sector_counts = edges["supplier_gsector"].value_counts()
    plog("Sector breakdown of suppliers (gsector counts):")
    for s, c in sector_counts.items():
        plog(f"  {s}: {c:,}")
    edges["srcdate_dt"] = pd.to_datetime(edges["srcdate"], errors="coerce")
    plog("Year distribution of edges (by srcdate year):")
    for yr, c in edges["srcdate_dt"].dt.year.value_counts().sort_index().items():
        plog(f"  {yr}: {c:,}")

    out_cols = ["supplier_gvkey", "supplier_tic", "supplier_permno", "supplier_sich", "supplier_gsector", "supplier_sale", "supplier_at",
                "customer_gvkey", "customer_tic", "customer_permno", "customer_sich", "customer_gsector",
                "cnms", "salecs", "pct_revenue", "pct_revenue_missing", "srcdate", "signal_date", "rdq_source",
                "confidence_bucket", "score", "match_source", "permno_missing_supplier", "permno_missing_customer"]
    master = edges[[c for c in out_cols if c in edges.columns]]
    master.to_csv(PROCESSED / "master_edges.csv", index=False)
    plog("Saved master_edges.csv")
    return master


def run_stage4(master_edges_path):
    plog("\n" + "=" * 60)
    plog("STAGE 4 — RETURNS PANEL")
    plog("=" * 60)
    master = pd.read_csv(master_edges_path, low_memory=False)
    target_permnos = set(master["supplier_permno"].dropna().astype(int).tolist()) | set(master["customer_permno"].dropna().astype(int).tolist())
    plog(f"Target permnos (from master_edges): {len(target_permnos):,}")

    chunks = []
    for chunk in pd.read_csv(CRSP_PATH, chunksize=500_000, low_memory=False):
        chunk = chunk[chunk["PERMNO"].isin(target_permnos)]
        if len(chunk) > 0:
            chunks.append(chunk)
    if not chunks:
        panel = pd.DataFrame(columns=["PERMNO", "date", "PRC", "RET", "log_ret", "mktcap", "VOL", "is_delisting"])
    else:
        panel = pd.concat(chunks, ignore_index=True)
    panel["date"] = pd.to_datetime(panel["date"], errors="coerce")
    panel = panel.sort_values(["PERMNO", "date"])
    panel["RET"] = pd.to_numeric(panel["RET"], errors="coerce")
    panel.loc[panel["RET"].isin([-99.0, -999.0]), "RET"] = np.nan
    panel["is_delisting"] = (panel["RET"] == -88.0) | (panel["RET"] == -55.0)
    panel.loc[panel["RET"] == -88.0, "RET"] = -0.30
    panel.loc[panel["RET"] == -55.0, "RET"] = -0.55
    panel["log_ret"] = np.nan
    panel.loc[panel["RET"].notna(), "log_ret"] = np.log(1 + panel.loc[panel["RET"].notna(), "RET"])
    if "SHROUT" in panel.columns:
        panel["mktcap"] = panel["PRC"].abs() * panel["SHROUT"]
    else:
        panel["mktcap"] = np.nan
    panel = panel[["PERMNO", "date", "PRC", "RET", "log_ret", "mktcap", "VOL", "is_delisting"]]
    found = panel["PERMNO"].nunique()
    missing = target_permnos - set(panel["PERMNO"].astype(int))
    plog(f"Permnos found in CRSP: {found:,} ({100*found/len(target_permnos):.1f}%)")
    if len(missing) < 20:
        plog(f"Missing permnos (in edges but not CRSP): {sorted(missing)}")
    else:
        plog(f"Missing permnos (in edges but not CRSP): {len(missing):,}")
    plog(f"Date range of panel: {panel['date'].min()} to {panel['date'].max()}")
    plog(f"Total rows in returns_panel: {len(panel):,}")
    plog(f"Delisting events: {panel['is_delisting'].sum():,}")
    panel.to_csv(PROCESSED / "returns_panel.csv", index=False)
    plog("Saved returns_panel.csv")
    return panel


def run_stage5(edges_path_a_path, master_edges_path, returns_panel_path):
    plog("\n" + "=" * 60)
    plog("STAGE 5 — PIPELINE SUMMARY")
    plog("=" * 60)
    master = pd.read_csv(master_edges_path, low_memory=False)
    panel = pd.read_csv(returns_panel_path, low_memory=False)
    edges = pd.read_csv(edges_path_a_path, low_memory=False)
    edges = edges[edges["confidence_bucket"].isin(["HIGH_CONFIDENCE", "MEDIUM_CONFIDENCE"])]
    panel_permnos = set(panel["PERMNO"].astype(int))
    master["supplier_permno"] = pd.to_numeric(master["supplier_permno"], errors="coerce")
    master["customer_permno"] = pd.to_numeric(master["customer_permno"], errors="coerce")
    both_valid = master["supplier_permno"].notna() & master["customer_permno"].notna()
    has_ret = master["supplier_permno"].isin(panel_permnos) & master["customer_permno"].isin(panel_permnos)
    analysis_ready = (both_valid & has_ret).sum()
    univ_supp = master.loc[both_valid & has_ret, "supplier_gvkey"].nunique()
    univ_cust = master.loc[both_valid & has_ret, "customer_gvkey"].nunique()
    pairs = master.loc[both_valid & has_ret, ["supplier_gvkey", "customer_gvkey"]].drop_duplicates()
    plog("FINAL ANALYSIS-READY UNIVERSE:")
    plog(f"  Edges with both permnos valid AND returns data available: {analysis_ready:,}")
    plog(f"  Unique suppliers in universe: {univ_supp:,}")
    plog(f"  Unique customers in universe: {univ_cust:,}")
    plog(f"  Unique (supplier, customer) pairs: {len(pairs):,}")
    plog(f"  Date range of edges: {master['signal_date'].min()} to {master['signal_date'].max()}")
    csv1 = pd.read_csv(CSV1_PATH, low_memory=False)
    total_salecs = csv1["salecs"].sum()
    matched_salecs = edges["salecs"].sum()
    plog(f"  Revenue-weighted coverage of original CSV1 salecs: {100*matched_salecs/total_salecs:.1f}%")
    plog("DATA QUALITY FLAGS:")
    fallback = (master["rdq_source"] == "FALLBACK").sum()
    plog(f"  Edges using fallback signal_date (not true rdq): {fallback:,} ({100*fallback/len(master):.1f}%)")
    null_rev = master["pct_revenue"].isna().sum()
    plog(f"  Edges with null pct_revenue: {null_rev:,} ({100*null_rev/len(master):.1f}%)")
    med = (master["confidence_bucket"] == "MEDIUM_CONFIDENCE").sum()
    plog(f"  MEDIUM_CONFIDENCE edges (lower trust): {med:,} ({100*med/len(master):.1f}%)")
    plog("TOP 10 CUSTOMERS BY EDGE FREQUENCY:")
    cust_cols = [c for c in ["customer_tic", "customer_gvkey"] if c in master.columns]
    if cust_cols:
        cust_counts = master.groupby(cust_cols).agg(edge_count=("customer_gvkey", "count"), total_salecs=("salecs", "sum")).reset_index().sort_values("edge_count", ascending=False).head(10)
        for _, r in cust_counts.iterrows():
            parts = [str(r.get(c, "")) for c in cust_cols] + [f"edge_count={r['edge_count']}", f"total_salecs={r['total_salecs']:.2f}"]
            plog("  " + ", ".join(parts))
    plog("TOP 10 SUPPLIERS BY TOTAL DISCLOSED REVENUE:")
    supp_cols = [c for c in ["supplier_tic", "supplier_gvkey"] if c in master.columns]
    if supp_cols:
        supp_tot = master.groupby(supp_cols).agg(total_salecs=("salecs", "sum"), num_customers=("customer_gvkey", "nunique")).reset_index().sort_values("total_salecs", ascending=False).head(10)
        for _, r in supp_tot.iterrows():
            parts = [str(r.get(c, "")) for c in supp_cols] + [f"total_salecs={r['total_salecs']:.2f}", f"num_customers={r['num_customers']}"]
            plog("  " + ", ".join(parts))

    # Transactions CSV: supplier, customer, amount, date for all analysis-ready edges
    mask_ready = both_valid & has_ret
    trans = master.loc[mask_ready].copy()
    trans = trans.rename(columns={"salecs": "amount", "signal_date": "date"})
    trans_cols = ["supplier_gvkey", "supplier_tic", "customer_gvkey", "customer_tic", "amount", "date"]
    if "supplier_tic" not in trans.columns:
        trans["supplier_tic"] = trans["supplier_gvkey"].astype(str)
    if "customer_tic" not in trans.columns:
        trans["customer_tic"] = trans["customer_gvkey"].astype(str)
    trans_out = trans[[c for c in trans_cols if c in trans.columns]]
    if "srcdate" in trans.columns:
        trans_out["srcdate"] = trans["srcdate"]
    trans_out.to_csv(PROCESSED / "transactions.csv", index=False)
    plog(f"Saved transactions.csv: {len(trans_out):,} rows (supplier, customer, amount, date)")

    PROCESSED.mkdir(parents=True, exist_ok=True)
    with open(PROCESSED / "pipeline_summary.txt", "w") as f:
        f.write("\n".join(LOG_LINES))
    plog("Saved pipeline_summary.txt")


def main():
    PROCESSED.mkdir(parents=True, exist_ok=True)
    plog("FILE MANIFEST (input sizes)")
    plog("=" * 60)
    for name, path in [
        ("CSV1 segments", CSV1_PATH),
        ("CSV2 company index", CSV2_PATH),
        ("RDQ", RDQ_PATH),
        ("CCM link", CCM_PATH),
        ("Fundamentals", FUND_PATH),
    ]:
        if path.exists():
            df = pd.read_csv(path, nrows=0)
            nrows = sum(1 for _ in open(path, "rb")) - 1
            plog(f"  {name}: {nrows:,} x {len(df.columns)}")
        else:
            plog(f"  {name}: NOT FOUND")
    if CRSP_PATH.exists():
        nrows = sum(1 for _ in open(CRSP_PATH, "rb")) - 1
        df = pd.read_csv(CRSP_PATH, nrows=0)
        plog(f"  CRSP daily: {nrows:,} x {len(df.columns)}")
    else:
        plog(f"  CRSP daily: NOT FOUND")
    if FIRM_LIKE_PATH.exists():
        n = sum(1 for _ in open(FIRM_LIKE_PATH))
        plog(f"  firm_like_cnms_list.txt: {n:,} lines")
    else:
        plog(f"  firm_like_cnms_list.txt: NOT FOUND")
    plog("")

    # Checkpoint paths: skip a stage only if its output exists AND was produced by current pipeline version
    RDQ_PATH_OUT = PROCESSED / "rdq_lookup.csv"
    EDGES_PATH_OUT = PROCESSED / "edges_path_a.csv"
    MASTER_PATH_OUT = PROCESSED / "master_edges.csv"
    RETURNS_PATH_OUT = PROCESSED / "returns_panel.csv"
    VERSION_FILE = PROCESSED / ".pipeline_version"

    def checkpoint_valid():
        if not VERSION_FILE.exists():
            return False
        try:
            return VERSION_FILE.read_text().strip() == PIPELINE_VERSION
        except Exception:
            return False

    if any(p.exists() for p in [RDQ_PATH_OUT, EDGES_PATH_OUT, MASTER_PATH_OUT, RETURNS_PATH_OUT]) and not checkpoint_valid():
        plog(f"Checkpoint version mismatch or missing (current={PIPELINE_VERSION}) — ignoring existing outputs, will rerun all stages")
        for p in [RDQ_PATH_OUT, EDGES_PATH_OUT, MASTER_PATH_OUT, RETURNS_PATH_OUT]:
            if p.exists():
                p.unlink()
                plog(f"  Removed {p.name}")

    rdq_lookup = None

    # Stage 1
    if RDQ_PATH_OUT.exists():
        plog("Checkpoint: rdq_lookup.csv exists — skipping Stage 1")
        rdq_lookup = pd.read_csv(RDQ_PATH_OUT, low_memory=False)
    else:
        try:
            rdq_lookup = run_stage1()
        except Exception as e:
            plog(f"Stage 1 error: {e}")
            traceback.print_exc()
            rdq_lookup = pd.read_csv(RDQ_PATH_OUT, low_memory=False) if RDQ_PATH_OUT.exists() else None

    # Stage 2
    if EDGES_PATH_OUT.exists():
        plog("Checkpoint: edges_path_a.csv exists — skipping Stage 2")
    elif rdq_lookup is not None and len(rdq_lookup) > 0:
        try:
            run_stage2(rdq_lookup)
        except Exception as e:
            plog(f"Stage 2 error: {e}")
            traceback.print_exc()
    else:
        plog("Skipping Stage 2 (no rdq_lookup)")

    # Stage 3
    if MASTER_PATH_OUT.exists():
        plog("Checkpoint: master_edges.csv exists — skipping Stage 3")
    elif EDGES_PATH_OUT.exists():
        try:
            run_stage3(EDGES_PATH_OUT)
        except Exception as e:
            plog(f"Stage 3 error: {e}")
            traceback.print_exc()
    else:
        plog("Skipping Stage 3 (no edges_path_a.csv)")

    # Stage 4
    if RETURNS_PATH_OUT.exists():
        plog("Checkpoint: returns_panel.csv exists — skipping Stage 4")
    elif MASTER_PATH_OUT.exists():
        try:
            run_stage4(MASTER_PATH_OUT)
        except Exception as e:
            plog(f"Stage 4 error: {e}")
            traceback.print_exc()
    else:
        plog("Skipping Stage 4 (no master_edges.csv)")

    # Stage 5: always run when inputs exist (writes pipeline_summary.txt and transactions.csv)
    ran_stage5 = False
    if EDGES_PATH_OUT.exists() and MASTER_PATH_OUT.exists() and RETURNS_PATH_OUT.exists():
        try:
            run_stage5(EDGES_PATH_OUT, MASTER_PATH_OUT, RETURNS_PATH_OUT)
            ran_stage5 = True
        except Exception as e:
            plog(f"Stage 5 error: {e}")
            traceback.print_exc()
    else:
        plog("Skipping Stage 5 (missing edges_path_a.csv, master_edges.csv, or returns_panel.csv)")

    with open(PROCESSED / "pipeline_summary.txt", "w") as f:
        f.write("\n".join(LOG_LINES))
    # Only stamp version when Stage 5 ran, so checkpoints are only trusted when outputs are complete and current
    if ran_stage5:
        VERSION_FILE.write_text(PIPELINE_VERSION + "\n")
    plog("\nPipeline finished.")


if __name__ == "__main__":
    main()
