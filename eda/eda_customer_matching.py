#!/usr/bin/env python3
"""
Customer-matching EDA: firm-like cnms, known-customer variants, overlap with conm,
cgvkey resolution, and hardest-to-match list. Outputs EDA_CUSTOMER_MATCHING_REPORT.md.
"""
import pandas as pd
import re
from pathlib import Path

BASE = Path(__file__).resolve().parent
DATA = BASE.parent / "data"
CSV1 = DATA / "gqqatnce2nuvm7hl.csv"
CSV2 = DATA / "hwihvjjrzgyby1vf.csv"
OUT = BASE / "EDA_CUSTOMER_MATCHING_REPORT.md"

# Exclude cnms containing any of these (case insensitive) for "firm-like" list
NON_FIRM_KEYWORDS = [
    "government", "federal", "defense", "foreign", "international", "domestic",
    "commercial", "other", "corporate", "unallocated", "elimination", "geographic",
    "department", "americas", "europe", "asia", "pacific",
]

# Known large customers: search terms for substring match in cnms
KNOWN_CUSTOMERS = [
    "Apple", "Microsoft", "Amazon", "Google", "Alphabet", "Walmart", "Dell",
    "HP", "Hewlett-Packard", "Ford", "GM", "General Motors", "Boeing",
    "AT&T", "Verizon", "Home Depot", "Target", "Costco",
]

def pct(x, total):
    return (x / total * 100) if total else 0

def is_non_firm(cnm):
    if pd.isna(cnm) or not str(cnm).strip():
        return True
    lower = str(cnm).lower()
    return any(k in lower for k in NON_FIRM_KEYWORDS)

def run():
    lines = []
    def w(s=""):
        lines.append(s)

    w("# Customer matching EDA — CSV1 & CSV2")
    w("")
    df1 = pd.read_csv(CSV1, low_memory=False)
    df2 = pd.read_csv(CSV2, low_memory=False)
    n1 = len(df1)

    # Build sets for overlap
    conm_set = set(df2["conm"].dropna().astype(str).str.strip().unique())
    conm_set.discard("")

    # ----- 1. Unique cnms that are NOT obviously non-firm -----
    w("## 1. Unique firm-like cnms (excluded: government, federal, defense, foreign, ...)")
    w("")
    all_cnms = df1["cnms"].dropna().astype(str).str.strip()
    all_cnms = all_cnms[all_cnms != ""].unique()
    firm_like = sorted([c for c in all_cnms if not is_non_firm(c)])
    w(f"**Count:** {len(firm_like):,} unique cnms (out of {len(all_cnms):,} total unique cnms).")
    w("")
    # Save full list to file; show first 500 in report
    firm_like_file = BASE / "firm_like_cnms_list.txt"
    firm_like_file.write_text("\n".join(firm_like), encoding="utf-8")
    w(f"Full list saved to `{firm_like_file.name}` ({len(firm_like):,} entries).")
    w("")
    w("<details><summary>First 500 firm-like cnms (click to expand)</summary>")
    w("")
    w("```")
    for c in firm_like[:500]:
        w(c)
    w("```")
    w("</details>")
    w("")

    # ----- 2. Known large customers: distinct cnms variants -----
    w("## 2. Known large customers — cnms variants in CSV1")
    w("")
    w("| Customer search | # distinct cnms | Variants (all matching cnms) |")
    w("|-----------------|------------------|------------------------------|")
    for label in KNOWN_CUSTOMERS:
        # Word boundary to avoid e.g. Ford matching Ashford, GM matching GmbH
        pat = r"\b" + re.escape(label) + r"\b"
        regex = re.compile(pat, re.I)
        matching = [c for c in all_cnms if regex.search(c)]
        matching = sorted(set(matching))
        n_var = len(matching)
        variants_str = "; ".join(matching) if matching else "—"
        if len(variants_str) > 200:
            variants_str = variants_str[:197] + "..."
        w(f"| {label} | {n_var} | {variants_str} |")
    w("")

    # ----- 3. Overlap: cnms exactly in conm -----
    w("## 3. Overlap: cnms that exactly match a conm in CSV2")
    w("")
    cnms_in_conm = [c for c in all_cnms if c in conm_set]
    rows_cnms_in_conm = df1["cnms"].astype(str).str.strip().isin(conm_set).sum()
    w(f"- **Unique cnms that exactly match some conm:** {len(cnms_in_conm):,}")
    w(f"- **Rows in CSV1 with such a cnms:** {rows_cnms_in_conm:,} ({pct(rows_cnms_in_conm, n1):.2f}%)")
    w("")

    # ----- 4. cgvkey -----
    w("## 4. cgvkey column and resolution to ticker")
    w("")
    if "cgvkey" not in df1.columns:
        w("**cgvkey** is **not** present in CSV1. No resolution to ticker possible from this column.")
    else:
        cgv_valid = df1["cgvkey"].notna() & (df1["cgvkey"].astype(str).str.strip() != "")
        n_cgv = cgv_valid.sum()
        w(f"- **% of firm-customer rows with cgvkey populated:** {pct(n_cgv, n1):.2f}% ({n_cgv:,} rows)")
        # CSV2: gvkey -> tic. Normalize gvkey to 6-char string for Compustat-style join.
        def norm_gvkey(x):
            if pd.isna(x): return None
            s = str(x).strip().lstrip("0") or "0"
            return s.zfill(6)
        gvkey_tic = df2.dropna(subset=["gvkey", "tic"]).drop_duplicates("gvkey")[["gvkey", "tic"]]
        gvkey_tic["gvkey_norm"] = gvkey_tic["gvkey"].apply(norm_gvkey)
        gvkey_to_tic = gvkey_tic.set_index("gvkey_norm")["tic"]
        df1_cgv = df1.loc[cgv_valid].copy()
        df1_cgv["cgvkey_norm"] = df1_cgv["cgvkey"].apply(norm_gvkey)
        df1_cgv["customer_tic"] = df1_cgv["cgvkey_norm"].map(gvkey_to_tic)
        resolvable = df1_cgv["customer_tic"].notna()
        unique_customer_tickers = df1_cgv.loc[resolvable, "customer_tic"].nunique()
        w(f"- **Unique customer tickers resolvable via cgvkey → CSV2 (no fuzzy matching):** {unique_customer_tickers:,}")
    w("")

    # ----- 5. Hardest to match: top 50 cnms (no exact conm match) by frequency × avg salecs -----
    w("## 5. Top 50 hardest-to-match cnms (no exact conm match; ranked by frequency × avg salecs)")
    w("")
    salecs_num = pd.to_numeric(df1["salecs"], errors="coerce")
    df1["salecs_num"] = salecs_num
    no_match = [c for c in all_cnms if c not in conm_set]
    # For each cnms in no_match: count rows, mean salecs (use 0 for null when computing score, or only non-null?)
    # "frequency × avg salecs" -> use avg of non-null salecs per cnms; for nulls we could treat as 0 so avg is mean of non-null only.
    rows_no_match = df1[df1["cnms"].astype(str).str.strip().isin(no_match)].copy()
    rows_no_match["cnms_norm"] = rows_no_match["cnms"].astype(str).str.strip()
    agg = rows_no_match.groupby("cnms_norm").agg(
        freq=("salecs", "size"),
        avg_salecs=("salecs_num", "mean"),
    ).reset_index()
    agg = agg.rename(columns={"cnms_norm": "cnms"})
    agg["avg_salecs"] = agg["avg_salecs"].fillna(0)
    agg["score"] = agg["freq"] * agg["avg_salecs"]
    top50 = agg.nlargest(50, "score")[["cnms", "freq", "avg_salecs", "score"]]
    w("| Rank | cnms | freq | avg_salecs | score (freq × avg_salecs) |")
    w("|------|------|------|------------|---------------------------|")
    for i, row in enumerate(top50.itertuples(index=False), 1):
        w(f"| {i} | {repr(row.cnms)[:70]} | {row.freq:,} | {row.avg_salecs:,.2f} | {row.score:,.2f} |")
    w("")

    report = "\n".join(lines)
    OUT.write_text(report, encoding="utf-8")
    print(f"Report written to {OUT}")
    return report

if __name__ == "__main__":
    run()
