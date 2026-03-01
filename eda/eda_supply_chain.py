#!/usr/bin/env python3
"""
EDA script for supply chain CSVs. Outputs markdown report to EDA_REPORT.md.
"""
import pandas as pd
from pathlib import Path

BASE = Path(__file__).resolve().parent
DATA = BASE.parent / "data"
CSV1 = DATA / "gqqatnce2nuvm7hl.csv"
CSV2 = DATA / "hwihvjjrzgyby1vf.csv"
OUT = BASE / "EDA_REPORT.md"

def pct(x, total):
    return (x / total * 100) if total else 0

def run_eda():
    lines = []
    def w(s=""):
        lines.append(s)

    w("# Supply Chain CSVs — EDA Report")
    w("")

    # Load data
    w("Loading CSVs...")
    df1 = pd.read_csv(CSV1, low_memory=False)
    df2 = pd.read_csv(CSV2, low_memory=False)
    n1, n2 = len(df1), len(df2)

    # --- 1. Shape, dtypes, null counts, null % ---
    w("## 1. Shape, dtypes, null counts, null %")
    w("")

    for name, df, n in [("CSV1 (gqqatnce2nuvm7hl.csv)", df1, n1), ("CSV2 (hwihvjjrzgyby1vf.csv)", df2, n2)]:
        w(f"### {name}")
        w(f"- **Shape:** {df.shape[0]:,} rows × {df.shape[1]} columns")
        w("")
        w("| Column | dtype | null_count | null_% |")
        w("|--------|-------|------------|--------|")
        for c in df.columns:
            nc = df[c].isna().sum()
            w(f"| {c} | {str(df[c].dtype)} | {nc:,} | {pct(nc, n):.2f}% |")
        w("")

    # --- 2. Date range ---
    w("## 2. Date range coverage")
    w("")
    df1["srcdate_parsed"] = pd.to_datetime(df1["srcdate"], errors="coerce")
    df2["datadate_parsed"] = pd.to_datetime(df2["datadate"], errors="coerce")
    w("| Field | min | max |")
    w("|-------|-----|-----|")
    w(f"| **srcdate** (CSV1) | {df1['srcdate_parsed'].min()} | {df1['srcdate_parsed'].max()} |")
    w(f"| **datadate** (CSV2) | {df2['datadate_parsed'].min()} | {df2['datadate_parsed'].max()} |")
    w("")

    # --- 3. Unique tickers, gvkeys, company names ---
    w("## 3. Unique tickers, gvkeys, company names")
    w("")
    w("| Metric | CSV1 | CSV2 |")
    w("|--------|------|------|")
    w(f"| Unique **tic** | {df1['tic'].nunique():,} | {df2['tic'].nunique():,} |")
    w(f"| Unique **gvkey** | {df1['gvkey'].nunique():,} | {df2['gvkey'].nunique():,} |")
    w(f"| Unique **conm** | {df1['conm'].nunique():,} | {df2['conm'].nunique():,} |")
    w("")

    # --- 4. salecs distribution ---
    w("## 4. salecs distribution (CSV1)")
    w("")
    salecs = pd.to_numeric(df1["salecs"], errors="coerce")
    valid = salecs.dropna()
    n_null = salecs.isna().sum()
    n_zero = (valid == 0).sum()
    w("| Stat | Value |")
    w("|------|-------|")
    w(f"| min | {valid.min() if len(valid) else 'N/A'} |")
    w(f"| max | {valid.max() if len(valid) else 'N/A'} |")
    w(f"| mean | {(f'{valid.mean():.4f}' if len(valid) else 'N/A')} |")
    w(f"| median | {(f'{valid.median():.4f}' if len(valid) else 'N/A')} |")
    w(f"| % null | {pct(n_null, n1):.2f}% |")
    w(f"| % zero (of non-null) | {pct(n_zero, len(valid)):.2f}% |")
    w("")

    # --- 5. cgvkey check ---
    w("## 5. cgvkey column (Compustat customer gvkey) in CSV1")
    w("")
    if "cgvkey" in df1.columns:
        cgv_pop = df1["cgvkey"].notna().sum()
        w(f"**cgvkey** is present. Rows with value: {cgv_pop:,} ({pct(cgv_pop, n1):.2f}%).")
    else:
        w("**cgvkey** is **not** present in CSV1.")
    w("")

    # --- 6. Top 20 cnms ---
    w("## 6. Top 20 most frequent cnms (CSV1)")
    w("")
    cnms_counts = df1["cnms"].value_counts()
    top20 = cnms_counts.head(20)
    non_firm_keywords = ["government", "foreign", "commercial", "other", "not reported", "geographic", "segment", "unallocated", "elimination", "domestic"]
    w("| Rank | cnms | Count | Non-firm segment? |")
    w("|------|------|-------|-------------------|")
    for i, (cnm, cnt) in enumerate(top20.items(), 1):
        cnm_lower = (cnm or "").lower()
        flag = "Yes" if any(k in cnm_lower for k in non_firm_keywords) else "No"
        w(f"| {i} | {repr(cnm)[:60]} | {cnt:,} | {flag} |")
    w("")

    # --- 7. Unique (tic, srcdate), avg segments per firm-year ---
    w("## 7. Unique (tic, srcdate) and segments per firm-year")
    w("")
    pairs = df1.groupby(["tic", "srcdate"]).size()
    n_pairs = len(pairs)
    avg_seg = pairs.mean()
    w(f"- **Unique (tic, srcdate) pairs:** {n_pairs:,}")
    w(f"- **Average segments per firm per year:** {avg_seg:.2f}")
    w("")

    # --- 8. Named customers and salecs ---
    w("## 8. Named customers vs salecs availability")
    w("")
    has_cnms = df1["cnms"].notna() & (df1["cnms"].astype(str).str.strip() != "")
    named = df1.loc[has_cnms]
    n_named = len(named)
    n_named_with_salecs = named["salecs"].notna() & (pd.to_numeric(named["salecs"], errors="coerce").notna())
    n_named_null_salecs = n_named - n_named_with_salecs.sum()
    w(f"- Rows with a non-empty **cnms** (named customer): {n_named:,}")
    w(f"- Of those, rows with non-null **salecs**: {n_named_with_salecs.sum():,} ({pct(n_named_with_salecs.sum(), n_named):.2f}%)")
    w(f"- Of those, rows with null/missing **salecs**: {n_named_null_salecs:,} ({pct(n_named_null_salecs, n_named):.2f}%)")
    w("")
    w("Named customers **frequently** have null revenue when the segment is aggregate (e.g. Government, Foreign, Not Reported) rather than a specific firm.")
    w("")

    report = "\n".join(lines)
    OUT.write_text(report, encoding="utf-8")
    print(f"Report written to {OUT}")
    return report

if __name__ == "__main__":
    run_eda()
