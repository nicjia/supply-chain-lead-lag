#!/usr/bin/env python3
"""
EDA: srcdate distribution, fiscal year-end, sample ticker time series,
look-ahead bias, and implausible dates. Uses CSV1 only.
Outputs EDA_SRCDATE_REPORT.md and srcdate_histogram_by_year.png.
"""
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

BASE = Path(__file__).resolve().parent
DATA = BASE.parent / "data"
CSV1 = DATA / "gqqatnce2nuvm7hl.csv"
OUT_MD = BASE / "EDA_SRCDATE_REPORT.md"
OUT_PNG = BASE / "srcdate_histogram_by_year.png"

SAMPLE_TICKERS = ["AAPL", "MSFT", "WMT", "F", "BA"]
FILING_LAG_DAYS = 60
CUTOFF_PRE_1993 = "1993-01-01"
REFERENCE_DATE = "2026-02-24"  # report date for "future" check

def pct(x, total):
    return (x / total * 100) if total else 0

def run():
    lines = []
    def w(s=""):
        lines.append(s)

    df = pd.read_csv(CSV1, low_memory=False)
    df["srcdate_dt"] = pd.to_datetime(df["srcdate"], errors="coerce")
    n = len(df)

    w("# srcdate & fiscal year EDA (CSV1)")
    w("")

    # --- 1. Histogram by year + month distribution ---
    w("## 1. srcdate distribution by year")
    w("")
    valid = df["srcdate_dt"].dropna()
    years = valid.dt.year
    year_counts = years.value_counts().sort_index()
    w("Count of rows by year:")
    w("")
    w("| Year | Row count |")
    w("|------|-----------|")
    for yr, cnt in year_counts.items():
        w(f"| {yr} | {cnt:,} |")
    w("")

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        fig, ax = plt.subplots(figsize=(10, 4))
        ax.bar(year_counts.index.astype(int), year_counts.values, color="steelblue", edgecolor="navy", alpha=0.8)
        ax.set_xlabel("Year (srcdate)")
        ax.set_ylabel("Number of rows")
        ax.set_title("Histogram of srcdate by year")
        fig.tight_layout()
        fig.savefig(OUT_PNG, dpi=120)
        plt.close()
        w(f"![srcdate by year]({OUT_PNG.name})")
        w("")
    except Exception as e:
        w(f"(Matplotlib not available: {e}. Text bar chart below.)")
        w("")
        # Text-based bar chart (max bar width 40 chars)
        max_cnt = year_counts.max()
        for yr, cnt in year_counts.items():
            bar_len = int(40 * cnt / max_cnt) if max_cnt else 0
            w(f"  {yr}: {'█' * bar_len} {cnt:,}")
        w("")

    months = valid.dt.month
    month_counts = months.value_counts().sort_index()
    w("**Month distribution (all years combined):**")
    w("")
    w("| Month | Row count | % |")
    w("|-------|-----------|---|")
    for m in range(1, 13):
        cnt = month_counts.get(m, 0)
        w(f"| {m} | {cnt:,} | {pct(cnt, len(valid)):.1f}% |")
    w("")
    december_pct = pct(month_counts.get(12, 0), len(valid))
    if december_pct > 30:
        w("**Interpretation:** There is strong **fiscal year-end concentration**: a large share of observations fall in December (calendar year-end) or in other peak months, consistent with many firms having fiscal year ends in those months.")
    else:
        w("**Interpretation:** Months are more spread out; December does not dominate, so fiscal year ends are **not** heavily clustered in December alone.")
    w("")

    # --- 2. % non-December fiscal year ends; top 10 months ---
    w("## 2. Fiscal year-end months (firm-level)")
    w("")
    # One row per (tic, srcdate) - take unique firm-periods; then for each firm, "fiscal year end month" could be the most common srcdate month or the latest. We use: for each (tic, year), take the srcdate that is the "fiscal year end" for that year (e.g. max srcdate per tic per year = usually the FYE).
    firm_dates = df[["tic", "srcdate_dt"]].drop_duplicates().dropna()
    firm_dates["year"] = firm_dates["srcdate_dt"].dt.year
    firm_dates["month"] = firm_dates["srcdate_dt"].dt.month
    # Per firm, get the most common month (mode) as proxy for FYE month
    fye_by_firm = firm_dates.groupby("tic")["month"].agg(lambda x: x.mode().iloc[0] if len(x.mode()) else x.iloc[0])
    total_firms = len(fye_by_firm)
    month_counts_firms = fye_by_firm.value_counts()
    dec_firms = month_counts_firms.get(12, 0)
    non_dec_firms = total_firms - dec_firms
    w(f"- **% of firms with non-December fiscal year end (proxy):** {pct(non_dec_firms, total_firms):.1f}% ({non_dec_firms:,} of {total_firms:,} firms)")
    w("")
    w("**Top 10 most common fiscal year-end months (by number of firms):**")
    w("")
    top10 = month_counts_firms.head(10)
    month_names = {1:"Jan",2:"Feb",3:"Mar",4:"Apr",5:"May",6:"Jun",7:"Jul",8:"Aug",9:"Sep",10:"Oct",11:"Nov",12:"Dec"}
    w("| Rank | Month | # firms | % |")
    w("|------|-------|--------|---|")
    for i, (mo, cnt) in enumerate(top10.items(), 1):
        w(f"| {i} | {month_names.get(mo, mo)} ({mo}) | {cnt:,} | {pct(cnt, total_firms):.1f}% |")
    w("")

    # --- 3. Sample tickers: full time series (srcdate, cnms, salecs) ---
    w("## 3. Sample tickers: (srcdate, cnms, salecs) time series")
    w("")
    present = [t for t in SAMPLE_TICKERS if (df["tic"] == t).any()]
    missing = [t for t in SAMPLE_TICKERS if t not in present]
    if missing:
        w(f"Tickers not found in CSV1: {missing}. Showing: {present}.")
    w("")
    max_rows_per_ticker = 120  # cap table size for readability
    for tic in present:
        sub = df[df["tic"] == tic][["srcdate", "cnms", "salecs"]].copy()
        sub = sub.sort_values("srcdate")
        total_rows = len(sub)
        show = sub.head(max_rows_per_ticker)
        w(f"### {tic} (total {total_rows} rows)")
        if total_rows > max_rows_per_ticker:
            w(f"*(Showing first {max_rows_per_ticker} rows.)*")
        w("")
        w("| srcdate | cnms | salecs |")
        w("|---------|------|--------|")
        for _, row in show.iterrows():
            sal = row["salecs"] if pd.notna(row["salecs"]) else ""
            cnm = (str(row["cnms"])[:50] + "…") if len(str(row["cnms"])) > 50 else str(row["cnms"])
            w(f"| {row['srcdate']} | {cnm} | {sal} |")
        w("")
        # Consistency: same customers YoY?
        dates = sub["srcdate"].unique()
        cnms_by_date = sub.groupby("srcdate")["cnms"].apply(set).to_dict()
        if len(dates) >= 2:
            prev = None
            changes = []
            for d in sorted(dates):
                cur = cnms_by_date.get(d, set())
                if prev is not None:
                    added = cur - prev
                    dropped = prev - cur
                    if added or dropped:
                        changes.append((d, len(added), len(dropped)))
                prev = cur
            if changes:
                w(f"**Customer set changes:** Year-over-year, the set of reported customers/segments changes (additions/drops) at some dates; not fully consistent.")
            else:
                w(f"**Customer set:** Same customers/segments reported every period (no YoY change).")
        w("")

    # --- 4. Look-ahead bias ---
    w("## 4. Look-ahead bias (SEC filing lag ≈ 60 days)")
    w("")
    w("If we naively use **srcdate** as the signal date (when the supplier–customer link is known), we are using it before the filing is public. Assuming information is public at **srcdate + 60 days**, every such use is exposed to look-ahead bias.")
    w("")
    edges_naive = df[["tic", "cnms", "srcdate"]].drop_duplicates()
    n_edges = len(edges_naive)
    w(f"- **Number of (ticker, customer, srcdate) edge-period observations:** {n_edges:,}")
    w(f"- If signal date = srcdate, then **all {n_edges:,}** are used at a time (srcdate) that is **before** srcdate + 60 days, i.e. before the information is publicly available.")
    w(f"- **Conclusion:** 100% of these edge activations would be exposed to look-ahead bias unless the signal date is set to at least srcdate + 60 days.")
    w("")

    # --- 5. Implausible srcdate ---
    w("## 5. Implausible srcdate values")
    w("")
    ref_dt = pd.Timestamp(REFERENCE_DATE)
    cutoff = pd.Timestamp(CUTOFF_PRE_1993)
    future = df["srcdate_dt"] > ref_dt
    before_1993 = df["srcdate_dt"] < cutoff
    invalid_dt = df["srcdate_dt"].isna()
    n_future = future.sum()
    n_pre93 = before_1993.sum()
    n_invalid = invalid_dt.sum()
    w("**Checks:**")
    w("")
    w(f"- **Future dates** (srcdate > {REFERENCE_DATE}): **{n_future:,}** rows")
    if n_future > 0:
        w("  Sample: " + ", ".join(df.loc[future, "srcdate"].drop_duplicates().head(10).astype(str).tolist()))
    w("")
    w(f"- **Before 1993:** **{n_pre93:,}** rows")
    if n_pre93 > 0:
        w("  Sample: " + ", ".join(df.loc[before_1993, "srcdate"].drop_duplicates().head(10).astype(str).tolist()))
    w("")
    w(f"- **Unparseable/invalid dates:** **{n_invalid:,}** rows")
    w("")

    # Same firm: consecutive srcdate more than 6 months apart
    firm_sorted = df[["tic", "srcdate_dt"]].drop_duplicates().dropna().sort_values(["tic", "srcdate_dt"])
    firm_sorted["prev_srcdate"] = firm_sorted.groupby("tic")["srcdate_dt"].shift(1)
    firm_sorted["gap_days"] = (firm_sorted["srcdate_dt"] - firm_sorted["prev_srcdate"]).dt.days
    gap_gt_6m = firm_sorted["gap_days"] > 183
    gap_negative = firm_sorted["gap_days"] < 0
    n_gap_gt_6m = gap_gt_6m.sum()
    n_gap_neg = gap_negative.sum()
    w("- **Same firm, consecutive srcdate > 6 months apart:** **{:,}** (firm-period) pairs".format(n_gap_gt_6m))
    w("  *(Includes normal 12-month gaps for annual filers.)*")
    w("- **Same firm, consecutive srcdate out of order (negative gap):** **{:,}**".format(n_gap_neg))
    if n_gap_gt_6m > 0:
        sample = firm_sorted.loc[gap_gt_6m, ["tic", "prev_srcdate", "srcdate_dt", "gap_days"]].head(5)
        w("  Sample (tic, prev_srcdate, srcdate_dt, gap_days):")
        for _, r in sample.iterrows():
            w(f"  - {r['tic']}: {r['prev_srcdate']} → {r['srcdate_dt']} (gap {r['gap_days']} days)")
    w("")

    report = "\n".join(lines)
    OUT_MD.write_text(report, encoding="utf-8")
    print(f"Report written to {OUT_MD}")
    if OUT_PNG.exists():
        print(f"Histogram saved to {OUT_PNG}")
    return report

if __name__ == "__main__":
    run()
