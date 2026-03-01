#!/usr/bin/env python3
"""
EDA on CSV2: costat breakdown, active/inactive over time, survivorship bias,
ticker stability (gvkey–tic), coverage gaps vs CSV1, and effective research universe.
Outputs EDA_CSV2_REPORT.md and costat_active_inactive_by_year.png (if matplotlib).
"""
import pandas as pd
import numpy as np
from pathlib import Path

BASE = Path(__file__).resolve().parent
DATA = BASE.parent / "data"
CSV1 = DATA / "gqqatnce2nuvm7hl.csv"
CSV2 = DATA / "hwihvjjrzgyby1vf.csv"
OUT_MD = BASE / "EDA_CSV2_REPORT.md"
OUT_PNG = BASE / "costat_active_inactive_by_year.png"

def pct(x, total):
    return (x / total * 100) if total else 0

def norm_gvkey(x):
    if pd.isna(x): return None
    if isinstance(x, float) and x == x:  # non-NaN float
        x = int(x)
    s = str(x).strip().lstrip("0") or "0"
    return s.zfill(6)

def run():
    lines = []
    def w(s=""):
        lines.append(s)

    df1 = pd.read_csv(CSV1, low_memory=False)
    df2 = pd.read_csv(CSV2, low_memory=False)
    df2["datadate_dt"] = pd.to_datetime(df2["datadate"], errors="coerce")
    df2["year"] = df2["datadate_dt"].dt.year
    df2["gvkey_norm"] = df2["gvkey"].apply(norm_gvkey)
    df1["gvkey_norm"] = df1["gvkey"].apply(norm_gvkey)

    w("# CSV2 coverage & survivorship EDA")
    w("")

    # --- 1. costat breakdown + plot over time ---
    w("## 1. costat breakdown (Active vs Inactive vs other)")
    w("")
    costat_counts = df2["costat"].value_counts()
    total = len(df2)
    w("| costat | Row count | % |")
    w("|--------|-----------|---|")
    for c, cnt in costat_counts.items():
        w(f"| {c} | {cnt:,} | {pct(cnt, total):.1f}% |")
    other = set(df2["costat"].dropna().unique()) - {"A", "I"}
    if other:
        w("")
        w(f"**Other values:** {sorted(other)}")
    w("")

    # Active vs Inactive firms by year (unique gvkeys per year with costat A vs I)
    by_year = df2.dropna(subset=["year", "costat"]).groupby(["year", "costat"])["gvkey_norm"].nunique().unstack(fill_value=0)
    by_year = by_year.reindex(columns=["A", "I"] + [c for c in by_year.columns if c not in ("A", "I")], fill_value=0)
    w("**Active vs Inactive firms by datadate year (unique gvkeys):**")
    w("")
    w("| Year | Active (A) | Inactive (I) | Other |")
    w("|------|------------|--------------|------|")
    for yr in sorted(by_year.index):
        a = int(by_year.loc[yr, "A"]) if "A" in by_year.columns else 0
        i = int(by_year.loc[yr, "I"]) if "I" in by_year.columns else 0
        other_cols = [c for c in by_year.columns if c not in ("A", "I")]
        o = int(by_year.loc[yr, other_cols].sum()) if other_cols else 0
        w(f"| {yr} | {a:,} | {i:,} | {o:,} |")
    w("")

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        fig, ax = plt.subplots(figsize=(10, 4))
        x = by_year.index.astype(int)
        ax.bar(x - 0.2, by_year.get("A", pd.Series(0, index=by_year.index)), width=0.35, label="Active (A)", color="steelblue")
        ax.bar(x + 0.2, by_year.get("I", pd.Series(0, index=by_year.index)), width=0.35, label="Inactive (I)", color="coral", alpha=0.9)
        ax.set_xlabel("Year (datadate)")
        ax.set_ylabel("Number of unique firms (gvkey)")
        ax.set_title("Active vs Inactive firms over time (CSV2)")
        ax.legend()
        ax.set_xticks(x)
        fig.tight_layout()
        fig.savefig(OUT_PNG, dpi=120)
        plt.close()
        w(f"![Active vs Inactive by year]({OUT_PNG.name})")
    except Exception as e:
        w(f"(Plot skipped: {e})")
        w("")
        for yr in sorted(by_year.index):
            a = int(by_year.loc[yr, "A"]) if "A" in by_year.columns else 0
            i = int(by_year.loc[yr, "I"]) if "I" in by_year.columns else 0
            w(f"  {yr}: Active {a:,}  Inactive {i:,}")
    w("")

    # --- 2. Survivorship bias ---
    w("## 2. Survivorship bias (firms in CSV1)")
    w("")
    gvkeys_csv1 = set(df1["gvkey_norm"].dropna().unique())
    # "Currently" = latest datadate per gvkey in CSV2
    latest = df2.sort_values("datadate_dt").groupby("gvkey_norm").last().reset_index()[["gvkey_norm", "costat"]]
    latest = latest.rename(columns={"costat": "current_costat"})
    in_both = [g for g in gvkeys_csv1 if g in latest["gvkey_norm"].values]
    in_both_set = set(in_both)
    status = latest[latest["gvkey_norm"].isin(in_both_set)]
    status_counts = status["current_costat"].value_counts()
    n_both = len(status)
    w(f"Firms in CSV1 (with segment data): **{len(gvkeys_csv1):,}** unique gvkeys.")
    w("")
    w("Among these, using **current** costat from CSV2 (latest datadate):")
    w("")
    w("| costat | # firms | % |")
    w("|--------|--------|---|")
    for c, cnt in status_counts.items():
        w(f"| {c} | {cnt:,} | {pct(cnt, n_both):.1f}% |")
    w("")
    not_in_csv2 = len(gvkeys_csv1) - n_both
    w(f"Firms in CSV1 with no row in CSV2: **{not_in_csv2:,}** (excluded from above).")
    w("")

    # --- 3. Ticker stability: gvkey -> distinct tickers ---
    w("## 3. Ticker stability (gvkey → distinct tickers)")
    w("")
    tic_per_gvkey = df2.dropna(subset=["gvkey_norm", "tic"]).groupby("gvkey_norm")["tic"].nunique()
    multi = tic_per_gvkey[tic_per_gvkey >= 2].sort_values(ascending=False)
    n_single = (tic_per_gvkey == 1).sum()
    n_multi = (tic_per_gvkey >= 2).sum()
    w(f"- **gvkeys with exactly 1 distinct ticker:** {n_single:,}")
    w(f"- **gvkeys with 2+ distinct tickers (rename/restatement risk):** {n_multi:,}")
    w("")
    w("**All gvkeys with 2+ distinct tickers (will break ticker-based joins):**")
    w("")
    if len(multi) == 0:
        w("*None.*")
    else:
        w("| gvkey | # distinct tic | tickers |")
        w("|-------|----------------|--------|")
        for gvkey in multi.index:
            tics = df2[df2["gvkey_norm"] == gvkey]["tic"].dropna().unique().tolist()
            tic_str = ", ".join(sorted(str(t) for t in tics)[:15])
            if len(tics) > 15:
                tic_str += ", ..."
            w(f"| {gvkey} | {multi[gvkey]} | {tic_str} |")
    w("")

    # --- 4. Coverage gap ---
    w("## 4. Coverage gap (CSV1 vs CSV2)")
    w("")
    gvkeys_csv2 = set(df2["gvkey_norm"].dropna().unique())
    only_csv1 = gvkeys_csv1 - gvkeys_csv2
    only_csv2 = gvkeys_csv2 - gvkeys_csv1
    w(f"- **gvkeys in CSV1 with NO entry in CSV2:** **{len(only_csv1):,}**")
    w(f"- **gvkeys in CSV2 with no segment data in CSV1:** **{len(only_csv2):,}**")
    w("")
    w("*(Segment data = firm appears as supplier in CSV1.)*")
    w("")

    # --- 5. Effective research universe: (gvkey, year) in both ---
    w("## 5. Effective research universe (intersection)")
    w("")
    df1["year1"] = pd.to_datetime(df1["srcdate"], errors="coerce").dt.year
    pairs1 = df1[["gvkey_norm", "year1"]].dropna().drop_duplicates()
    pairs2 = df2[["gvkey_norm", "year"]].dropna().drop_duplicates()
    pairs2 = pairs2.rename(columns={"year": "year1"})
    inter = pairs1.merge(pairs2, on=["gvkey_norm", "year1"], how="inner")
    n_firm_years = len(inter)
    w(f"**Total unique (firm × year) in the intersection of both files:** **{n_firm_years:,}**")
    w("")
    w("This is the effective research universe (firm-years with both segment data and CSV2 header).")
    w("")

    report = "\n".join(lines)
    OUT_MD.write_text(report, encoding="utf-8")
    print(f"Report written to {OUT_MD}")
    if OUT_PNG.exists():
        print(f"Plot saved to {OUT_PNG}")
    return report

if __name__ == "__main__":
    run()
