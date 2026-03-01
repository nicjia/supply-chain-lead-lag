# Supply Chain Data — Consolidated EDA Summary

Single-page summary of all EDA reports. Full details are in `EDA_REPORT.md`, `EDA_CUSTOMER_MATCHING_REPORT.md`, `EDA_SRCDATE_REPORT.md`, and `EDA_CSV2_REPORT.md` in this folder. Data lives in `../data/`.

---

## Data at a glance

| | CSV1 (segments) | CSV2 (header) |
|---|-----------------|---------------|
| **File** | `gqqatnce2nuvm7hl.csv` | `hwihvjjrzgyby1vf.csv` |
| **Rows** | 364,409 | 202,076 |
| **Role** | Supplier–customer/segment sales (tic, srcdate, gvkey, conm, cid, cnms, salecs, sid) | Company-period index (costat, curcd, datafmt, indfmt, consol, tic, datadate, gvkey, conm) |
| **Date range** | 2010-01-31 to 2025-12-31 | 2010-01-31 to 2026-01-31 |
| **Unique gvkey** | 8,295 | 24,480 |
| **Unique tic** | 8,294 | 24,459 |

Join key: **gvkey** (normalize to 6-digit string; CSV1 has float, CSV2 has int).

---

## CSV1 — Segment data

- **Nulls:** tic/gvkey/conm ~0.2%; salecs 13.92%. cnms/srcdate/cid/sid effectively complete.
- **salecs:** min ≈ -22.5k, max ≈ 397k, mean ≈ 800, median ≈ 64; 13.92% null, 0.01% zero (of non-null).
- **cgvkey:** Not present — no direct Compustat customer gvkey.
- **Firm-years:** 61,007 unique (tic, srcdate); ~5.96 segments per firm per year on average.
- **Named customers:** 364,405 rows with non-empty cnms; 86.08% have non-null salecs (13.92% null).
- **srcdate:** 79.3% of rows in December (strong fiscal year-end concentration). 22.9% of firms have non-December FYE (proxy); top FYE months: Dec 77.1%, Jun 5.4%, Mar 4.9%, Sep 4.6%.
- **Look-ahead:** 292,674 (tic, cnms, srcdate) edge-periods; using srcdate as signal date implies 100% exposed to look-ahead vs 60-day filing lag — use **srcdate + 60 days** as signal date.
- **Implausible dates:** 0 future, 0 before 1993, 0 unparseable; 52,710 firm-periods with consecutive srcdate > 6 months (includes normal 12-month gaps); 0 out-of-order.

---

## Customer matching (cnms)

- **Firm-like cnms:** 25,354 unique (after dropping government, foreign, commercial, other, etc.); full list in `firm_like_cnms_list.txt`.
- **Exact match to CSV2 conm:** 338 cnms match; 3,531 rows (0.97% of CSV1).
- **Known customers:** Multiple string variants per name (e.g. Apple 6, Microsoft 7, Walmart 7, Ford 14, GM 7); use word-boundary matching to avoid false hits (e.g. Ford vs Ashford).
- **Hardest-to-match (no conm match, by freq × avg salecs):** Top entries include United States, International, North America, Not Reported, Europe, Americas, Asia, Other, Japan, then named entities (e.g. CVS Health Corp, Walgreens Boots Alliance). Many are geographic/aggregate segments.

---

## CSV2 — Coverage & survivorship

- **costat:** A 73%, I 27% (row counts). By year, active gvkeys rise (e.g. 5.5k in 2010 → 12.2k in 2024); inactive fall.
- **Survivorship (firms in CSV1):** 8,295 gvkeys in CSV1; all have a row in CSV2. **Current** costat (latest datadate): 61.2% Active, 38.8% Inactive.
- **Ticker stability:** 24,459 gvkeys with one distinct tic; **0** with 2+ tickers — no rename/restatement issues for joins in this extract.
- **Coverage:** 0 gvkeys in CSV1 missing from CSV2; 16,185 gvkeys in CSV2 have no segment data in CSV1.
- **Effective research universe:** **60,971** unique (gvkey × year) in the intersection of both files — use this for supplier-level analysis.

---

## Recommendations

1. **Join on gvkey** (normalized 6-digit string); do not rely on tic alone for linking.
2. **Signal date:** Use **srcdate + 60 days** (or later) to avoid look-ahead bias.
3. **Customer identification:** Only 0.97% of segment rows have cnms that exactly match a conm; most customer names need fuzzy matching or cgvkey if added later.
4. **Fiscal dates:** Most activity is December; account for non-December FYE (22.9% of firms) when aligning to calendar year.
5. **Survivorship:** 38.8% of CSV1 firms are currently Inactive; consider costat when building panels or backtests.

---

*Generated from eda_supply_chain.py, eda_customer_matching.py, eda_srcdate_fiscal.py, eda_csv2_coverage.py. Run from project root or from `eda/` with data in `data/`.*
