# CSV2 coverage & survivorship EDA

## 1. costat breakdown (Active vs Inactive vs other)

| costat | Row count | % |
|--------|-----------|---|
| A | 147,465 | 73.0% |
| I | 54,611 | 27.0% |

**Active vs Inactive firms by datadate year (unique gvkeys):**

| Year | Active (A) | Inactive (I) | Other |
|------|------------|--------------|------|
| 2010 | 5,507 | 5,344 | 0 |
| 2011 | 5,838 | 5,389 | 0 |
| 2012 | 6,373 | 5,264 | 0 |
| 2013 | 6,748 | 4,989 | 0 |
| 2014 | 7,005 | 4,601 | 0 |
| 2015 | 7,321 | 4,171 | 0 |
| 2016 | 7,688 | 3,726 | 0 |
| 2017 | 7,997 | 3,312 | 0 |
| 2018 | 8,432 | 2,960 | 0 |
| 2019 | 9,016 | 2,633 | 0 |
| 2020 | 9,670 | 2,274 | 0 |
| 2021 | 10,317 | 1,948 | 0 |
| 2022 | 10,961 | 1,597 | 0 |
| 2023 | 11,599 | 1,046 | 0 |
| 2024 | 12,228 | 535 | 0 |
| 2025 | 8,350 | 20 | 0 |
| 2026 | 1 | 0 | 0 |

(Plot skipped: No module named 'matplotlib')

  2010: Active 5,507  Inactive 5,344
  2011: Active 5,838  Inactive 5,389
  2012: Active 6,373  Inactive 5,264
  2013: Active 6,748  Inactive 4,989
  2014: Active 7,005  Inactive 4,601
  2015: Active 7,321  Inactive 4,171
  2016: Active 7,688  Inactive 3,726
  2017: Active 7,997  Inactive 3,312
  2018: Active 8,432  Inactive 2,960
  2019: Active 9,016  Inactive 2,633
  2020: Active 9,670  Inactive 2,274
  2021: Active 10,317  Inactive 1,948
  2022: Active 10,961  Inactive 1,597
  2023: Active 11,599  Inactive 1,046
  2024: Active 12,228  Inactive 535
  2025: Active 8,350  Inactive 20
  2026: Active 1  Inactive 0

## 2. Survivorship bias (firms in CSV1)

Firms in CSV1 (with segment data): **8,295** unique gvkeys.

Among these, using **current** costat from CSV2 (latest datadate):

| costat | # firms | % |
|--------|--------|---|
| A | 5,074 | 61.2% |
| I | 3,221 | 38.8% |

Firms in CSV1 with no row in CSV2: **0** (excluded from above).

## 3. Ticker stability (gvkey → distinct tickers)

- **gvkeys with exactly 1 distinct ticker:** 24,459
- **gvkeys with 2+ distinct tickers (rename/restatement risk):** 0

**All gvkeys with 2+ distinct tickers (will break ticker-based joins):**

*None.*

## 4. Coverage gap (CSV1 vs CSV2)

- **gvkeys in CSV1 with NO entry in CSV2:** **0**
- **gvkeys in CSV2 with no segment data in CSV1:** **16,185**

*(Segment data = firm appears as supplier in CSV1.)*

## 5. Effective research universe (intersection)

**Total unique (firm × year) in the intersection of both files:** **60,971**

This is the effective research universe (firm-years with both segment data and CSV2 header).
