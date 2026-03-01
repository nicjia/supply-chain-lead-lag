# Supply Chain CSVs — EDA Report

Loading CSVs...
## 1. Shape, dtypes, null counts, null %

### CSV1 (gqqatnce2nuvm7hl.csv)
- **Shape:** 364,409 rows × 8 columns

| Column | dtype | null_count | null_% |
|--------|-------|------------|--------|
| tic | str | 698 | 0.19% |
| srcdate | str | 0 | 0.00% |
| gvkey | float64 | 682 | 0.19% |
| conm | str | 682 | 0.19% |
| cid | int64 | 0 | 0.00% |
| cnms | str | 4 | 0.00% |
| salecs | float64 | 50,741 | 13.92% |
| sid | int64 | 0 | 0.00% |

### CSV2 (hwihvjjrzgyby1vf.csv)
- **Shape:** 202,076 rows × 9 columns

| Column | dtype | null_count | null_% |
|--------|-------|------------|--------|
| costat | str | 0 | 0.00% |
| curcd | str | 0 | 0.00% |
| datafmt | str | 0 | 0.00% |
| indfmt | str | 0 | 0.00% |
| consol | str | 0 | 0.00% |
| tic | str | 128 | 0.06% |
| datadate | str | 0 | 0.00% |
| gvkey | int64 | 0 | 0.00% |
| conm | str | 0 | 0.00% |

## 2. Date range coverage

| Field | min | max |
|-------|-----|-----|
| **srcdate** (CSV1) | 2010-01-31 00:00:00 | 2025-12-31 00:00:00 |
| **datadate** (CSV2) | 2010-01-31 00:00:00 | 2026-01-31 00:00:00 |

## 3. Unique tickers, gvkeys, company names

| Metric | CSV1 | CSV2 |
|--------|------|------|
| Unique **tic** | 8,294 | 24,459 |
| Unique **gvkey** | 8,295 | 24,480 |
| Unique **conm** | 8,295 | 24,479 |

## 4. salecs distribution (CSV1)

| Stat | Value |
|------|-------|
| min | -22511.902 |
| max | 397133.55 |
| mean | 800.3944 |
| median | 63.9200 |
| % null | 13.92% |
| % zero (of non-null) | 0.01% |

## 5. cgvkey column (Compustat customer gvkey) in CSV1

**cgvkey** is **not** present in CSV1.

## 6. Top 20 most frequent cnms (CSV1)

| Rank | cnms | Count | Non-firm segment? |
|------|------|-------|-------------------|
| 1 | 'Not Reported' | 52,053 | Yes |
| 2 | 'United States' | 8,575 | No |
| 3 | 'International' | 6,950 | No |
| 4 | 'Other' | 6,242 | Yes |
| 5 | 'Europe' | 5,975 | No |
| 6 | 'North America' | 4,942 | No |
| 7 | '2 Customers' | 4,193 | No |
| 8 | '3 Customers' | 3,781 | No |
| 9 | '5 Customers' | 3,284 | No |
| 10 | 'Canada' | 2,898 | No |
| 11 | '4 Customers' | 2,845 | No |
| 12 | '10 Customers' | 2,779 | No |
| 13 | '9 Customers' | 2,772 | No |
| 14 | 'Asia' | 2,670 | No |
| 15 | 'Foreign' | 2,415 | Yes |
| 16 | 'Asia Pacific' | 2,275 | No |
| 17 | 'Americas' | 2,140 | No |
| 18 | 'Commercial' | 1,998 | Yes |
| 19 | 'United Kingdom' | 1,932 | No |
| 20 | 'China' | 1,868 | No |

## 7. Unique (tic, srcdate) and segments per firm-year

- **Unique (tic, srcdate) pairs:** 61,007
- **Average segments per firm per year:** 5.96

## 8. Named customers vs salecs availability

- Rows with a non-empty **cnms** (named customer): 364,405
- Of those, rows with non-null **salecs**: 313,666 (86.08%)
- Of those, rows with null/missing **salecs**: 50,739 (13.92%)

Named customers **frequently** have null revenue when the segment is aggregate (e.g. Government, Foreign, Not Reported) rather than a specific firm.
