# srcdate & fiscal year EDA (CSV1)

## 1. srcdate distribution by year

Count of rows by year:

| Year | Row count |
|------|-----------|
| 2010 | 21,892 |
| 2011 | 22,285 |
| 2012 | 23,202 |
| 2013 | 23,810 |
| 2014 | 24,285 |
| 2015 | 23,624 |
| 2016 | 22,987 |
| 2017 | 22,328 |
| 2018 | 24,916 |
| 2019 | 25,236 |
| 2020 | 25,471 |
| 2021 | 25,525 |
| 2022 | 25,375 |
| 2023 | 25,047 |
| 2024 | 24,195 |
| 2025 | 4,231 |

(Matplotlib not available: No module named 'matplotlib'. Text bar chart below.)

  2010: ██████████████████████████████████ 21,892
  2011: ██████████████████████████████████ 22,285
  2012: ████████████████████████████████████ 23,202
  2013: █████████████████████████████████████ 23,810
  2014: ██████████████████████████████████████ 24,285
  2015: █████████████████████████████████████ 23,624
  2016: ████████████████████████████████████ 22,987
  2017: ██████████████████████████████████ 22,328
  2018: ███████████████████████████████████████ 24,916
  2019: ███████████████████████████████████████ 25,236
  2020: ███████████████████████████████████████ 25,471
  2021: ████████████████████████████████████████ 25,525
  2022: ███████████████████████████████████████ 25,375
  2023: ███████████████████████████████████████ 25,047
  2024: █████████████████████████████████████ 24,195
  2025: ██████ 4,231

**Month distribution (all years combined):**

| Month | Row count | % |
|-------|-----------|---|
| 1 | 5,336 | 1.5% |
| 2 | 1,759 | 0.5% |
| 3 | 16,882 | 4.6% |
| 4 | 3,331 | 0.9% |
| 5 | 2,991 | 0.8% |
| 6 | 15,708 | 4.3% |
| 7 | 2,778 | 0.8% |
| 8 | 2,995 | 0.8% |
| 9 | 18,132 | 5.0% |
| 10 | 4,111 | 1.1% |
| 11 | 1,339 | 0.4% |
| 12 | 289,047 | 79.3% |

**Interpretation:** There is strong **fiscal year-end concentration**: a large share of observations fall in December (calendar year-end) or in other peak months, consistent with many firms having fiscal year ends in those months.

## 2. Fiscal year-end months (firm-level)

- **% of firms with non-December fiscal year end (proxy):** 22.9% (1,898 of 8,294 firms)

**Top 10 most common fiscal year-end months (by number of firms):**

| Rank | Month | # firms | % |
|------|-------|--------|---|
| 1 | Dec (12) | 6,396 | 77.1% |
| 2 | Jun (6) | 446 | 5.4% |
| 3 | Mar (3) | 405 | 4.9% |
| 4 | Sep (9) | 378 | 4.6% |
| 5 | Jan (1) | 192 | 2.3% |
| 6 | Oct (10) | 101 | 1.2% |
| 7 | Apr (4) | 84 | 1.0% |
| 8 | Aug (8) | 72 | 0.9% |
| 9 | Jul (7) | 69 | 0.8% |
| 10 | May (5) | 54 | 0.7% |

## 3. Sample tickers: (srcdate, cnms, salecs) time series


### AAPL (total 82 rows)

| srcdate | cnms | salecs |
|---------|------|--------|
| 2010-09-30 | Not Reported |  |
| 2010-09-30 | Not Reported |  |
| 2012-09-30 | Not Reported |  |
| 2012-09-30 | Not Reported |  |
| 2012-09-30 | Cellular Network Carriers |  |
| 2012-09-30 | International | 95469.88 |
| 2013-09-30 | Cellular Network Carriers |  |
| 2013-09-30 | Direct Distribution | 51273.0 |
| 2013-09-30 | Indirect Distribution | 119637.0 |
| 2013-09-30 | Not Reported |  |
| 2013-09-30 | Not Reported |  |
| 2013-09-30 | International | 104255.1 |
| 2014-09-30 | Not Reported |  |
| 2014-09-30 | Not Reported |  |
| 2014-09-30 | International | 113332.9 |
| 2014-09-30 | Indirect Distribution | 131612.4 |
| 2014-09-30 | Direct Distribution | 51182.6 |
| 2014-09-30 | Cellular Network Carriers |  |
| 2015-09-30 | Not Reported |  |
| 2015-09-30 | Cellular Network Carriers |  |
| 2015-09-30 | Direct Distribution | 60765.9 |
| 2015-09-30 | Indirect Distribution | 172949.1 |
| 2015-09-30 | United States | 81732.0 |
| 2015-09-30 | China | 56547.0 |
| 2015-09-30 | Other Countries | 95436.0 |
| 2016-09-30 | Not Reported |  |
| 2016-09-30 | Indirect Distribution | 161318.25 |
| 2016-09-30 | Direct Distribution | 53772.75 |
| 2016-09-30 | Cellular Network Carriers |  |
| 2016-09-30 | United States | 75667.0 |
| 2016-09-30 | Other Countries | 93623.0 |
| 2016-09-30 | China | 46349.0 |
| 2017-09-30 | Direct Distribution | 64185.52 |
| 2017-09-30 | Cellular Network Carriers |  |
| 2017-09-30 | Indirect Distribution | 165048.48 |
| 2017-09-30 | Other Countries | 100131.0 |
| 2017-09-30 | Not Reported |  |
| 2017-09-30 | United States | 84339.0 |
| 2017-09-30 | Not Reported |  |
| 2018-09-30 | Cellular Network Carriers |  |
| 2018-09-30 | Direct Distribution | 76954.11 |
| 2018-09-30 | Indirect Distribution | 188404.89 |
| 2018-09-30 | Not Reported |  |
| 2018-09-30 | Other Countries | 115592.0 |
| 2018-09-30 | United States | 98061.0 |
| 2019-09-30 | Cellular Network Carriers |  |
| 2019-09-30 | Direct Distribution | 80653.94 |
| 2019-09-30 | Indirect Distribution | 179520.06 |
| 2019-09-30 | United States | 102266.0 |
| 2019-09-30 | Other Countries | 114230.0 |
| 2020-09-30 | Other Countries | 125010.0 |
| 2020-09-30 | United States | 109197.0 |
| 2020-09-30 | Indirect Distribution | 181179.9 |
| 2020-09-30 | Direct Distribution | 93335.1 |
| 2021-09-30 | Cellular Network Carriers |  |
| 2021-09-30 | Direct Distribution | 131694.12 |
| 2021-09-30 | Indirect Distribution | 234122.88 |
| 2021-09-30 | United States | 133803.0 |
| 2021-09-30 | Other Countries | 163648.0 |
| 2022-09-30 | Not Reported |  |
| 2022-09-30 | Other Countries | 172269.0 |
| 2022-09-30 | United States | 147859.0 |
| 2022-09-30 | Indirect Distribution | 244483.36 |
| 2022-09-30 | Direct Distribution | 149844.64 |
| 2022-09-30 | Cellular Network Carriers |  |
| 2023-09-30 | Cellular Network Carriers |  |
| 2023-09-30 | Direct Distribution | 141815.45 |
| 2023-09-30 | Indirect Distribution | 241469.55 |
| 2023-09-30 | United States | 138573.0 |
| 2023-09-30 | Other Countries | 172153.0 |
| 2023-09-30 | Not Reported |  |
| 2024-09-30 | Other Countries | 181887.0 |
| 2024-09-30 | United States | 142196.0 |
| 2024-09-30 | Direct Distribution | 148593.3 |
| 2024-09-30 | Cellular Network Carriers |  |
| 2024-09-30 | Indirect Distribution | 242441.7 |
| 2025-09-30 | Other Countries | 199994.0 |
| 2025-09-30 | Cellular Network Carriers |  |
| 2025-09-30 | Direct Distribution | 166464.4 |
| 2025-09-30 | Indirect Distribution | 249696.6 |
| 2025-09-30 | United States | 151790.0 |
| 2025-09-30 | Not Reported |  |

**Customer set changes:** Year-over-year, the set of reported customers/segments changes (additions/drops) at some dates; not fully consistent.

### MSFT (total 2 rows)

| srcdate | cnms | salecs |
|---------|------|--------|
| 2013-06-30 | Original Equipment Manufacturers (OEM) | 12142.0 |
| 2013-06-30 | Commercial and Retail | 6538.0 |


### WMT (total 53 rows)

| srcdate | cnms | salecs |
|---------|------|--------|
| 2019-01-31 | eCommerce | 2700.0 |
| 2019-01-31 | eCommerce | 15700.0 |
| 2019-01-31 | Mexico and Central America | 31790.0 |
| 2019-01-31 | United Kingdom | 30547.0 |
| 2019-01-31 | Canada | 18613.0 |
| 2019-01-31 | China | 10702.0 |
| 2019-01-31 | Other | 29172.0 |
| 2019-01-31 | eCommerce | 6700.0 |
| 2020-01-31 | eCommerce | 3600.0 |
| 2020-01-31 | eCommerce | 11800.0 |
| 2020-01-31 | Other | 28446.0 |
| 2020-01-31 | eCommerce | 21500.0 |
| 2020-01-31 | Canada | 18420.0 |
| 2020-01-31 | United Kingdom | 29243.0 |
| 2020-01-31 | Mexico and Central America | 33350.0 |
| 2020-01-31 | China | 10671.0 |
| 2021-01-31 | Mexico and Central America | 32642.0 |
| 2021-01-31 | United Kingdom | 29234.0 |
| 2021-01-31 | Canada | 19991.0 |
| 2021-01-31 | China | 11430.0 |
| 2021-01-31 | Other | 28063.0 |
| 2021-01-31 | eCommerce | 43000.0 |
| 2021-01-31 | eCommerce | 16600.0 |
| 2021-01-31 | eCommerce | 5300.0 |
| 2022-01-31 | eCommerce | 47800.0 |
| 2022-01-31 | eCommerce | 6900.0 |
| 2022-01-31 | eCommerce | 18500.0 |
| 2022-01-31 | Other | 25559.0 |
| 2022-01-31 | Canada | 21773.0 |
| 2022-01-31 | United Kingdom | 3811.0 |
| 2022-01-31 | Mexico and Central America | 35964.0 |
| 2022-01-31 | China | 13852.0 |
| 2023-01-31 | eCommerce | 8400.0 |
| 2023-01-31 | Mexico and Central America | 40496.0 |
| 2023-01-31 | Canada | 22300.0 |
| 2023-01-31 | China | 14711.0 |
| 2023-01-31 | Other | 23476.0 |
| 2023-01-31 | eCommerce | 53400.0 |
| 2023-01-31 | eCommerce | 20300.0 |
| 2024-01-31 | eCommerce | 9900.0 |
| 2024-01-31 | eCommerce | 24800.0 |
| 2024-01-31 | eCommerce | 65400.0 |
| 2024-01-31 | Canada | 22639.0 |
| 2024-01-31 | China | 17011.0 |
| 2024-01-31 | Mexico and Central America | 49726.0 |
| 2024-01-31 | Other | 25265.0 |
| 2025-01-31 | eCommerce | 79300.0 |
| 2025-01-31 | Other | 26905.0 |
| 2025-01-31 | eCommerce | 29500.0 |
| 2025-01-31 | Canada | 23035.0 |
| 2025-01-31 | Mexico and Central America | 51970.0 |
| 2025-01-31 | China | 19975.0 |
| 2025-01-31 | eCommerce | 12100.0 |

**Customer set changes:** Year-over-year, the set of reported customers/segments changes (additions/drops) at some dates; not fully consistent.

### F (total 15 rows)

| srcdate | cnms | salecs |
|---------|------|--------|
| 2020-12-31 | North America | 80035.0 |
| 2020-12-31 | South America | 2463.0 |
| 2020-12-31 | Europe | 22644.0 |
| 2020-12-31 | China | 3202.0 |
| 2020-12-31 | International Markets | 7541.0 |
| 2021-12-31 | South America | 2399.0 |
| 2021-12-31 | Europe | 24466.0 |
| 2021-12-31 | China | 2547.0 |
| 2021-12-31 | International Markets | 8955.0 |
| 2021-12-31 | North America | 87783.0 |
| 2022-12-31 | North America | 108727.0 |
| 2022-12-31 | South America | 3096.0 |
| 2022-12-31 | Europe | 25578.0 |
| 2022-12-31 | China | 1769.0 |
| 2022-12-31 | International Markets | 9810.0 |

**Customer set:** Same customers/segments reported every period (no YoY change).

### BA (total 142 rows)
*(Showing first 120 rows.)*

| srcdate | cnms | salecs |
|---------|------|--------|
| 2010-12-31 | Europe | 6140.0 |
| 2010-12-31 | Asia | 5466.0 |
| 2010-12-31 | US Government | 27652.0 |
| 2010-12-31 | Commercial Aircraft |  |
| 2010-12-31 | Asia | 1822.0 |
| 2010-12-31 | Non-U.S Customers | 26365.46 |
| 2010-12-31 | Europe | 1574.4 |
| 2011-12-31 | Non-U.S Customers | 34368.0 |
| 2011-12-31 | Asia | 2826.0 |
| 2011-12-31 | Commercial Aircraft |  |
| 2011-12-31 | Europe | 1576.0 |
| 2011-12-31 | Asia | 4537.0 |
| 2011-12-31 | Europe | 8176.0 |
| 2011-12-31 | US Government | 26119.0 |
| 2012-12-31 | Airtran Airways Inc | 70.56 |
| 2012-12-31 | US Government | 26960.34 |
| 2012-12-31 | Commercial Aircraft |  |
| 2012-12-31 | International | 44116.92 |
| 2013-12-31 | International | 49375.11 |
| 2013-12-31 | Commercial Aircraft |  |
| 2013-12-31 | US Government | 29451.82 |
| 2014-12-31 | US Government | 27228.6 |
| 2014-12-31 | Commercial Aircraft |  |
| 2014-12-31 | International | 52641.96 |
| 2015-12-31 | International | 56707.26 |
| 2015-12-31 | US Government | 25950.78 |
| 2015-12-31 | Commercial Aircraft |  |
| 2016-12-31 | International | 55796.89 |
| 2016-12-31 | US Government | 21751.33 |
| 2016-12-31 | Commercial Aircraft |  |
| 2017-12-31 | United States Department of Defense | 16635.03 |
| 2017-12-31 | US Government | 28951.52 |
| 2017-12-31 | Commercial Aircraft |  |
| 2017-12-31 | International | 51365.6 |
| 2018-12-31 | United States | 16492.0 |
| 2018-12-31 | International | 56631.12 |
| 2018-12-31 | Commercial customer contracts |  |
| 2018-12-31 | US Government | 31349.37 |
| 2018-12-31 | US Government | 6064.92 |
| 2018-12-31 | Government | 7620.0 |
| 2018-12-31 | Commercial | 9227.0 |
| 2018-12-31 | Non United States | 6703.0 |
| 2018-12-31 | United States | 17081.0 |
| 2018-12-31 | Other | 5185.0 |
| 2018-12-31 | Middle East | 5876.0 |
| 2018-12-31 | Asia, other than China | 8274.0 |
| 2018-12-31 | China | 13068.0 |
| 2018-12-31 | Europe | 9719.0 |
| 2018-12-31 | United States Department of Defense | 19947.7 |
| 2019-12-31 | US Government | 33079.02 |
| 2019-12-31 | International | 46649.9 |
| 2019-12-31 | China | 5051.0 |
| 2019-12-31 | Europe | 5829.0 |
| 2019-12-31 | United States Department of Defense | 23342.03 |
| 2019-12-31 | US Government | 6213.16 |
| 2019-12-31 | Asia, other than China | 7395.0 |
| 2019-12-31 | Commercial | 10167.0 |
| 2019-12-31 | Middle East | 5761.0 |
| 2019-12-31 | Other | 3450.0 |
| 2019-12-31 | United States | 12676.0 |
| 2019-12-31 | United States | 19573.0 |
| 2019-12-31 | Government | 8107.0 |
| 2019-12-31 | Non United States | 6654.0 |
| 2020-12-31 | Government | 8368.0 |
| 2020-12-31 | Commercial | 6936.0 |
| 2020-12-31 | Non United States | 6595.0 |
| 2020-12-31 | US Government | 6274.64 |
| 2020-12-31 | United States | 7899.0 |
| 2020-12-31 | Other | 513.0 |
| 2020-12-31 | Middle East | 1647.0 |
| 2020-12-31 | Asia, other than China | 1408.0 |
| 2020-12-31 | US Government | 29914.56 |
| 2020-12-31 | China | 1271.0 |
| 2020-12-31 | International | 21702.72 |
| 2020-12-31 | United States | 19662.0 |
| 2020-12-31 | United States Department of Defense | 23368.73 |
| 2020-12-31 | Europe | 3872.0 |
| 2021-12-31 | Asia | 2792.0 |
| 2021-12-31 | US Government | 6432.0 |
| 2021-12-31 | Government | 8553.0 |
| 2021-12-31 | Non United States | 6671.0 |
| 2021-12-31 | United States | 19869.0 |
| 2021-12-31 | United States | 9472.0 |
| 2021-12-31 | Other | 1681.0 |
| 2021-12-31 | Middle East | 1098.0 |
| 2021-12-31 | Europe | 4334.0 |
| 2021-12-31 | United States Department of Defense | 23620.6 |
| 2021-12-31 | International | 23040.64 |
| 2021-12-31 | US Government | 30513.28 |
| 2021-12-31 | Commercial | 7527.0 |
| 2022-12-31 | US Government | 5689.53 |
| 2022-12-31 | Asia | 4484.0 |
| 2022-12-31 | Government | 7681.0 |
| 2022-12-31 | United States Department of Defense | 20614.18 |
| 2022-12-31 | Non United States | 6018.0 |
| 2022-12-31 | United States | 17144.0 |
| 2022-12-31 | United States | 12167.0 |
| 2022-12-31 | Other | 3042.0 |
| 2022-12-31 | Middle East | 2003.0 |
| 2022-12-31 | Europe | 4038.0 |
| 2022-12-31 | International | 27309.28 |
| 2022-12-31 | US Government | 26643.2 |
| 2022-12-31 | Commercial | 9560.0 |
| 2023-12-31 | United States | 14501.0 |
| 2023-12-31 | Other non-U.S. | 2431.0 |
| 2023-12-31 | Middle East | 4311.0 |
| 2023-12-31 | US Government | 5631.3 |
| 2023-12-31 | Asia | 6328.0 |
| 2023-12-31 | US Government | 28783.78 |
| 2023-12-31 | United States Department of Defense | 22689.03 |
| 2023-12-31 | United States | 20051.0 |
| 2023-12-31 | Non United States | 4882.0 |
| 2023-12-31 | Europe | 6172.0 |
| 2023-12-31 | International | 32673.48 |
| 2023-12-31 | Commercial | 11020.0 |
| 2023-12-31 | Government | 7751.0 |
| 2024-12-31 | Non United States | 5329.0 |
| 2024-12-31 | International | 30789.0 |
| 2024-12-31 | United States | 18589.0 |
| 2024-12-31 | US Government | 28123.2 |

**Customer set changes:** Year-over-year, the set of reported customers/segments changes (additions/drops) at some dates; not fully consistent.

## 4. Look-ahead bias (SEC filing lag ≈ 60 days)

If we naively use **srcdate** as the signal date (when the supplier–customer link is known), we are using it before the filing is public. Assuming information is public at **srcdate + 60 days**, every such use is exposed to look-ahead bias.

- **Number of (ticker, customer, srcdate) edge-period observations:** 292,674
- If signal date = srcdate, then **all 292,674** are used at a time (srcdate) that is **before** srcdate + 60 days, i.e. before the information is publicly available.
- **Conclusion:** 100% of these edge activations would be exposed to look-ahead bias unless the signal date is set to at least srcdate + 60 days.

## 5. Implausible srcdate values

**Checks:**

- **Future dates** (srcdate > 2026-02-24): **0** rows

- **Before 1993:** **0** rows

- **Unparseable/invalid dates:** **0** rows

- **Same firm, consecutive srcdate > 6 months apart:** **52,710** (firm-period) pairs
  *(Includes normal 12-month gaps for annual filers.)*
- **Same firm, consecutive srcdate out of order (negative gap):** **0**
  Sample (tic, prev_srcdate, srcdate_dt, gap_days):
  - 0030B: 2010-12-31 00:00:00 → 2011-12-31 00:00:00 (gap 365.0 days)
  - 0030B: 2011-12-31 00:00:00 → 2012-12-31 00:00:00 (gap 366.0 days)
  - 0030B: 2012-12-31 00:00:00 → 2013-12-31 00:00:00 (gap 365.0 days)
  - 0032A: 2010-12-31 00:00:00 → 2011-12-31 00:00:00 (gap 365.0 days)
  - 0032A: 2011-12-31 00:00:00 → 2012-12-31 00:00:00 (gap 366.0 days)
