# Supply-Chain Lead–Lag Research Framework

## 1. Project thesis

This project studies whether information diffuses slowly through supply-chain relationships and whether this diffusion can be detected, organized into network structure, and traded under a point-in-time backtest protocol.

The central economic channel is:

```text
Customer shock today → delayed supplier reaction tomorrow
```

If customer firm `j` is economically important to supplier firm `i`, then customer returns may predict future supplier returns. The supply-chain edge is directional:

```text
customer j → supplier i
```

and weighted by revenue dependence:

```math
w_{j \to i,t} = \frac{\text{sales from supplier } i \text{ to customer } j}{\text{total sales of supplier } i}.
```

The project upgrades standard return-based lead-lag detection by adding economically grounded supply-chain structure. The final research question is:

> Do point-in-time supply-chain links act as economically grounded priors that improve detection, stability, and tradability of cross-firm lead-lag information diffusion?

---

## 2. Research questions

The project should answer the following questions in order.

### RQ1. Does customer pressure predict future supplier returns?

Estimate panel regressions of future supplier returns on current customer-pressure signals. The main result should report whether the customer-to-supplier direction is statistically significant and economically meaningful.

### RQ2. Is the effect directional rather than symmetric comovement?

Run the reverse placebo test: supplier pressure predicting customer returns. A strong project result is:

```text
Customer → Supplier significant
Supplier → Customer insignificant
```

This supports directional information diffusion rather than generic correlation.

### RQ3. Is the effect tradable under a rolling point-in-time backtest?

Use only data available at each rebalance date. Estimate the lead-lag matrix from trailing returns and PIT edges, generate daily supplier-pressure signals, and apply next-day long-short weights.

The key test is whether the main strategy beats random, momentum, structural-only, and equal-weight baselines after costs.

### RQ4. Does higher-order network structure improve the signal?

Compare four strategy families:

1. Supplier Pressure
2. GlobalRank
3. MetaCluster
4. ClusterRank

This asks whether organizing noisy pairwise lead-lag edges into rankings or clusters improves prediction and trading performance.

### RQ5. Does direction-aware clustering outperform direction-blind clustering?

Compare multiple clustering methods while keeping the rest of the strategy fixed:

- Sector or SIC clustering baseline
- Supply-chain community clustering
- Symmetric spectral clustering on relationship strength
- Hermitian spectral clustering on directed imbalance
- Signed clustering
- Hybrid-prior clustering

The key test is whether methods that preserve directionality outperform methods that only use absolute relationship strength.

### RQ6. Does supply-chain structure stabilize return-based lead-lag networks?

Compare pure return-based lead-lag, pure supply-chain structure, and hybrid matrices:

```math
C^{hybrid} = \alpha C^{data} + (1-\alpha) C^{supply}.
```

Evaluate Sharpe, turnover, cluster stability, eigenspace drift, and drawdown across different values of `alpha`.

### RQ7. Is diffusion stronger around information events?

Use earnings calendars, large customer-return shocks, and high-volatility regimes to test whether supply-chain diffusion is event-amplified.

A strong result is:

```text
All days: weak but positive effect
Customer event days: stronger effect
Reverse direction: still weak
```

---

## 3. Data inputs

The expected data inputs are:

| File | Role |
|---|---|
| `output_edges.csv` or `data/merged_edges.csv` | PIT customer-supplier links with revenue weights |
| `returns_with_gvkey.parquet` | Daily CRSP returns matched to `gvkey` |
| `query3_translator.csv` | Identifier mapping among ticker, gvkey, PERMNO |
| `output_earnings_calendar.csv` | Earnings announcement dates for event-conditioned analysis |
| Optional sector map | SIC/GICS/Fama-French industry baseline clusters |

The key edge fields are:

```text
customer_gvkey, supplier_gvkey, date or filing_date, weight_wji
```

The key return fields are:

```text
gvkey, date, RET
```

All trading and estimation must be point-in-time. Future edges, future returns, and future identifiers must not be used.

---

## 4. Core objects

### 4.1 Supply-chain customer-pressure signal

For supplier `i` on day `t`:

```math
y_{i,t} = \sum_{j \in \mathcal{C}(i,t)} w_{j \to i,t} r_{j,t}.
```

This is the weighted return of supplier `i`'s customers.

### 4.2 Pairwise lead-lag matrix

Construct an `N × N` matrix `C`, where:

```text
C[j, i] = directional score from customer j to supplier i
```

Possible edge scores:

- `tstat_diff`: forward t-stat minus reverse t-stat
- `beta_diff`: forward beta minus reverse beta
- `cross_corr`: cross-correlation asymmetry
- `regression_r2`: predictive R² asymmetry
- `granger`: Granger-style asymmetry
- `levy`: signature / Lévy-area score
- `structural`: pure revenue-weighted supply-chain score
- `hybrid`: blend of return-based and structural matrices

### 4.3 Directional skew matrix

```math
S = C - C^\top.
```

`S[i, j] > 0` means firm `i` is a net leader relative to firm `j`.

### 4.4 Hermitian directed matrix

```math
H = iS.
```

`H` has real eigenvalues and can be used for direction-aware spectral ranking and clustering.

---

## 5. Empirical layers

The project has three empirical layers.

### Layer 1. Econometric validation

Run panel regressions:

```math
r_{i,t+h} = \alpha_i + \delta_t + \beta_h y_{i,t} + \sum_{k=1}^{5}\gamma_k r_{i,t-k} + \epsilon_{i,t}.
```

Use supplier fixed effects, date fixed effects, and supplier-clustered standard errors.

Also run the reverse placebo:

```math
r_{j,t+h} = \alpha_j + \delta_t + \tilde{\beta}_h z_{j,t} + \epsilon_{j,t},
```

where `z` aggregates supplier returns for each customer.

Required outputs:

- `panel_forward_reverse.csv`
- `horizon_decay.csv`
- `plots/forward_vs_reverse_coefficients.png`
- `plots/horizon_decay_beta.png`

### Layer 2. Network structure

Compute:

- Leadingness row-sums of `S`
- Hermitian spectrum of `H`
- GlobalRank using top eigenvectors
- MetaCluster using cluster-level flow
- ClusterRank using within-cluster leader-lagger ranks
- Permutation test for global directional structure

Required outputs:

- `spectral_summary.csv`
- `cluster_method_comparison.csv`
- `cluster_stability.csv`
- `plots/scree_plot.png`
- `plots/heatmap_C_sorted_by_cluster.png`
- `plots/meta_flow_network.png`
- `plots/cluster_stability_ari.png`

### Layer 3. Trading and backtesting

Run rolling PIT backtests for all strategy families and baselines.

Required outputs:

- `summary_metrics.csv`
- `strategy_family_comparison.csv`
- `turnover_costs.csv`
- `holdings_or_weights.parquet`
- `daily_returns.csv`
- `plots/cumulative_pnl_by_strategy.png`
- `plots/drawdown_by_strategy.png`
- `plots/sharpe_by_method.png`

---

## 6. Strategy families

### 6.1 Supplier Pressure

This is the primary tradable thesis.

At rebalance date `T`:

1. PIT-filter supply-chain edges.
2. Estimate `C_T` on trailing returns.
3. For each trading day `d` until the next rebalance:

```math
s_d = C_T^\top r_d.
```

4. Long top-quantile suppliers by `s_d`, short bottom-quantile suppliers.
5. Apply weights on day `d+1`.

Interpretation:

```text
Customers generate the signal; suppliers are the traded laggers.
```

### 6.2 GlobalRank

Estimate `C_T`, form `S_T = C_T - C_T^T`, and compute global leader-lagger scores using one of:

- leadingness row-sum
- Hermitian spectral ranking
- SyncRank-style ranking if implemented later

Then construct a static or signal-modulated long-short portfolio based on global ranks.

Research purpose:

> Does the whole supply-chain network contain a market-wide leader-lagger hierarchy?

### 6.3 MetaCluster

1. Cluster firms into `K` clusters.
2. Construct cluster returns:

```math
R_{c,t} = \frac{1}{|C_c|}\sum_{i \in C_c} r_{i,t}.
```

3. Estimate cluster-to-cluster net flow:

```math
F_{ab} = \frac{1}{|C_a||C_b|}\sum_{i\in C_a, j\in C_b}(C_{ij} - C_{ji}).
```

4. Identify leading and lagging clusters.
5. Use leading-cluster returns to trade lagging clusters.

Research purpose:

> Does supply-chain information diffusion become cleaner at the group level?

### 6.4 ClusterRank

1. Cluster firms.
2. Within each cluster, compute local leadingness:

```math
\ell_i^{local} = \sum_{j \in C(i)}(C_{ij} - C_{ji}).
```

3. Select local leaders and local laggers.
4. Use local leader returns as the signal.
5. Trade only local laggers.

Research purpose:

> Is lead-lag stronger inside economically coherent clusters than across the full market?

---

## 7. Clustering methods to compare

Implement all clustering methods behind one interface:

```python
get_cluster_labels(C, method, n_clusters, side_info=None, sector_map=None) -> pd.Series
```

Required methods:

| Method | Matrix / input | Purpose |
|---|---|---|
| `sector` | SIC/GICS labels | Industry baseline |
| `supply_community` | Structural supply-chain graph | Economic topology baseline |
| `symmetric_spectral` | `abs(C) + abs(C.T)` | Direction-blind relationship strength |
| `hermitian` | `i(C - C.T)` | Direction-aware clustering |
| `signed` | positive/negative lead-lag edges | Separate aligned and opposing relationships |
| `hybrid_prior` | return graph + supply prior | Stabilized clusters |

---

## 8. Baselines

Every backtest should compare against:

| Baseline | Description |
|---|---|
| `random` | Shuffled strategy scores |
| `momentum` | Long-short on trailing returns |
| `structural` | Pure supply-chain leadingness / pressure |
| `equal_weight` | Long-only equal-weight network universe |
| `sector` | Sector-neutral or sector-ranking baseline |

---

## 9. Realism controls

Backtests should support:

- commission bps
- slippage bps
- borrow cost on short notional
- max absolute weight cap
- beta neutralization
- sector neutralization
- turnover reporting
- gross and net returns
- PIT edge date control using `filing_date`
- edge expiry window

---

## 10. Final expected contribution

The final project should contribute:

1. A PIT supply-chain lead-lag matrix construction.
2. Econometric evidence of directional customer-to-supplier diffusion.
3. A tradable supplier-pressure strategy.
4. A comparison of Supplier Pressure, GlobalRank, MetaCluster, and ClusterRank.
5. A clustering-method comparison showing whether direction-aware clustering matters.
6. A hybrid-prior study showing whether supply-chain structure stabilizes return-based lead-lag networks.
7. Event-conditioned evidence showing whether customer shocks amplify diffusion.

---

## 11. Final answer format

The final report should end with a table like this:

| Research question | Evidence | Answer |
|---|---|---|
| Does customer pressure predict supplier returns? | Forward panel regression | Yes / No |
| Is the effect directional? | Reverse placebo | Yes / No |
| Is it tradable? | Supplier-pressure PIT backtest | Yes / No |
| Does clustering help? | Strategy family comparison | Yes / No |
| Does direction-aware clustering help? | Hermitian vs symmetric clustering | Yes / No |
| Does supply-chain structure stabilize the network? | Hybrid alpha sweep + stability | Yes / No |
| Is diffusion event-amplified? | Earnings/shock conditioned tests | Yes / No |
