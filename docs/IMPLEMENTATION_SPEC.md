# Supply-Chain Lead–Lag Implementation Specification

This document is the coding-agent spec. It translates the research framework into concrete engineering tasks, APIs, scripts, outputs, and acceptance tests.

The coding agent should upgrade the existing repo. It should not rebuild working modules from scratch unless necessary. Preserve current behavior for existing scripts and tests.

---

## 0. Existing capabilities to preserve

The repo already has core modules for:

- Loading supply-chain edges and returns
- Building lead-lag matrix `C`
- Building structural and hybrid matrices
- Pairwise scores: correlation, regression R², Granger, Lévy/signature-style scores
- Hermitian spectrum / GlobalRank / MetaCluster / ClusterRank utilities
- Supplier-pressure signal
- Rolling PIT backtest
- Rank-factor backtest
- Baselines
- Costs and risk overlays
- YAML config parsing
- Grid search
- Tests

The upgrade should add clearer strategy-family abstractions, explicit cluster comparison, hybrid/stability analysis, event-conditioned analysis, and final report generation.

---

## 1. New config structure

Add or extend `config/research.yaml` with the following fields.

```yaml
paths:
  edges_csv: data/merged_edges.csv
  returns_parquet: data/returns_with_gvkey.parquet
  earnings_calendar_csv: data/output_earnings_calendar.csv
  sector_map_csv: data/sector_map.csv
  output_dir: results/final_research

pit:
  edge_date_col: filing_date
  edge_expiry_days: 550
  prefer_filing_date: true

matrix:
  lookback_rows: 504
  horizon: 1
  winsor_q: 0.001
  edge_scores:
    - tstat_diff
    - beta_diff
    - cross_corr
    - regression_r2
    - granger
    - levy
  hybrid_alpha_grid: [0.0, 0.25, 0.5, 0.75, 1.0]

clustering:
  n_clusters: 10
  random_state: 42
  methods:
    - sector
    - supply_community
    - symmetric_spectral
    - hermitian
    - signed
    - hybrid_prior

strategies:
  families:
    - supplier_pressure
    - globalrank
    - metacluster
    - clusterrank
  q: 0.2
  rebalance_freq: M
  max_rebalances: null
  apply_next_day: true

baselines:
  include: true
  methods:
    - random
    - momentum
    - structural
    - equal_weight
    - sector
  momentum_window: 20

costs:
  commission_bps: 0.0
  slippage_bps: 0.0
  borrow_bps_annual: 0.0

risk:
  max_abs_weight: null
  beta_neutralize: false
  market_gvkey: null
  beta_lookback_rows: 252
  sector_neutralize: false

events:
  enabled: true
  earnings_window_days: [-1, 0, 1]
  large_customer_return_quantile: 0.95
  high_vol_quantile: 0.80

report:
  generate_md: true
  generate_tex: true
  generate_pdf: false
```

Acceptance: all fields should have safe defaults if omitted.

---

## 2. Strategy family abstraction

Create a strategy-family interface in `supply_chain_leadlag/strategy_families.py`.

### Required public function

```python
def run_strategy_family(
    family: str,
    C: pd.DataFrame,
    returns_window: pd.DataFrame,
    returns_forward: pd.DataFrame,
    edges_pit: pd.DataFrame,
    cluster_labels: pd.Series | None = None,
    q: float = 0.2,
    **kwargs,
) -> dict:
    """Run one strategy family for one rebalance window.

    Returns a dictionary containing at least:
    - daily_returns: pd.Series
    - weights: pd.DataFrame
    - scores: pd.DataFrame or pd.Series
    - metadata: dict
    """
```

### Required families

```text
supplier_pressure
globalrank
metacluster
clusterrank
```

Each family must apply weights on the next trading day by default.

---

## 3. Supplier Pressure strategy

This should reuse the existing supplier-pressure code.

Formula:

```math
s_d = C^\top r_d.
```

Then long top `q` suppliers and short bottom `q` suppliers by `s_d`. Apply weights on day `d+1`.

Rules:

- Customers generate the signal.
- Suppliers are the traded laggers.
- No future returns may be used to compute `s_d`.
- If `hybrid_alpha` is provided, use the hybrid matrix before signal generation.

Outputs per rebalance:

- `scores`: daily supplier scores
- `weights`: daily supplier weights
- `daily_returns`: strategy returns

---

## 4. GlobalRank strategy

At rebalance date `T`:

1. Compute `S = C - C.T`.
2. Compute global scores using one selected rank method:
   - `leadingness`: row sum of `S`
   - `spectral`: leading Hermitian eigenvector of `H = 1j * S`
   - `cluster`: existing cluster rank if needed
   - `cluster_eigen`: existing cluster eigen rank if needed
3. Construct long-short portfolio from global scores.

Initial implementation:

```text
long top q by score, short bottom q by score
hold until next rebalance
```

Optional signal-modulated implementation can be added later.

Outputs:

- `globalrank_scores.csv`
- `globalrank_daily_returns.csv`
- `globalrank_weights.parquet`

---

## 5. MetaCluster strategy

Create or expose implementation in `supply_chain_leadlag/metacluster_strategy.py`.

At rebalance date `T`:

1. Estimate `C_T`.
2. Get cluster labels by selected clustering method.
3. Build cluster returns:

```math
R_{c,t} = \frac{1}{|C_c|}\sum_{i\in C_c} r_{i,t}.
```

4. Compute cluster meta-flow:

```math
F_{ab} = \frac{1}{|C_a||C_b|}\sum_{i\in C_a,j\in C_b}(C_{ij}-C_{ji}).
```

5. Identify leading and lagging cluster pairs.
6. Use leading cluster return at `t` to trade lagging cluster return at `t+1`.

Suggested simple version:

- Keep top 10% strongest positive meta-flow edges `a -> b`.
- For each edge, signal is `R_a,t`.
- Trade cluster `b` at `t+1` with sign of `R_a,t`.
- Average across active meta-flow edges.

Outputs:

- `meta_flow_matrix.parquet`
- `metacluster_edges.csv`
- `metacluster_daily_returns.csv`
- `metacluster_weights.parquet`

---

## 6. ClusterRank strategy

Create or expose implementation in `supply_chain_leadlag/clusterrank_strategy.py`.

At rebalance date `T`:

1. Estimate `C_T`.
2. Get cluster labels.
3. Within each cluster `c`, compute local skew:

```math
S^{(c)} = C^{(c)} - (C^{(c)})^\top.
```

4. Compute local leadingness:

```math
\ell_i^{local} = \sum_{j\in c} S^{(c)}_{ij}.
```

5. Select local leaders and local laggers inside each cluster.
6. Use local leader average return as signal.
7. Trade laggers only.

Suggested formula:

For cluster `c`:

```math
Signal_{c,t} = \frac{1}{|L_c|}\sum_{i\in L_c} r_{i,t}.
```

Then trade laggers:

```math
R^{c}_{t+1} = Signal_{c,t}\left(\bar r_{TopLag_c,t+1} - \bar r_{BotLag_c,t+1}\right).
```

Aggregate across clusters equally or volatility-weighted.

Outputs:

- `clusterrank_local_scores.csv`
- `clusterrank_daily_returns.csv`
- `clusterrank_weights.parquet`

---

## 7. Clustering interface

Create `supply_chain_leadlag/clustering_methods.py`.

### Required public function

```python
def get_cluster_labels(
    C: pd.DataFrame,
    method: str,
    n_clusters: int = 10,
    side_info: pd.DataFrame | None = None,
    sector_map: pd.DataFrame | None = None,
    random_state: int = 42,
    **kwargs,
) -> pd.Series:
    """Return cluster label for each node in C.index/C.columns."""
```

### Required methods

#### `sector`

Use provided sector/SIC/GICS labels. If no sector map is available, raise a clear error and skip method in runner.

#### `supply_community`

Cluster using the structural supply-chain graph only. Basic implementation can use connected components or spectral clustering on structural adjacency.

#### `symmetric_spectral`

Use:

```math
A_{sym} = |C| + |C^\top|.
```

Run spectral clustering on relationship strength, ignoring direction.

#### `hermitian`

Use:

```math
H = i(C - C^\top).
```

Use leading eigenvectors of the Hermitian matrix as embedding. Convert complex eigenvectors into real features by concatenating real and imaginary parts. Run k-means.

#### `signed`

Use positive and negative parts of `C` or `S`. A first implementation may use signed Laplacian or a SPONGE-like generalized eigenproblem if already available; otherwise implement a simple signed spectral baseline and mark it clearly.

#### `hybrid_prior`

Build:

```math
C^{hybrid} = \alpha C^{data} + (1-\alpha) C^{supply}.
```

Then run `hermitian` or `symmetric_spectral` on the hybrid matrix.

Outputs:

- cluster labels per rebalance
- cluster sizes
- cluster stability metrics

---

## 8. Hybrid alpha sweep

Create script:

```bash
python scripts/run_hybrid_alpha_sweep.py --config config/research.yaml
```

For each alpha in `hybrid_alpha_grid`:

1. Build hybrid matrix.
2. Run Supplier Pressure.
3. Run GlobalRank.
4. Run MetaCluster and ClusterRank if clustering enabled.
5. Save performance metrics and stability metrics.

Output:

```text
results/final_research/hybrid_alpha_sweep.csv
results/final_research/plots/hybrid_alpha_sweep.png
```

Required columns:

```text
alpha, strategy_family, cluster_method, edge_score, ann_return, ann_vol, sharpe, max_drawdown, turnover, net_sharpe
```

---

## 9. Event-conditioned analysis

Create module:

```text
supply_chain_leadlag/events.py
```

### Required event labels

Add daily flags for:

```text
all_days
customer_earnings_window
supplier_earnings_window
large_customer_return
nonzero_exposure
high_vol_regime
```

### Required function

```python
def add_event_flags(
    panel_or_signals: pd.DataFrame,
    earnings_calendar: pd.DataFrame | None,
    returns: pd.DataFrame,
    edges: pd.DataFrame,
    config: dict,
) -> pd.DataFrame:
    """Add event-condition columns for conditional regressions/backtests."""
```

### Panel event tests

For each condition, rerun forward/reverse panel regressions or filter existing panel and report coefficients.

### Backtest event tests

For each condition, either:

- trade only on event-condition days, or
- report PnL contribution conditional on event days.

Output:

```text
results/final_research/event_conditioned_panel.csv
results/final_research/event_conditioned_backtest.csv
results/final_research/plots/event_conditioned_beta.png
results/final_research/plots/event_conditioned_sharpe.png
```

---

## 10. Cluster stability and eigenspace drift

Create module:

```text
supply_chain_leadlag/stability.py
```

Required metrics:

### Adjusted Rand Index

Compare cluster labels between consecutive rebalances.

Output column:

```text
ari_prev
```

### Projection distance / eigenspace drift

For eigenvector matrix `V_t`:

```math
d_t = \|V_tV_t^\top - V_{t-1}V_{t-1}^\top\|_F.
```

For complex eigenvectors, use conjugate transpose:

```math
d_t = \|V_tV_t^* - V_{t-1}V_{t-1}^*\|_F.
```

Output column:

```text
eigenspace_drift
```

Outputs:

```text
results/final_research/cluster_stability.csv
results/final_research/plots/cluster_stability_ari.png
results/final_research/plots/eigenspace_drift.png
```

---

## 11. Unified runner

Create script:

```bash
python scripts/run_final_research_pipeline.py --config config/research.yaml
```

This script should run:

1. Data loading and validation
2. Panel forward/reverse validation
3. Matrix construction diagnostics
4. Strategy family comparison
5. Clustering method comparison
6. Hybrid alpha sweep
7. Event-conditioned analysis
8. Stability analysis
9. Report generation

It should be safe to run a quick smoke version:

```bash
python scripts/run_final_research_pipeline.py --config config/research.yaml --max-rebalances 3 --quick
```

---

## 12. Final report generation

Create:

```bash
python scripts/make_final_report.py --results-dir results/final_research
```

Generate:

```text
results/final_research/report/final_report.md
results/final_research/report/final_report.tex
```

PDF generation is optional. If LaTeX is not available, skip PDF with a warning.

---

## 13. Testing requirements

Add or update tests:

### PIT tests

- No edge with date after rebalance is used.
- No returns after rebalance window are used to estimate `C`.
- Weights from day `d` earn returns on day `d+1`, not day `d`.

### Orientation tests

- Positive customer shock should create positive supplier pressure when `C[j,i] > 0`.
- Supplier-pressure strategy should trade suppliers only.

### Clustering tests

- `get_cluster_labels` returns labels for every node.
- Hermitian method handles complex eigenvectors correctly.
- Symmetric and Hermitian methods produce deterministic labels with fixed random seed.

### Strategy tests

- Each strategy family returns daily returns and weights.
- Weights sum to approximately zero for long-short strategies.
- No NaNs in output returns after warmup.

### Output tests

- Quick pipeline creates all required CSV files.
- Report generator runs even when some optional plots are missing.

---

## 14. Acceptance criteria

The implementation is complete only when all are true:

```text
1. pytest passes.
2. Quick run completes with --max-rebalances 3.
3. Full or partial run writes results/final_research/.
4. summary_metrics.csv includes all requested strategy families.
5. cluster_method_comparison.csv includes all available clustering methods.
6. hybrid_alpha_sweep.csv includes alpha = 0, .25, .5, .75, 1.
7. event_conditioned_panel.csv and/or event_conditioned_backtest.csv are created when event data exists.
8. cluster_stability.csv includes ARI and eigenspace drift.
9. final_report.md and final_report.tex are generated.
10. Supplier-pressure uses customers only as signal generators and trades suppliers only.
11. All backtests are point-in-time.
```

---

## 15. Suggested implementation order

1. Add `research.yaml`.
2. Add clustering interface.
3. Add strategy-family wrapper using existing supplier-pressure and rank-factor logic.
4. Implement MetaCluster strategy.
5. Implement ClusterRank strategy.
6. Add clustering method comparison runner.
7. Add hybrid alpha sweep.
8. Add event-conditioned analysis.
9. Add stability metrics.
10. Add final pipeline runner.
11. Add report generator.
12. Add tests.

Do not optimize performance until the smoke pipeline is correct and PIT-safe.
