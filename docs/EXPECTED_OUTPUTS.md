# Supply-Chain Lead–Lag Expected Outputs and Final Deliverables

This document defines what the final project should produce. It is both a checklist for the coding agent and a guide for the final research report.

---

## 1. Final results directory

All final outputs should be written under:

```text
results/final_research/
```

Expected structure:

```text
results/final_research/
  config_used.yaml
  run_metadata.json

  summary_metrics.csv
  panel_forward_reverse.csv
  horizon_decay.csv
  spectral_summary.csv
  strategy_family_comparison.csv
  cluster_method_comparison.csv
  hybrid_alpha_sweep.csv
  event_conditioned_panel.csv
  event_conditioned_backtest.csv
  cluster_stability.csv
  turnover_costs.csv
  factor_exposure_alpha.csv

  daily_returns.csv
  cumulative_returns.csv
  drawdowns.csv
  holdings_or_weights.parquet

  matrices/
    C_last.parquet
    S_last.parquet
    H_spectrum_last.csv
    meta_flow_matrix_last.parquet

  clusters/
    cluster_labels_by_rebalance.parquet
    cluster_sizes.csv
    cluster_stability_by_method.csv

  plots/
    cumulative_pnl_by_strategy.png
    drawdown_by_strategy.png
    sharpe_by_method.png
    forward_vs_reverse_coefficients.png
    horizon_decay_beta.png
    heatmap_C_sorted_by_cluster.png
    meta_flow_network.png
    cluster_stability_ari.png
    eigenspace_drift.png
    hybrid_alpha_sweep.png
    event_conditioned_beta.png
    event_conditioned_sharpe.png
    turnover_by_strategy.png

  report/
    final_report.md
    final_report.tex
    final_report.pdf   # optional
```

If a file cannot be produced because optional input data is missing, the runner should write a warning into `run_metadata.json` and continue.

---

## 2. Required metadata

### `config_used.yaml`

A copy of the resolved config after applying CLI overrides.

### `run_metadata.json`

Required fields:

```json
{
  "run_timestamp": "YYYY-MM-DD HH:MM:SS",
  "git_commit": "optional",
  "python_version": "...",
  "data_start": "YYYY-MM-DD",
  "data_end": "YYYY-MM-DD",
  "n_return_assets": 0,
  "n_edge_rows": 0,
  "n_unique_customers": 0,
  "n_unique_suppliers": 0,
  "n_rebalances": 0,
  "pit_edge_date_col": "filing_date",
  "warnings": []
}
```

---

## 3. Core result tables

### 3.1 `summary_metrics.csv`

One row per strategy / baseline combination.

Required columns:

```text
strategy_family
cluster_method
edge_score
hybrid_alpha
baseline_type
ann_return
ann_vol
sharpe
max_drawdown
calmar
hit_rate
avg_daily_return
avg_turnover
gross_sharpe
net_sharpe
total_cost_bps
n_days
n_rebalances
```

Expected rows should include at least:

```text
supplier_pressure
supplier_pressure_random
supplier_pressure_momentum
supplier_pressure_structural
supplier_pressure_equal_weight
globalrank
metacluster
clusterrank
```

If some methods are skipped, include a `status` or `warning` field.

---

### 3.2 `panel_forward_reverse.csv`

One row per horizon and direction.

Required columns:

```text
direction
horizon
beta
std_error
t_stat
p_value
n_obs
n_entities
fixed_effects
clustered_se
condition
```

Expected directions:

```text
forward_customer_to_supplier
reverse_supplier_to_customer
```

Expected horizons:

```text
1, 2, 3, 4, 5
```

---

### 3.3 `horizon_decay.csv`

Summarizes effect decay across horizons.

Required columns:

```text
condition
horizon
beta
t_stat
p_value
economic_magnitude_bps
```

Optional columns:

```text
decay_lambda
half_life_days
```

This table should support the plot `horizon_decay_beta.png`.

---

### 3.4 `strategy_family_comparison.csv`

Compares the four main strategy families.

Required columns:

```text
strategy_family
cluster_method
edge_score
ann_return
ann_vol
sharpe
max_drawdown
avg_turnover
net_sharpe
n_traded_assets_avg
notes
```

Expected strategy families:

```text
supplier_pressure
globalrank
metacluster
clusterrank
```

Purpose:

> Answer whether higher-order network organization improves over direct supplier-pressure trading.

---

### 3.5 `cluster_method_comparison.csv`

Compares clustering methods, mainly for MetaCluster and ClusterRank.

Required columns:

```text
strategy_family
cluster_method
n_clusters
edge_score
hybrid_alpha
ann_return
ann_vol
sharpe
max_drawdown
avg_turnover
cluster_ari_mean
eigenspace_drift_mean
n_rebalances
```

Expected cluster methods:

```text
sector
supply_community
symmetric_spectral
hermitian
signed
hybrid_prior
```

Purpose:

> Answer whether direction-aware clustering improves trading and stability.

---

### 3.6 `hybrid_alpha_sweep.csv`

Compares pure return-based, pure supply-chain, and hybrid matrices.

Required columns:

```text
alpha
strategy_family
cluster_method
edge_score
ann_return
ann_vol
sharpe
max_drawdown
avg_turnover
net_sharpe
cluster_ari_mean
eigenspace_drift_mean
```

Expected alpha values:

```text
0.0, 0.25, 0.5, 0.75, 1.0
```

Interpretation:

```text
alpha = 0.0 → pure supply-chain structure
alpha = 1.0 → pure return-based lead-lag
0 < alpha < 1 → hybrid
```

---

### 3.7 `event_conditioned_panel.csv`

Panel results by event condition.

Required columns:

```text
condition
direction
horizon
beta
std_error
t_stat
p_value
n_obs
economic_magnitude_bps
```

Expected conditions:

```text
all_days
nonzero_exposure
customer_earnings_window
supplier_earnings_window
large_customer_return
high_vol_regime
```

Purpose:

> Answer whether supply-chain diffusion is stronger around customer information events.

---

### 3.8 `event_conditioned_backtest.csv`

Backtest metrics by event condition.

Required columns:

```text
condition
strategy_family
ann_return
ann_vol
sharpe
max_drawdown
avg_turnover
n_trade_days
net_sharpe
```

---

### 3.9 `cluster_stability.csv`

Required columns:

```text
rebalance_date
cluster_method
strategy_family
n_clusters
ari_prev
eigenspace_drift
n_assets
largest_cluster_size
smallest_cluster_size
```

Purpose:

> Answer whether supply-chain or hybrid-prior clusters are more stable than pure return-based clusters.

---

### 3.10 `turnover_costs.csv`

Required columns:

```text
strategy_family
cluster_method
edge_score
gross_return
net_return
total_turnover
total_cost_bps
commission_cost_bps
slippage_cost_bps
borrow_cost_bps
gross_sharpe
net_sharpe
```

---

## 4. Matrix and cluster artifacts

### `matrices/C_last.parquet`

Last estimated lead-lag matrix.

Rows and columns should be firm identifiers, preferably `gvkey`.

### `matrices/S_last.parquet`

Skew-symmetric directional matrix:

```math
S = C - C^\top.
```

### `matrices/H_spectrum_last.csv`

Required columns:

```text
eigen_index
eigenvalue
abs_eigenvalue
```

### `matrices/meta_flow_matrix_last.parquet`

Cluster-level net-flow matrix for MetaCluster.

### `clusters/cluster_labels_by_rebalance.parquet`

Required columns:

```text
rebalance_date
gvkey
cluster_method
cluster_label
strategy_family
```

### `clusters/cluster_sizes.csv`

Required columns:

```text
rebalance_date
cluster_method
cluster_label
cluster_size
```

---

## 5. Required plots

### 5.1 `cumulative_pnl_by_strategy.png`

Lines:

- Supplier Pressure
- GlobalRank
- MetaCluster
- ClusterRank
- Momentum baseline
- Random baseline
- Equal-weight baseline if applicable

### 5.2 `drawdown_by_strategy.png`

Drawdown curves for main strategies.

### 5.3 `sharpe_by_method.png`

Bar chart of Sharpe by:

```text
strategy_family × cluster_method
```

### 5.4 `forward_vs_reverse_coefficients.png`

Coefficient plot over horizons for forward and reverse regressions.

### 5.5 `horizon_decay_beta.png`

Shows `beta_h` across horizons and optional fitted exponential decay.

### 5.6 `heatmap_C_sorted_by_cluster.png`

Heatmap of `C` or `S`, sorted by cluster label and within-cluster leadingness.

### 5.7 `meta_flow_network.png`

Directed graph of cluster-to-cluster flows.

Nodes: clusters.  
Edges: net flow.  
Thicker edges: stronger flow.

### 5.8 `cluster_stability_ari.png`

ARI across consecutive rebalances by cluster method.

### 5.9 `eigenspace_drift.png`

Projection-distance drift across rebalances.

### 5.10 `hybrid_alpha_sweep.png`

Sharpe and turnover vs `alpha`.

### 5.11 `event_conditioned_beta.png`

Forward beta by condition.

### 5.12 `event_conditioned_sharpe.png`

Backtest Sharpe by event condition.

### 5.13 `turnover_by_strategy.png`

Average turnover by strategy.

---

## 6. Final report deliverables

### `report/final_report.md`

Should contain:

1. Abstract
2. Research questions
3. Data and PIT construction
4. Customer-pressure signal
5. Panel predictability results
6. Directional reverse placebo
7. Lead-lag matrix construction
8. Network structure and spectral diagnostics
9. Strategy family comparison
10. Clustering method comparison
11. Hybrid alpha sweep
12. Event-conditioned diffusion
13. Transaction costs and risk controls
14. Limitations
15. Conclusion

### `report/final_report.tex`

Same structure as markdown, LaTeX-ready.

### `report/final_report.pdf`

Optional. Generate only if LaTeX is available.

---

## 7. Final research-question answer table

The report should include this exact table, filled from results.

| Research question | Evidence file | Decision |
|---|---|---|
| Does customer pressure predict supplier returns? | `panel_forward_reverse.csv` | Yes / No / Mixed |
| Is the effect directional? | `panel_forward_reverse.csv` | Yes / No / Mixed |
| Is the effect tradable? | `summary_metrics.csv` | Yes / No / Mixed |
| Does higher-order network structure help? | `strategy_family_comparison.csv` | Yes / No / Mixed |
| Does direction-aware clustering help? | `cluster_method_comparison.csv` | Yes / No / Mixed |
| Does supply-chain structure stabilize return-based lead-lag? | `hybrid_alpha_sweep.csv`, `cluster_stability.csv` | Yes / No / Mixed |
| Is diffusion event-amplified? | `event_conditioned_panel.csv`, `event_conditioned_backtest.csv` | Yes / No / Mixed |

---

## 8. Minimum viable final run

A minimum final run is acceptable if it produces:

```text
summary_metrics.csv
panel_forward_reverse.csv
strategy_family_comparison.csv
cluster_method_comparison.csv
hybrid_alpha_sweep.csv
cluster_stability.csv
final_report.md
final_report.tex
```

and these plots:

```text
cumulative_pnl_by_strategy.png
forward_vs_reverse_coefficients.png
sharpe_by_method.png
hybrid_alpha_sweep.png
```

Event-conditioned files may be marked optional if earnings calendar data is unavailable or incomplete.

---

## 9. Quality checks before submission

Before considering the project complete, verify:

```text
[ ] All backtests use PIT edge filtering.
[ ] Matrix C is estimated only from trailing returns.
[ ] Supplier-pressure signal uses C.T @ r_t and earns return at t+1.
[ ] Supplier-pressure trades suppliers only.
[ ] Reverse-direction regression is included.
[ ] Random and momentum baselines are included.
[ ] Costs are reported separately from gross returns.
[ ] Cluster methods are compared under the same calendar and universe.
[ ] Hybrid alpha sweep includes both alpha = 0 and alpha = 1.
[ ] Final report includes research-question answer table.
```

---

## 10. Expected final narrative

The final project should be able to say one of the following:

### Strong positive outcome

> Customer shocks predict supplier returns in a directionally asymmetric way. The effect is tradable in a rolling PIT supplier-pressure strategy, improves when organized through direction-aware cluster structure, and is strengthened by supply-chain priors and event conditioning.

### Mixed outcome

> Customer-to-supplier predictability is statistically visible, but trading performance is sensitive to costs, clustering method, and event conditioning. Supply-chain priors improve stability, but alpha is concentrated in specific regimes or event windows.

### Negative but still valuable outcome

> Panel evidence exists but does not survive tradable implementation after costs. The project still contributes a rigorous PIT framework for testing supply-chain information diffusion and shows which modeling assumptions fail.
