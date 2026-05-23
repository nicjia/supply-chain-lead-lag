# Start here — research results

This folder is the **main entry point** after a pipeline run.

## Strategy families (Step 4)

All four families use the same return-based \(C\) each rebalance. **Clustering for metacluster / clusterrank:** `hermitian` (10 clusters). `globalrank` uses `spectral` scores; no clusters.

  strategy_family cluster_method    sharpe  ann_return  ann_vol  max_drawdown
supplier_pressure      hermitian  0.481159    0.041367 0.085974     -0.183721
       globalrank      hermitian  0.196249    0.015893 0.080985     -0.291659
      metacluster      hermitian -0.239349   -0.026382 0.110226     -0.530480
      clusterrank      hermitian -0.550340   -0.031399 0.057055     -0.449463

**Best Sharpe:** `supplier_pressure` (0.481)

**Key plots:** `plots/strategy_families_dashboard.png`, `plots/cumulative_pnl_by_strategy.png`

**Key tables:** `strategy_family_comparison.csv`, `daily_returns.csv`, `summary_metrics.csv`


## Where everything else lives

| If you need… | Open… |
|-------------|-------|
| Cluster method sweep (Step 5) | `cluster_method_comparison.csv`, `plots/cluster_sweep_dashboard.png` |
| Deep dive / diagnostics | `matrices/`, `clusters/`, `run_metadata.json` |
| Full narrative report | `report/final_report.md` |


*Pipeline steps in this run:* `plots, report` · plot profile: `full`
