# Supply Chain Lead–Lag: Final Research Report

## Strategy family comparison

  strategy_family cluster_method edge_score  ann_return  ann_vol    sharpe  max_drawdown  avg_turnover  net_sharpe  n_traded_assets_avg  notes
supplier_pressure      hermitian tstat_diff    0.041367 0.085974  0.481159     -0.183721           NaN    0.481159                  NaN    NaN
       globalrank      hermitian tstat_diff    0.015893 0.080985  0.196249     -0.291659           NaN    0.196249                  NaN    NaN
      metacluster      hermitian tstat_diff   -0.026382 0.110226 -0.239349     -0.530480           NaN   -0.239349                  NaN    NaN
      clusterrank      hermitian tstat_diff   -0.031399 0.057055 -0.550340     -0.449463           NaN   -0.550340                  NaN    NaN


## Summary metrics

  strategy_family cluster_method edge_score  hybrid_alpha baseline_type  ann_return  ann_vol    sharpe  max_drawdown  net_sharpe  calmar  hit_rate  avg_daily_return  avg_turnover  gross_sharpe  total_cost_bps  n_days  n_rebalances
supplier_pressure      hermitian tstat_diff           NaN          main    0.041367 0.085974  0.481159     -0.183721    0.481159     NaN       NaN               NaN           NaN      0.481159             0.0    3774            12
       globalrank      hermitian tstat_diff           NaN          main    0.015893 0.080985  0.196249     -0.291659    0.196249     NaN       NaN               NaN           NaN      0.196249             0.0    3774            12
      metacluster      hermitian tstat_diff           NaN          main   -0.026382 0.110226 -0.239349     -0.530480   -0.239349     NaN       NaN               NaN           NaN     -0.239349             0.0    3774            12
      clusterrank      hermitian tstat_diff           NaN          main   -0.031399 0.057055 -0.550340     -0.449463   -0.550340     NaN       NaN               NaN           NaN     -0.550340             0.0    3774            12


## Cluster method comparison (top 10 by Sharpe)

strategy_family     cluster_method  n_clusters edge_score  hybrid_alpha  ann_return  ann_vol    sharpe  max_drawdown  avg_turnover  cluster_ari_mean  eigenspace_drift_mean  n_rebalances
    clusterrank   supply_community          10 tstat_diff           NaN    0.052054 0.102657  0.507065     -0.208904           NaN          0.398399                1.77446            12
    metacluster             signed          10 tstat_diff           NaN    0.050019 0.104267  0.479720     -0.400615           NaN          0.731133                1.77446            12
    clusterrank             signed          10 tstat_diff           NaN    0.026855 0.070053  0.383356     -0.152914           NaN          0.731133                1.77446            12
    clusterrank       hybrid_prior          10 tstat_diff           NaN    0.000106 0.056360  0.001877     -0.363305           NaN          0.215135                1.77446            12
    metacluster       hybrid_prior          10 tstat_diff           NaN   -0.005862 0.169611 -0.034564     -0.619071           NaN          0.215135                1.77446            12
    clusterrank symmetric_spectral          10 tstat_diff           NaN   -0.012532 0.099479 -0.125974     -0.364925           NaN          0.372646                1.77446            12
    metacluster          hermitian          10 tstat_diff           NaN   -0.026382 0.110226 -0.239349     -0.530480           NaN          0.101397                1.77446            12
    clusterrank          hermitian          10 tstat_diff           NaN   -0.031399 0.057055 -0.550340     -0.449463           NaN          0.101397                1.77446            12
    metacluster             sector          10 tstat_diff           NaN         NaN      NaN       NaN           NaN           NaN               NaN                    NaN             0
    clusterrank             sector          10 tstat_diff           NaN         NaN      NaN       NaN           NaN           NaN               NaN                    NaN             0


## Configuration

- Edge score: `tstat_diff` · Rebalance: `BME`
- Default cluster (Step 4): `hermitian`
- GlobalRank method: `spectral`


See `START_HERE.md` in the results root for a shorter guide.
