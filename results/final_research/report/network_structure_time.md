# Network structure & time variation (presentation notes)

## Network structure & clustering

Setup: at each BME rebalance, estimate return-based \(C\) (`tstat_diff`), cluster nodes with **spectral embedding** methods (`signed`, `hermitian`, `symmetric_spectral`) plus **sector** baseline. Decompose customer pressure on supplier \(i\):

- **Intra:** \(\sum_{j: L(j)=L(i)} w_{ji} r_{j,t}\)
- **Inter:** \(\sum_{j: L(j)\neq L(i)} w_{ji} r_{j,t}\)

Regress \(r_{i,t+1}\) on intra and inter signals (joint specification).

| Cluster method | β intra | t intra | β inter | t inter | R² | share \|intra\| |
|----------------|---------|---------|---------|---------|-----|----------------|
| signed | -0.0228 | -0.67 | 0.0415 | 4.25 | 0.0001 | 11.35% |
| hermitian | 0.0347 | 1.72 | 0.0370 | 3.49 | 0.0001 | 14.71% |
| symmetric_spectral | 0.0365 | 3.90 | 0.0565 | 0.03 | 0.0001 | 98.22% |
| sector | 0.0395 | 3.97 | 0.0136 | 0.50 | 0.0001 | 57.59% |

**Univariate (full sample):**

        cluster_method      period   n_obs  n_suppliers  mean_share_intra        spec  beta_intra  beta_inter  beta_total    t_stat            r2  t_intra  t_inter
0               signed  all_sample  169832          273          0.113459  intra_only   -0.021784         NaN         NaN -0.642243  2.487057e-06      NaN      NaN
1               signed  all_sample  169832          273          0.113459  inter_only         NaN    0.041442         NaN  4.246758  1.062412e-04      NaN      NaN
16           hermitian  all_sample  169832          273          0.147123  intra_only    0.034988         NaN         NaN  1.736398  1.781149e-05      NaN      NaN
17           hermitian  all_sample  169832          273          0.147123  inter_only         NaN    0.037046         NaN  3.496770  7.205099e-05      NaN      NaN
32  symmetric_spectral  all_sample  169832          273          0.982229  intra_only    0.036468         NaN         NaN  3.896012  8.942740e-05      NaN      NaN
33  symmetric_spectral  all_sample  169832          273          0.982229  inter_only         NaN    0.056359         NaN  0.027282  6.268827e-08      NaN      NaN
46              sector  all_sample  169832          273          0.575901  intra_only    0.039499         NaN         NaN  3.965284  9.263335e-05      NaN      NaN
47              sector  all_sample  169832          273          0.575901  inter_only         NaN    0.013612         NaN  0.497363  1.514877e-06      NaN      NaN

**Edge-level check** (`signed`, subsample of rebalances):

  cluster_method   edge_type      beta    t_stat        r2     n_obs  n_rebalances_used
0         signed  intra_edge -0.021655 -0.757505  0.000022   26625.0                156
1         signed  inter_edge  0.041841  4.610891  0.000102  208717.0                156

### Interpretation guide

- If **β_intra > β_inter** (joint) and intra-only t-stat is larger → lead–lag is **concentrated among firms grouped by the directed network embedding** (information travels within diffusion communities).
- If **sector** shows weak intra but network methods show strong intra → effect is **network-specific**, not just industry co-movement.
- **symmetric_spectral** (direction-blind) vs **signed/hermitian** gap → direction-aware clustering matters.

## Time variation (crisis vs non-crisis)

Crisis windows: gfc, euro_debt, covid_crash, rate_shock_2022. **non_crisis** = all other days.

**Supplier-pressure Sharpe by period:**

     strategy_family           period    sharpe  ann_return   ann_vol  n_days  cum_return
0  supplier_pressure       all_sample  0.521166    0.111417  0.213785    3774    2.746721
1  supplier_pressure        euro_debt       NaN    0.000000  0.000000     107    0.000000
2  supplier_pressure      covid_crash -3.017497   -1.390025  0.460655      50   -0.257920
3  supplier_pressure  rate_shock_2022 -0.044155   -0.011189  0.253416     209   -0.035505
4  supplier_pressure       non_crisis  0.692643    0.144463  0.208567    3408    4.234806

**Panel predictability by period** (joint intra/inter, `signed`):

   cluster_method           period   n_obs  n_suppliers  mean_share_intra               spec  beta_intra  beta_inter  beta_total  t_stat        r2   t_intra   t_inter
3          signed       all_sample  169832          273          0.113459  joint_intra_inter   -0.022828    0.041489         NaN     NaN  0.000109 -0.673023  4.251510
7          signed      covid_crash    1713           40          0.153168  joint_intra_inter   -1.053474   -0.231754         NaN     NaN  0.012691 -4.322196 -1.816637
11         signed  rate_shock_2022    2965           17          0.159528  joint_intra_inter    0.341005    0.134420         NaN     NaN  0.007540  3.508282  3.193592
15         signed       non_crisis  165154          271          0.112220  joint_intra_inter    0.051096    0.045007         NaN     NaN  0.000136  1.370478  4.522024

### Interpretation guide

- Compare **non_crisis** vs crisis subperiods: amplification during stress supports **slow diffusion under friction**; collapse suggests the signal is a calm-market artifact.
- Use **GFC** and **COVID** as distinct episodes — different supply-chain disruption mechanisms.

Plots: `plots/intra_inter_betas_by_cluster_method.png`, `plots/crisis_sharpe_*.png`.
