# Supply Chain Lead–Lag: Research Report (preliminary artifact set)

> **Deliverable status:** Preliminary results — backtest, clustering, and hybrid sweep are in good shape; panel FE/clustered SE, events, turnover/costs, and full rebalance calendar are **not** final. See [Appendix B](#appendix-b-submission-readiness-preliminary-vs-final).

## 1. Abstract
Point-in-time (PIT) supply-chain lead–lag study on Compustat customer–supplier links and daily returns (2010-01-04–2024-12-31). We compare four tradable families, sweep clustering methods, and blend \(C_{\text{data}}\) with \(C_{\text{supply}}\) via hybrid **α** (`signed` clusters; `tstat_diff`).

**Headline (defensible now):** The pipeline runs end-to-end and produces meaningful differences across families, cluster methods, and α. **Supplier pressure** is the most stable tradable baseline; **clusterrank** benefits from supply-community clustering; hybrid α materially affects **globalrank**. **Metacluster** can show high Sharpe but extreme drawdown — treat as unstable. Claims on statistical predictability, clean directionality, event amplification, and net-of-cost performance require additional reruns.

## 2. Research questions

Decisions use **conservative** labels when artifacts are pooled-OLS or capped at 12 rebalances.

                                           Research question                  Evidence file                                                                    Decision
            Does customer pressure predict supplier returns?      panel_forward_reverse.csv                              Preliminary yes; needs FE + clustered-SE rerun
                                  Is the effect directional?      panel_forward_reverse.csv Inconclusive in this artifact; rerun reverse placebo with FE + clustered SE
                                     Is the effect tradable?            summary_metrics.csv                                                             Preliminary yes
                   Does higher-order network structure help? strategy_family_comparison.csv                                                             Preliminary yes
                       Does direction-aware clustering help?  cluster_method_comparison.csv                                                                   Mixed yes
Does supply-chain structure stabilize return-based lead-lag?         hybrid_alpha_sweep.csv                                                             Preliminary yes
                               Is diffusion event-amplified? event_conditioned_backtest.csv                                                                     Not run

## 3. Data and PIT construction
- Returns: `data/returns_with_gvkey.parquet` · Edges: `data/merged_edges.csv`
- PIT date column: `filing_date` · Rebalance: `BME`
- Window: 2010-01-04–2024-12-31 · 12 rebalances · 5461 return assets · 17153 PIT edge rows
- Pipeline steps in last run: `cluster_sweep, load, report`

## 4. Customer-pressure signal
At each day \(d\), signal \(s = C^\top r_d\) (customer pressure on suppliers); long–short **suppliers only** with return earned on \(d+1\) (`supplier_pressure` family).

Rolling backtest (signed cluster label for bookkeeping, no clustering in signal): Sharpe **0.48**, ann. return 4.1%, max DD -18.4%.

## 5. Panel predictability results
**Panel quality warning:** This run used `none_pooled` with `clustered_se=False` and missing `std_error` / `p_value` on some rows. Rebuild `data/leadlag_panel_forward.parquet` and `data/leadlag_panel_reverse.parquet` (`scripts/build_leadlag_panel.py`), install `linearmodels`, then rerun `--steps load,panel` for firm + time FE and entity-clustered standard errors.

                   direction  horizon      beta  std_error    t_stat  p_value    n_obs  n_entities fixed_effects  clustered_se condition
forward_customer_to_supplier        1  0.041264        NaN  7.907461      NaN 633674.0         460   none_pooled         False  all_days
forward_customer_to_supplier        2  0.038797        NaN  7.434801      NaN 633599.0         460   none_pooled         False  all_days
forward_customer_to_supplier        3 -0.012025        NaN -2.304075      NaN 633604.0         461   none_pooled         False  all_days
forward_customer_to_supplier        4 -0.004167        NaN -0.798383      NaN 633530.0         461   none_pooled         False  all_days
forward_customer_to_supplier        5  0.000701        NaN  0.134268      NaN 633454.0         461   none_pooled         False  all_days

Forward horizon 1 shows positive pooled β with large |t|, but **reverse horizons 1–2 also have |t| > 2** in this artifact — directionality is **not** established until FE + clustered SE. Do not cite this table as final regression evidence.

## 6. Directional reverse placebo

**Forward:**

                   direction  horizon      beta  std_error    t_stat  p_value    n_obs  n_entities fixed_effects  clustered_se condition
forward_customer_to_supplier        1  0.041264        NaN  7.907461      NaN 633674.0         460   none_pooled         False  all_days
forward_customer_to_supplier        2  0.038797        NaN  7.434801      NaN 633599.0         460   none_pooled         False  all_days
forward_customer_to_supplier        3 -0.012025        NaN -2.304075      NaN 633604.0         461   none_pooled         False  all_days
forward_customer_to_supplier        4 -0.004167        NaN -0.798383      NaN 633530.0         461   none_pooled         False  all_days
forward_customer_to_supplier        5  0.000701        NaN  0.134268      NaN 633454.0         461   none_pooled         False  all_days

**Reverse:**

                   direction  horizon      beta  std_error    t_stat  p_value    n_obs  n_entities fixed_effects  clustered_se condition
reverse_supplier_to_customer        1  0.008175        NaN  2.548883      NaN 633563.0         460   none_pooled         False  all_days
reverse_supplier_to_customer        2  0.008087        NaN  2.520901      NaN 633375.0         460   none_pooled         False  all_days
reverse_supplier_to_customer        3 -0.005523        NaN -1.721166      NaN 633187.0         460   none_pooled         False  all_days
reverse_supplier_to_customer        4 -0.003465        NaN -1.079393      NaN 632998.0         460   none_pooled         False  all_days
reverse_supplier_to_customer        5 -0.008932        NaN -2.781741      NaN 632809.0         460   none_pooled         False  all_days

**Read with caution:** Reverse placebo is not cleanly insignificant under pooled OLS (e.g. reverse h=1,2 with |t|≈2.5; h=5 with |t|≈−2.8). Rerun with PanelOLS on built panel parquets.

## 7. Lead-lag matrix construction
Rolling \(C\), lookback 504 rows, score `tstat_diff`.

## 8. Network structure and spectral diagnostics
*Optional — run `artifacts` step.*

## 9. Strategy family comparison
**Interpretation (α = 1, signed clusters for meta/clusterrank):**
- **Supplier pressure** (Sharpe 0.48) is the **most stable baseline** (shallower drawdown).
- **Metacluster** matches on Sharpe (0.48) but has **unacceptable path instability** (~−40% max DD here; ~−99% in hybrid sweep) — do not rank it as simply “best.”
- **Clusterrank** (0.38) and **globalrank** (0.20) lag on risk-adjusted return.
- Supplier pressure trades **suppliers only**; network families use global or cluster-local ranks.

  strategy_family cluster_method edge_score  ann_return  ann_vol   sharpe  max_drawdown  avg_turnover  net_sharpe  n_traded_assets_avg  notes
supplier_pressure         signed tstat_diff    0.041367 0.085974 0.481159     -0.183721           NaN    0.481159                  NaN    NaN
      metacluster         signed tstat_diff    0.050019 0.104267 0.479720     -0.400615           NaN    0.479720                  NaN    NaN
      clusterrank         signed tstat_diff    0.026855 0.070053 0.383356     -0.152914           NaN    0.383356                  NaN    NaN
       globalrank         signed tstat_diff    0.015893 0.080985 0.196249     -0.291659           NaN    0.196249                  NaN    NaN

![Families](../plots/strategy_families_dashboard.png)

![Cumulative PnL](../plots/cumulative_pnl_by_strategy.png)

## 10. Clustering method comparison
**Interpretation:** Best cluster×family cell is **clusterrank / supply_community** (Sharpe 0.51). **Signed** clustering works well for metacluster; **supply_community** is best for clusterrank. **Hermitian** and **hybrid_prior** underperform.
- Metacluster: signed Sharpe 0.48 vs hermitian -0.24.

strategy_family   cluster_method  n_clusters edge_score  hybrid_alpha  ann_return  ann_vol   sharpe  max_drawdown  avg_turnover  cluster_ari_mean  eigenspace_drift_mean  n_rebalances
    clusterrank supply_community          10 tstat_diff           NaN    0.052054 0.102657 0.507065     -0.208904           NaN          0.398399                1.77446            12
    metacluster           signed          10 tstat_diff           NaN    0.050019 0.104267 0.479720     -0.400615           NaN          0.731133                1.77446            12
    metacluster           ggroup          10 tstat_diff           NaN    0.051813 0.123175 0.420641     -0.230714           NaN          0.974179                1.77446            12
    clusterrank           signed          10 tstat_diff           NaN    0.026855 0.070053 0.383356     -0.152914           NaN          0.731133                1.77446            12
    metacluster           naics2          10 tstat_diff           NaN    0.034230 0.103587 0.330447     -0.308423           NaN          0.978027                1.77446            12
    metacluster             gind          10 tstat_diff           NaN    0.032965 0.108857 0.302823     -0.350419           NaN          0.970007                1.77446            12
    metacluster             sic2          10 tstat_diff           NaN    0.061017 0.220382 0.276868     -0.517821           NaN          0.972333                1.77446            12
    metacluster          gsubind          10 tstat_diff           NaN    0.014403 0.060514 0.238009     -0.130435           NaN          0.959779                1.77446            12
    clusterrank             sic2          10 tstat_diff           NaN    0.034272 0.177249 0.193357     -0.479803           NaN          0.972333                1.77446            12
    clusterrank             sic4          10 tstat_diff           NaN    0.045730 0.249575 0.183231     -0.640818           NaN          0.966710                1.77446            12
    clusterrank            naics          10 tstat_diff           NaN    0.049869 0.273798 0.182138     -0.707776           NaN          0.964343                1.77446            12
    clusterrank           naics2          10 tstat_diff           NaN    0.016908 0.122295 0.138254     -0.329614           NaN          0.978027                1.77446            12

![Cluster sweep](../plots/cluster_sweep_dashboard.png)

## 11. Hybrid alpha sweep
**Interpretation (signed clustering, full sample):**
- **α = 0** uses only the normalized supply-chain adjacency; **α = 1** uses only return-estimated \(C_{\text{data}}\).
- **supplier pressure:** best Sharpe at **α = 1** (Sharpe 0.52, max DD -0.71).
- **globalrank:** best Sharpe at **α = 0.75** (Sharpe 0.42, max DD -0.44).
- **metacluster:** best Sharpe at **α = 0** (Sharpe 0.28, max DD -0.98).
- **clusterrank:** best Sharpe at **α = 0.75** (Sharpe 0.25, max DD -0.26).
- **Supplier pressure** is strong across α (supply prior ≈ returns-only); **globalrank** is highly α-sensitive (best at α = 0.75, weak at α ∈ {0, 1}).
- **Metacluster** shows high in-sample Sharpe at low α but **~99% drawdown** in cumulative PnL (2020–21 spike/crash); treat as unstable despite positive Sharpe.
- **Clusterrank** improves with more data weight (α = 0.75–1.0) vs pure supply (α = 0).

See `plots/methods_comparison.png` (α = 1 vs best-α) and `plots/hybrid_sharpe_heatmap.png`.

**Best α per family:**

  strategy_family  alpha   sharpe  ann_return  max_drawdown
supplier_pressure   1.00 0.521166    0.111417     -0.710557
       globalrank   0.75 0.422398    0.083280     -0.440753
      metacluster   0.00 0.275460    0.149869     -0.981191
      clusterrank   0.75 0.246663    0.026877     -0.264412

**Full grid:**

 alpha   strategy_family cluster_method edge_score  ann_return  ann_vol    sharpe  max_drawdown  avg_turnover  net_sharpe  hit_rate    calmar  avg_daily_return  n_days  cluster_ari_mean  eigenspace_drift_mean
  0.00       clusterrank         signed tstat_diff   -0.005553 0.109077 -0.050908     -0.508830           NaN   -0.050908  0.339428 -0.010913         -0.000022    3774          0.742510               0.658475
  0.25       clusterrank         signed tstat_diff   -0.000375 0.107771 -0.003479     -0.473718           NaN   -0.003479  0.340223 -0.000792         -0.000001    3774          0.742510               0.974374
  0.50       clusterrank         signed tstat_diff    0.001902 0.107445  0.017702     -0.434711           NaN    0.017702  0.335188  0.004375          0.000008    3774          0.740793               1.168470
  0.75       clusterrank         signed tstat_diff    0.026877 0.108961  0.246663     -0.264412           NaN    0.246663  0.350026  0.101647          0.000107    3774          0.719363               1.229595
  1.00       clusterrank         signed tstat_diff    0.023690 0.132406  0.178922     -0.384330           NaN    0.178922  0.358506  0.061641          0.000094    3774          0.719605               1.291126
  0.00        globalrank         signed tstat_diff   -0.026356 0.213705 -0.123328     -0.698418           NaN   -0.123328  0.383148 -0.037736         -0.000105    3774          0.742510               0.658475
  0.25        globalrank         signed tstat_diff    0.026130 0.207518  0.125919     -0.553925           NaN    0.125919  0.398516  0.047173          0.000104    3774          0.742510               0.974374
  0.50        globalrank         signed tstat_diff    0.034004 0.179572  0.189361     -0.488843           NaN    0.189361  0.401961  0.069560          0.000135    3774          0.740793               1.168470
  0.75        globalrank         signed tstat_diff    0.083280 0.197161  0.422398     -0.440753           NaN    0.422398  0.409645  0.188950          0.000330    3774          0.719363               1.229595
  1.00        globalrank         signed tstat_diff   -0.067516 0.168676 -0.400271     -0.823270           NaN   -0.400271  0.384473 -0.082010         -0.000268    3774          0.719605               1.291126
  0.00       metacluster         signed tstat_diff    0.149869 0.544068  0.275460     -0.981191           NaN    0.275460  0.402226  0.152742          0.000595    3774          0.742510               0.658475
  0.25       metacluster         signed tstat_diff    0.105709 0.547600  0.193041     -0.991651           NaN    0.193041  0.404081  0.106599          0.000419    3774          0.742510               0.974374
  0.50       metacluster         signed tstat_diff    0.142771 0.539081  0.264842     -0.993945           NaN    0.264842  0.403021  0.143641          0.000567    3774          0.740793               1.168470
  0.75       metacluster         signed tstat_diff    0.055902 0.513041  0.108961     -0.998037           NaN    0.108961  0.397986  0.056012          0.000222    3774          0.719363               1.229595
  1.00       metacluster         signed tstat_diff    0.097077 0.509686  0.190465     -0.988833           NaN    0.190465  0.415739  0.098174          0.000385    3774          0.719605               1.291126
  0.00 supplier_pressure         signed tstat_diff    0.114496 0.224661  0.509641     -0.580705           NaN    0.509641  0.399046  0.197168          0.000454    3774          0.742510               0.658475
  0.25 supplier_pressure         signed tstat_diff    0.103483 0.226159  0.457567     -0.661069           NaN    0.457567  0.405935  0.156539          0.000411    3774          0.742510               0.974374
  0.50 supplier_pressure         signed tstat_diff    0.075190 0.220584  0.340869     -0.761728           NaN    0.340869  0.396926  0.098710          0.000298    3774          0.740793               1.168470
  0.75 supplier_pressure         signed tstat_diff    0.081823 0.212156  0.385671     -0.736043           NaN    0.385671  0.397191  0.111165          0.000325    3774          0.719363               1.229595
  1.00 supplier_pressure         signed tstat_diff    0.111417 0.213785  0.521166     -0.710557           NaN    0.521166  0.408850  0.156803          0.000442    3774          0.719605               1.291126

![Methods comparison](../plots/methods_comparison.png)

![Hybrid Sharpe heatmap](../plots/hybrid_sharpe_heatmap.png)

![Hybrid Sharpe curves](../plots/hybrid_alpha_sweep.png)

![Hybrid PnL panels](../plots/hybrid_alpha_sweep_cumulative_pnl.png)

## 12. Event-conditioned diffusion
*Optional — run `events` step.*

## 13. Transaction costs and risk controls
  strategy_family cluster_method edge_score  hybrid_alpha baseline_type  ann_return  ann_vol   sharpe  max_drawdown  net_sharpe  calmar  hit_rate  avg_daily_return  avg_turnover  gross_sharpe  total_cost_bps  n_days  n_rebalances
supplier_pressure         signed tstat_diff           NaN          main    0.041367 0.085974 0.481159     -0.183721    0.481159     NaN       NaN               NaN           NaN      0.481159             0.0    3774            12
       globalrank         signed tstat_diff           NaN          main    0.015893 0.080985 0.196249     -0.291659    0.196249     NaN       NaN               NaN           NaN      0.196249             0.0    3774            12
      metacluster         signed tstat_diff           NaN          main    0.050019 0.104267 0.479720     -0.400615    0.479720     NaN       NaN               NaN           NaN      0.479720             0.0    3774            12
      clusterrank         signed tstat_diff           NaN          main    0.026855 0.070053 0.383356     -0.152914    0.383356     NaN       NaN               NaN           NaN      0.383356             0.0    3774            12

**Not final for trading claims:** `avg_turnover` is NaN and `total_cost_bps` is 0 — gross Sharpe equals net Sharpe. Populate turnover/cost assumptions before submission.

## 14. Limitations
- **Panel:** Current `panel_forward_reverse.csv` may be pooled OLS (`none_pooled`) without clustered SE; not comparable to midterm PanelOLS. Build panel parquets + `pip install linearmodels` + rerun panel.
- **12 rebalances** (if capped): smoke-style; use full calendar for final numbers or state cap explicitly.
- **Metacluster:** High Sharpe with **~−40% to −99%** drawdown — emphasize path instability, not “best family.”
- **Events:** `event_conditioned_*.csv` not produced — diffusion-around-earnings claim is open.
- **Costs / turnover:** Incomplete in `summary_metrics.csv`.
- **Sector clustering:** empty rows in cluster comparison — implement or drop from method list.

## 15. Conclusion
This artifact set supports a **preliminary, mixed-positive** pipeline result: meaningful variation across families, clustering, and hybrid α under PIT rules, with **supplier pressure** as the most credible baseline. It does **not** yet support final claims on regression significance, asymmetric diffusion, event amplification, or net-of-cost tradability. See §2 and Appendix B before calling results “final.”

**Stable baseline:** supplier pressure (Sharpe 0.48, max DD -18.4%).
**Hybrid tuning (best α by Sharpe):** `supplier_pressure` @ α=1 (Sharpe 0.52), `globalrank` @ α=0.75 (Sharpe 0.42), `metacluster` @ α=0 (Sharpe 0.28), `clusterrank` @ α=0.75 (Sharpe 0.25).

## Appendix A: artifact checklist

**Plots:**
- `plots/cluster_sweep_dashboard.png`
- `plots/cluster_sweep_sharpe.png`
- `plots/cluster_sweep_sharpe_vs_ari.png`
- `plots/cumulative_pnl_by_strategy.png`
- `plots/drawdown_by_strategy.png`
- `plots/hybrid_alpha_sweep.png`
- `plots/hybrid_alpha_sweep_cumulative_pnl.png`
- `plots/hybrid_sharpe_heatmap.png`
- `plots/methods_comparison.png`
- `plots/strategy_families_dashboard.png`

**Required CSVs (EXPECTED_OUTPUTS §8):**
- ✓ `summary_metrics.csv`
- ✓ `panel_forward_reverse.csv`
- ✓ `strategy_family_comparison.csv`
- ✓ `cluster_method_comparison.csv`
- ✓ `hybrid_alpha_sweep.csv`
- ✓ `cluster_stability.csv`

## Appendix B: Submission readiness (preliminary vs final)

**Status:** This directory is a **preliminary results** bundle (backtest, clustering, hybrid). It is sufficient for pipeline validation and exploratory comparison; it is **not** a complete final research deliverable until the ⚠ / ✗ items below are addressed.

**Defensible headline:** The PIT pipeline runs end-to-end and shows meaningful differences across strategy families, clustering methods, and hybrid α. **Supplier pressure** is the most stable baseline; **clusterrank** benefits from supply-community clustering; hybrid tuning materially affects **globalrank** and **clusterrank**. Statistical predictability (FE panel), directionality, event amplification, transaction-cost robustness, and full rebalance calendar still need work.

| File | Status |
|------|--------|
| `summary_metrics.csv` | ⚠ turnover/costs incomplete |
| `strategy_family_comparison.csv` | ✓ present |
| `cluster_method_comparison.csv` | ✓ present |
| `hybrid_alpha_sweep.csv` | ✓ present |
| `cluster_stability.csv` | ✓ present |
| `panel_forward_reverse.csv` | ⚠ pooled OLS fallback; NaN SE/p-values |
| `horizon_decay.csv` | ✓ present |
| `event_conditioned_backtest.csv` | ✗ missing (run events step) |
| `turnover_costs.csv` | ✗ missing |
| `factor_exposure_alpha.csv` | ✗ optional |

**Recommended reruns:** (1) `build_leadlag_panel.py` + `--steps load,panel` with `linearmodels`; (2) `--steps events` with earnings calendar; (3) full rebalance calendar (drop or raise `--max-rebalances` cap); (4) populate turnover/cost columns in `summary_metrics.csv`.
