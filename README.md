# Supply Chain Lead–Lag

Unified framework for **network-based lead–lag** between statistical return predictability and **supply-chain weights** \(w_{j \to i}\). The conceptual write-up lives in [FRAMEWORK.md](FRAMEWORK.md).

## Repository layout

| Path | Purpose |
|------|---------|
| `supply_chain_leadlag/` | Package: `matrix` (data loaders + \(C\)), `pairwise`, `global_structure` (spectrum + rankings), `signals` (signals + portfolio metrics), `backtest` (rolling long–short) |
| `data/merged_edges.csv` (or `merged_edges.csv`) | Point-in-time edges: `weight_wji`, `supplier_gvkey`, resolved `customer_gvkey` |
| `data/returns_with_gvkey.parquet` | Daily returns (`RET`) by `gvkey` |
| `scripts/build_lead_lag_matrix.py` | Build **data-driven** \(C\) on edges (forward vs reverse regression asymmetry × weight) → `results/leadlag_stage2_gvkey/` |
| `scripts/build_leadlag_panel.py` | Build **structural** signal panel \(y_{i,t} = \sum_j w_{j\to i,t} r_{j,t}\) for econometric tests |
| `scripts/run_leadlag_tests.py` | Panel regressions (needs `linearmodels`) |
| `scripts/spectral_analysis.py` | Spectrum of \(H=iS\), optional permutation test for \(\lambda_{\max}\) |
| `scripts/backtest_leadlag.py` | **Rolling** PIT long–short on real returns + edges → CSV + JSON summary |
| `scripts/grid_backtest.py` | **Grid** over `score` × `rank_method` (main Sharpe); see README |

## Install

```bash
cd /path/to/supply-chain-lead-lag
pip install -e ".[dev]"
# Optional: panel regressions
pip install linearmodels
```

From the repo root you can also run `python scripts/backtest_leadlag.py` (and the other `supply_chain_leadlag`-importing scripts) **without** installing the package: those scripts add the project root to `sys.path`. For `pytest` or `import supply_chain_leadlag` in arbitrary directories, use `pip install -e .` or `PYTHONPATH=.`.

## Quick pipeline

1. **Lead–lag matrix (full sample, edge-level scores).** Choose `score`: `tstat_diff` (default), `beta_diff`, `cross_corr`, `regression_r2`, `granger`, `levy`.

   ```bash
   python scripts/build_lead_lag_matrix.py
   python scripts/build_lead_lag_matrix.py --score cross_corr --max_lag 5
   ```

2. **Spectral analysis on \(S = C - C^\top\):**

   ```bash
   python scripts/spectral_analysis.py \
     --S_parquet results/leadlag_stage2_gvkey/S_h1.parquet \
     --C_parquet results/leadlag_stage2_gvkey/C_h1.parquet \
     --edge_scores_csv results/leadlag_stage2_gvkey/edge_scores_h1.csv \
     --outdir results/leadlag_spectral_h1 \
     --do_permtest --n_perm 1000 --seed 42 \
     --embed_plot
   ```

3. **Structural signal panel + tests:**

   ```bash
   python scripts/build_leadlag_panel.py --direction forward --horizon_max 5 --out data/leadlag_panel_forward.parquet
   python scripts/run_leadlag_tests.py --panel data/leadlag_panel_forward.parquet --direction forward
   ```

4. **Rolling long–short backtest** (monthly rebalance, trailing window, PIT edges). Rebuilding \(C\) on every edge each month is heavy; use `--max_rebalances` for a quick run:

   ```bash
   python scripts/backtest_leadlag.py --score tstat_diff --rank_method leadingness --lookback_rows 504 --max_rebalances 12
   ```

   Outputs (default): **multi-column** `results/backtest/daily_strategy.csv` (`main`, `random`, `momentum`, `structural`, `equal_weight`), `results/backtest/summary_comparison.csv` (Sharpe and other metrics per leg), and `results/backtest/summary.json` (main strategy only). Use `--no_compare` to run **only** the main leg and write a single-column CSV.

### Backtest baselines (same rebalances, same \(q\))

| Leg | Meaning |
|-----|----------------|
| **main** | Lead–lag rank from `build_lead_lag_matrix_gvkey` → long top / short bottom \(q\). |
| **random** | Same rule on a **random permutation** of the main scores (noise control). |
| **momentum** | Long–short on **sum of returns** over `--momentum_window` days in the lookback window. |
| **structural** | Long–short on **row-sum** of \(S\) from **supply weights only** (`structural_C_from_edges`), no return-based \(C\). |
| **equal_weight** | **Long-only** equal weight on the same network nodes (not dollar-neutral; level benchmark). |

Interpretation: **main** should beat **random** if the signal is not just noise; **structural** isolates economics; **momentum** checks overlap with simple price trend; **equal_weight** is a naive network-wide exposure.

### Two knobs (what “the model” is)

| Axis | Code | Options |
|------|------|--------|
| **Edge score → \(C\)** | `build_lead_lag_matrix_gvkey(..., score=...)` | `tstat_diff`, `beta_diff`, `cross_corr`, `regression_r2`, `granger`, `levy` |
| **Global rank from \(C\)** | `scores_from_result(..., rank_method=...)` | `leadingness` (row-sum of \(S\)), `spectral` (leading eigenvector of \(H=i(C-C^\top)\)) |

Defaults in `backtest_leadlag.py`: `score=tstat_diff`, `rank_method=leadingness`. Baselines (**structural**, **random**, etc.) do not vary these; they are fixed comparators.

### Grid search (pick score × rank_method)

Run a **main-leg-only** sweep (sorted by Sharpe in the CSV). Use a small `--max_rebalances` first.

```bash
python scripts/grid_backtest.py --max_rebalances 8 --out_csv results/backtest/grid_main.csv
python scripts/grid_backtest.py --scores tstat_diff,cross_corr,levy --rank_methods leadingness,spectral --max_rebalances 12
```

Or in Python: `grid_search_main_backtest(R, edges, max_rebalances=12)`.

### Reading results (diagnosis)

- **main > random:** statistical \(C\) is doing more than noise (in-sample path still needs OOS discipline).
- **structural > main:** pure supply weights can rank more stably than return-based edge tests—consider **hybrid** \(C\) (`hybrid_matrix`) or using structural ranks as a second signal.
- **momentum** baseline is *not* academic 12–1 momentum; it is **long–short on summed returns** over `momentum_window` inside the lookback. A negative Sharpe there does not imply “stock momentum fails”; it means that particular raw-sum long–short underperformed in your sample.
- **Improvements:** try the grid; adjust `lookback_rows`, `q`, `horizon`; add transaction-cost filters; walk-forward or hold-out years to reduce overfitting to the best cell.

## Python API (examples)

```python
from supply_chain_leadlag import (
    load_edges,
    load_returns_wide_by_gvkey,
    build_lead_lag_matrix_gvkey,
    structural_C_from_edges,
    hybrid_matrix,
    global_rank_spectral_df,
    matrix_compare_frobenius,
)

edges = load_edges("data/merged_edges.csv")
R = load_returns_wide_by_gvkey("data/returns_with_gvkey.parquet")
res = build_lead_lag_matrix_gvkey(R, edges, horizon=1, min_obs=80)

nodes = list(res.C.index)
C_sup = structural_C_from_edges(edges, nodes)
C_mix = hybrid_matrix(res.C, C_sup, alpha=0.5)
print("Frobenius distance data vs supply:", matrix_compare_frobenius(res.C, C_sup))
print(global_rank_spectral_df(C_mix).sort_values(ascending=False).head())
```

## Tests

```bash
pytest tests/
```

## Legacy

`pipeline.cpp` is an older / auxiliary artifact; the active research path is the Python package and scripts above.
