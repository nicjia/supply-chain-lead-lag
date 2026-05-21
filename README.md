# Supply Chain Lead–Lag

Unified framework for **network-based lead–lag** between statistical return predictability and **supply-chain weights** \(w_{j \to i}\). The conceptual write-up lives in [FRAMEWORK.md](FRAMEWORK.md).

## Repository layout

| Path | Purpose |
|------|---------|
| `config/backtest.yaml` | Optional central defaults for backtest/grid (paths, matrix, ranking, outputs, grid); CLI overrides YAML |
| `supply_chain_leadlag/` | Package: `matrix` (data loaders + \(C\)), `pairwise`, `global_structure` (spectrum + rankings), `signals` (signals + portfolio metrics), `backtest` (rolling long–short) |
| `data/merged_edges.csv` (or `merged_edges.csv`) | Point-in-time edges: `weight_wji`, `supplier_gvkey`, resolved `customer_gvkey` |
| `data/returns_with_gvkey.parquet` | Daily returns (`RET`) by `gvkey` |
| `scripts/build_lead_lag_matrix.py` | Build **data-driven** \(C\) on edges (forward vs reverse regression asymmetry × weight) → `results/leadlag_stage2_gvkey/` |
| `scripts/build_leadlag_panel.py` | Build **structural** signal panel \(y_{i,t} = \sum_j w_{j\to i,t} r_{j,t}\) for econometric tests |
| `scripts/run_leadlag_tests.py` | Panel regressions (needs `linearmodels`) |
| `scripts/spectral_analysis.py` | Spectrum of \(H=iS\), optional permutation test for \(\lambda_{\max}\) |
| `scripts/backtest_leadlag.py` | **Rolling** PIT long–short on real returns + edges → CSV + JSON summary |
| `scripts/grid_backtest.py` | **Grid** over `score` × `rank_method` (main Sharpe); see README |
| `config/research.yaml` | Final research pipeline defaults (strategy families, clustering, hybrid sweep) |
| `scripts/run_final_research_pipeline.py` | Unified PIT research run → `results/final_research/` |
| `scripts/run_hybrid_alpha_sweep.py` | Hybrid α sweep only (`hybrid_alpha_sweep.csv`) |
| `scripts/make_final_report.py` | Regenerate `report/final_report.md` / `.tex` from results |

## Install

```bash
cd /path/to/supply-chain-lead-lag
pip install -e ".[dev]"
# Optional: panel regressions
pip install linearmodels
```

From the repo root you can also run `python scripts/backtest_leadlag.py` (and the other `supply_chain_leadlag`-importing scripts) **without** installing the package: those scripts add the project root to `sys.path`. For `pytest` or `import supply_chain_leadlag` in arbitrary directories, use `pip install -e .` or `PYTHONPATH=.` (the package lists `pyyaml` so YAML config works after install).

## Configuration (`config/backtest.yaml`)

`scripts/backtest_leadlag.py` and `scripts/grid_backtest.py` load **`config/backtest.yaml`** from the repo root when it exists. Pass **`--config /path/to/file.yaml`** to use another file, or omit the file to rely on code defaults only. YAML support comes from the **PyYAML** distribution on PyPI (`pip install pyyaml` or `pip install -e .`); do not run `pip install yaml`—that is a different, unrelated name.

**Precedence:** CLI flags override YAML; anything not set in either uses the script’s built-in defaults (see `supply_chain_leadlag.yaml_config.flat_backtest_run_params`).

| Section | Role |
|---------|------|
| `paths` | `returns_parquet`, `edges_csv` |
| `strategy` | `signal_method` (`supplier_pressure` \| `rank_factor`), `edge_date_col` (`filing_date` \| `srcdate`), optional `edge_expiry_days` |
| `matrix` | `score`, `horizon`, `max_lag`, `min_obs`, optional `hybrid_alpha` (blend return-based `C` with supply-only `C` before ranking; main leg only) |
| `ranking` | `rank_method`, `n_clusters`, `cluster_random_state` |
| `backtest` | `lookback_rows`, `rebalance_freq`, `q`, `max_rebalances`, `compare_baselines`, `momentum_window`, `baseline_seed` |
| `outputs` | `daily_csv`, `summary_json`, `comparison_csv` |
| `grid` | (grid script only) `scores`, `rank_methods`, optional `n_clusters` / `max_rebalances` **as lists** to sweep, `out_csv` |

## Current Progress And Results

- Core pipeline is implemented end-to-end in Python: edge-level lead-lag scoring, global ranking, structural/hybrid matrices, rolling PIT backtests, and grid sweeps.
- Checked-in artifacts currently include spectral diagnostics only in `results/leadlag_spectral_h1/`:
  - skew-symmetry sanity check (`max|S+S^T|=0`, shape `(521, 521)`),
  - spectrum and embedding plots,
  - permutation null test with `obs_maxeig=3.16026`, `p_value=0.0739261`.
- Real-data backtest outputs are not committed yet under `results/backtest/` in the current tree.
- `data/returns_with_gvkey.parquet` is required to run the full real-data backtest.

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
    python scripts/build_leadlag_panel.py --direction forward --horizon_max 5 --edge_date_col filing_date --edge_expiry_days 550 --out data/leadlag_panel_forward.parquet
    python scripts/run_leadlag_tests.py --panel data/leadlag_panel_forward.parquet --direction forward
   ```

4. **Rolling long–short backtest** (monthly rebalance, trailing window, PIT edges). Rebuilding \(C\) on every edge each month is heavy; use `--max_rebalances` for a quick run:

   ```bash
    python scripts/backtest_leadlag.py --signal_method supplier_pressure --score tstat_diff --rank_method leadingness --lookback_rows 504 --max_rebalances 12
   ```

   Outputs (default): **multi-column** `results/backtest/daily_strategy.csv` (`main`, `random`, `momentum`, `structural`, `equal_weight`), `results/backtest/summary_comparison.csv` (Sharpe and other metrics per leg), and `results/backtest/summary.json` (main strategy only). Use `--no_compare` to run **only** the main leg and write a single-column CSV.

  Strategy semantics:
  - `signal_method=supplier_pressure` (default): at each rebalance estimate \(C\), then on each day \(d\) in the block compute supplier signals \(s_d = C^\top r_d\), and apply weights on \(d+1\).
  - `signal_method=rank_factor`: classic leader-vs-lagger cross-sectional rank-factor portfolio from `scores_from_result(...)`.
  - `edge_date_col=filing_date` is safer for tradable PIT timing (falls back to `srcdate` if unavailable).
  - `edge_expiry_days` (optional) drops stale links beyond the lookback horizon before taking latest edge state.

### Real backtesting controls

The backtest engine now supports a configurable net-of-cost path and risk overlays:

- **Costs at execution:** `commission_bps`, `slippage_bps` applied on rebalance turnover, plus daily `borrow_bps_annual` on short notional.
- **Diagnostics:** output includes net return, gross return, daily cost, and daily turnover series per strategy.
- **Risk overlays:** optional `max_abs_weight` cap, `beta_neutralize` (with `market_gvkey` and `beta_lookback_rows`), and `sector_neutralize` (with `sector_map_csv` containing `gvkey,sector`).

Examples (once returns parquet is available):

```bash
python scripts/backtest_leadlag.py --progress
```

```bash
python scripts/backtest_leadlag.py \         
  --commission_bps 1 \
  --slippage_bps 5 \
  --borrow_bps_annual 100 \
  --progress
```

```bash
python scripts/backtest_leadlag.py \
  --returns_parquet data/returns_with_gvkey.parquet \
  --edges_csv data/merged_edges.csv \
  --score tstat_diff --rank_method cluster_eigen --n_clusters 6 \
  --lookback_rows 504 --rebalance_freq BME --q 0.2 \
  --commission_bps 2.0 --slippage_bps 3.0 --borrow_bps_annual 50.0 \
  --max_abs_weight 0.05 --beta_neutralize --market_gvkey 001690 --beta_lookback_rows 252 \
  --sector_neutralize --sector_map_csv data/gvkey_sector_map.csv \
  --progress
```

Interpretation:
- `main` is **net** return after all enabled costs.
- `main_gross` is before costs.
- `main_cost` is total daily cost drag in return units.
- `main_turnover` is one-way turnover applied at rebalance transitions.

### Static vs tradable outputs

- `scripts/build_lead_lag_matrix.py` builds a **static full-sample diagnostic matrix** (good for analysis/plots, not a tradable PIT object; supports `--edge_date_col` for consistency).
- `scripts/backtest_leadlag.py` runs the **rolling PIT tradable backtest** (re-estimates inside each rebalance window).

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
| **Global / local rank from \(C\)** | `scores_from_result(..., rank_method=...)` | `leadingness` (row-sum of \(S\)); `spectral` (GlobalRank on \(H=iS\)); `cluster` / `cluster_eigen` (MetaCluster + ClusterRank — local row-sum or local spectral inside clusters). Use `n_clusters`, `cluster_random_state`. |

Defaults in `backtest_leadlag.py`: `score=tstat_diff`, `rank_method=leadingness`. Baselines (**structural**, **random**, etc.) do not vary these; they are fixed comparators.

### Grid search (pick score × rank_method)

Run a **main-leg-only** sweep (sorted by Sharpe in the CSV). A **tqdm** progress bar runs over grid cells; use `--no-progress` to disable it. Use a small `--max_rebalances` first.

```bash
python scripts/grid_backtest.py --max_rebalances 8 --out_csv results/backtest/grid_main.csv
python scripts/grid_backtest.py --scores tstat_diff,cross_corr,levy --rank_methods leadingness,spectral --max_rebalances 12
python scripts/grid_backtest.py --rank_methods leadingness,spectral,cluster,cluster_eigen --n_clusters 6 --max_rebalances 8
```

Or in Python: `grid_search_main_backtest(R, edges, max_rebalances=12, show_progress=True)`.

### Reading results (diagnosis)

- **main > random:** statistical \(C\) is doing more than noise (in-sample path still needs OOS discipline).
- **structural > main:** pure supply weights can rank more stably than return-based edge tests—consider **hybrid** \(C\) (`hybrid_matrix`) or using structural ranks as a second signal.
- **momentum** baseline is *not* academic 12–1 momentum; it is **long–short on summed returns** over `momentum_window` inside the lookback. A negative Sharpe there does not imply “stock momentum fails”; it means that particular raw-sum long–short underperformed in your sample.
- **Improvements:** try the grid; adjust `lookback_rows`, `q`, `horizon`; add transaction-cost filters; walk-forward or hold-out years to reduce overfitting to the best cell.

## Tests

```bash
pytest tests/
```

## Legacy

`pipeline.cpp` is an older / auxiliary artifact; the active research path is the Python package and scripts above.
