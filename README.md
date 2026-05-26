# Supply Chain Lead–Lag

Point-in-time framework for **supply-chain lead–lag**: statistical edge scores, network rankings, panel validation, and rolling backtests on customer → supplier information diffusion.

| Doc | Contents |
|-----|----------|
| [docs/FRAMEWORK.md](docs/FRAMEWORK.md) | Research design and code map |
| [docs/IMPLEMENTATION_SPEC.md](docs/IMPLEMENTATION_SPEC.md) | Engineering spec for agents |
| [docs/EXPECTED_OUTPUTS.md](docs/EXPECTED_OUTPUTS.md) | Final artifact checklist |

---

## Install

```bash
cd /path/to/supply-chain-lead-lag
pip install -e ".[dev]"

# Optional: panel regressions with firm + time FE
pip install -e ".[panel]"   # or: pip install linearmodels
```

Scripts add the repo root to `sys.path`, so you can run them without installing. For `pytest` or imports from other directories, use `pip install -e .`.

---

## Data

| File | Role |
|------|------|
| `data/merged_edges.csv` | PIT supply-chain edges (`customer_gvkey`, `supplier_gvkey`, `weight_wji`, dates) |
| `data/returns_with_gvkey.parquet` | Daily `RET` by `gvkey` (**required for real backtests**) |
| `data/leadlag_panel_forward.parquet` | Optional; built by `build_leadlag_panel.py` for PanelOLS |
| `data/leadlag_panel_reverse.parquet` | Optional; reverse-direction placebo panel |
| `data/wrds_classification.csv` | Raw Compustat GICS / NAICS / SIC panel (annual rows per gvkey) |
| `data/firm_classification_map.csv` | Built by `scripts/build_firm_classification_maps.py` — one row per gvkey |
| `data/sector_map.csv` | Built from WRDS — `gvkey`, `sector` (= GICS `gsector`) for `sector` clustering |
| `data/output_earnings_calendar.csv` | Optional; event-conditioned analysis |

If returns parquet is missing, the final research pipeline falls back to a small synthetic panel (smoke only).

---

## Final research pipeline

Unified run: strategy families, clustering comparison, hybrid α sweep, events, stability metrics, plots, and reports under **`results/final_research/`**.

Config: [`config/research.yaml`](config/research.yaml). CLI overrides YAML.

**After every run, open [`results/final_research/START_HERE.md`](results/final_research/START_HERE.md)** — short guide to the 3 tables and 2 plots that matter most.

### Full run

```bash
# All 11 steps (slow; use real returns parquet)
uv run --extra dev python scripts/run_final_research_pipeline.py --max-rebalances 12

# Smoke
uv run --extra dev python scripts/run_final_research_pipeline.py --quick --max-rebalances 2
```

### Selective runs (skip steps you already have)

```bash
# Your next run: load data + 4-family comparison only (no panel, baselines, sweeps)
uv run --extra dev python scripts/run_final_research_pipeline.py \
  --only families \
  --max-rebalances 12

# Same thing, explicit steps
uv run --extra dev python scripts/run_final_research_pipeline.py \
  --steps load,families,summary,plots,report \
  --skip-steps panel,baselines,cluster_sweep,hybrid_sweep,events,artifacts

# Full run but skip forward/reverse panel (keep existing panel_forward_reverse.csv)
uv run --extra dev python scripts/run_final_research_pipeline.py \
  --skip-steps panel \
  --max-rebalances 12

# Regenerate plots + START_HERE from existing CSVs (no backtest)
uv run --extra dev python scripts/run_final_research_pipeline.py --steps plots,report

# Hybrid α sweep only
uv run --extra dev python scripts/run_hybrid_alpha_sweep.py --config config/research.yaml

# Panel regressions + refresh report (comma-separated — not space-separated)
uv run --extra dev python scripts/run_final_research_pipeline.py --steps load,panel,report
```

### Valid pipeline step names

Pass to `--steps` or `--skip-steps` as a **comma-separated** list (e.g. `load,panel,report`, not `load panel report`):

| Step | Role |
|------|------|
| `load` | Load returns + edges; write `config_used.yaml` (required for any backtest step) |
| `panel` | Forward/reverse predictability → `panel_forward_reverse.csv` |
| `baselines` | Rolling supplier-pressure vs momentum/random baselines |
| `families` | Four strategy families → `strategy_family_comparison.csv`, `daily_returns.csv` |
| `cluster_sweep` | Cluster methods × meta/clusterrank → `cluster_method_comparison.csv` |
| `hybrid_sweep` | α grid → `hybrid_alpha_sweep.csv` |
| `summary` | `summary_metrics.csv`, turnover tables |
| `events` | Event-conditioned panel/backtest (needs earnings calendar) |
| `artifacts` | Last-rebalance matrices, cluster labels |
| `plots` | PNGs under `results/final_research/plots/` |
| `report` | `report/final_report.md`, `START_HERE.md` |

**Presets:** `--only families` → `load,families,summary,plots,report` · `--only hybrid` → `load,hybrid_sweep,plots,report`

**Plot profiles:** `--plot-profile minimal` (default with `--only families`) writes a 2-panel dashboard + cumulative PnL; `full` writes all diagnostic charts.

Regenerate reports from disk:

```bash
uv run --extra dev python scripts/make_final_report.py --results-dir results/final_research
```

### What Step 4 uses (initial 4-family comparison)

| Family | Clustering? | Method |
|--------|-------------|--------|
| `supplier_pressure` | No | Daily \(s_d = C^\top r_d\) on suppliers |
| `globalrank` | No | **`spectral`** GlobalRank (config: `strategies.globalrank_method`) |
| `metacluster` | Yes | **`hermitian`** (config: `clustering.default_cluster_method`, 10 clusters) |
| `clusterrank` | Yes | Same as metacluster |

All four share the same return-based \(C_{\text{data}}\) at each rebalance (no hybrid α in Step 4). Step 5 varies clustering for metacluster/clusterrank only; Step 6 blends \(C_{\text{data}}\) and \(C_{\text{supply}}\).

**Strategy families:** `supplier_pressure`, `globalrank`, `metacluster`, `clusterrank`

**PIT rules:** edges with `date ≤` rebalance; \(C\) from trailing returns only; supplier-pressure uses \(s_d = C^\top r_d\), weights on \(d+1\); **customers signal, suppliers are traded**.

Build industry maps from WRDS (dedupe to latest fiscal year per gvkey):

```bash
uv run python scripts/build_firm_classification_maps.py
```

**Classification clustering methods** (require `firm_classification_map.csv`):

| Method | Source column | Typical # of groups |
|--------|---------------|---------------------|
| `sector` | GICS `gsector` | ~11 |
| `ggroup` | GICS `ggroup` | ~27 |
| `gind` | GICS `gind` | ~81 |
| `gsubind` | GICS `gsubind` | ~189 |
| `naics2` / `naics` | NAICS 2- or 6-digit | varies |
| `sic2` / `sic4` | SIC 2- or 4-digit | varies |

Optional panel FE before the pipeline:

```bash
python scripts/build_leadlag_panel.py --direction forward --out data/leadlag_panel_forward.parquet
python scripts/build_leadlag_panel.py --direction reverse --out data/leadlag_panel_reverse.parquet
```

---

## Core workflows

### 1. Static lead–lag matrix (diagnostics)

Full-sample \(C\) for analysis and spectral scripts (not a tradable PIT object).

```bash
python scripts/build_lead_lag_matrix.py
python scripts/build_lead_lag_matrix.py --score cross_corr --max_lag 5
```

Output: `results/leadlag_stage2_gvkey/` (`C_h1.parquet`, `S_h1.parquet`, edge scores).

### 2. Spectral analysis

```bash
python scripts/spectral_analysis.py \
  --S_parquet results/leadlag_stage2_gvkey/S_h1.parquet \
  --C_parquet results/leadlag_stage2_gvkey/C_h1.parquet \
  --edge_scores_csv results/leadlag_stage2_gvkey/edge_scores_h1.csv \
  --outdir results/leadlag_spectral_h1 \
  --do_permtest --n_perm 1000 --seed 42 --embed_plot
```

### 3. Panel predictability

```bash
python scripts/build_leadlag_panel.py \
  --direction forward --horizon_max 5 \
  --edge_date_col filing_date --edge_expiry_days 550 \
  --out data/leadlag_panel_forward.parquet

python scripts/run_leadlag_tests.py \
  --panel data/leadlag_panel_forward.parquet --direction forward
```

### 4. Rolling PIT backtest

Re-estimates \(C\) each rebalance on PIT edges and a trailing return window.

```bash
python scripts/backtest_leadlag.py \
  --signal_method supplier_pressure \
  --score tstat_diff \
  --lookback_rows 504 \
  --max_rebalances 12
```

| Output | Description |
|--------|-------------|
| `results/backtest/daily_strategy.csv` | Daily returns: `main`, baselines |
| `results/backtest/summary_comparison.csv` | Sharpe and metrics per leg |
| `results/backtest/summary.json` | Main strategy summary |

**Signal modes**

- `supplier_pressure` — daily \(s_d = C^\top r_d\), trade suppliers on \(d+1\) (primary thesis).
- `rank_factor` — long–short on global/cluster ranks from \(C\) (benchmark).

**Baselines** (same calendar, same \(q\)): `random`, `momentum`, `structural`, `equal_weight`. Use `--no_compare` for main leg only.

**Costs and risk** (optional): `--commission_bps`, `--slippage_bps`, `--borrow_bps_annual`, `--max_abs_weight`, `--beta_neutralize`, `--sector_neutralize`.

Config: [`config/backtest.yaml`](config/backtest.yaml) — CLI overrides YAML.

### 5. Grid search (score × rank method)

```bash
python scripts/grid_backtest.py --max_rebalances 8
python scripts/grid_backtest.py \
  --scores tstat_diff,cross_corr,levy \
  --rank_methods leadingness,spectral,cluster_eigen \
  --n_clusters 6 --max_rebalances 12
```

---

## Package map

| Module | Role |
|--------|------|
| `matrix` | Load data, build \(C\), structural/hybrid matrices |
| `pairwise` | Corr, regression R², Granger, Lévy edge scores |
| `global_structure` | GlobalRank, MetaCluster, ClusterRank, spectrum |
| `backtest` | PIT filter, rolling comparison, supplier pressure |
| `strategy_families` | `supplier_pressure`, `globalrank`, `metacluster`, `clusterrank` |
| `clustering_methods` | Six clustering methods for research runs |
| `research_pipeline` | Final unified runner and artifact writers |

**Edge scores:** `tstat_diff`, `beta_diff`, `cross_corr`, `regression_r2`, `granger`, `levy`  
**Rank methods:** `leadingness`, `spectral`, `cluster`, `cluster_eigen`

---

## Tests

```bash
# Fast unit + integration tests (excludes slow full-pipeline test)
pytest tests/ --ignore=tests/test_research_pipeline.py

# Full suite including slow pipeline smoke (~30+ min)
pytest tests/
```

---

## Legacy

`pipeline.cpp` is an older auxiliary artifact. The active path is the Python package and scripts above.
