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
| `data/sector_map.csv` | Optional; `gvkey`, `sector` for sector clustering |
| `data/output_earnings_calendar.csv` | Optional; event-conditioned analysis |

If returns parquet is missing, the final research pipeline falls back to a small synthetic panel (smoke only).

---

## Final research pipeline

Unified run: strategy families, clustering comparison, hybrid α sweep, events, stability metrics, plots, and reports under **`results/final_research/`**.

Config: [`config/research.yaml`](config/research.yaml). CLI overrides YAML.

```bash
# Full deliverables (slow; use real returns parquet when available)
python scripts/run_final_research_pipeline.py --config config/research.yaml --max-rebalances 12

# Faster smoke (fewer rebalances, trimmed sweeps, no baseline backtest)
python scripts/run_final_research_pipeline.py --quick --max-rebalances 2

# Verbose: step-by-step INFO + per-rebalance DEBUG lines
python scripts/run_final_research_pipeline.py --config config/research.yaml --max-rebalances 12 -v

# Hybrid α sweep only → hybrid_alpha_sweep.csv + plot
python scripts/run_hybrid_alpha_sweep.py --config config/research.yaml
```

Regenerate reports from existing results:

```bash
python scripts/make_final_report.py --results-dir results/final_research
```

**Strategy families:** `supplier_pressure`, `globalrank`, `metacluster`, `clusterrank`  
**Clustering methods:** `sector`, `supply_community`, `symmetric_spectral`, `hermitian`, `signed`, `hybrid_prior`  
**Hybrid matrix:** \(C_{\text{hybrid}} = \alpha C_{\text{data}} + (1-\alpha) C_{\text{supply}}\) for \(\alpha \in \{0, 0.25, 0.5, 0.75, 1\}\) on full runs.

**PIT rules:** edges with `date ≤` rebalance; \(C\) from trailing returns only; supplier-pressure uses \(s_d = C^\top r_d\), weights on \(d+1\); **customers signal, suppliers are traded**.

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
