# Supply Chain Lead–Lag: A Unified Framework for Network-Based Information Diffusion

## 1. Motivation

Financial markets exhibit lead–lag relationships, where the price movement of one asset precedes and predicts the movement of another. Traditional approaches detect such relationships using purely statistical methods (e.g., cross-correlation or regression), often without grounding in economic structure.

However, real-world economic systems—especially supply chains—naturally encode **directional dependencies**. When a downstream customer experiences a shock (e.g., earnings surprise), upstream suppliers may react with a delay due to information frictions, demand transmission, or investor attention.

This project aims to unify **statistical and structural approaches** to lead–lag detection within a single framework and evaluate their effectiveness in explaining and predicting asset returns.

---

## 2. Core Idea

We propose a **modular framework** for constructing and evaluating lead–lag relationships.

### Step 1: Construct Pairwise Lead–Lag Matrix

We define a pairwise matrix:

\[
C_{ij} = \text{strength of lead–lag from asset } i \text{ to asset } j
\]

Implementations in this repo include:

| Definition | Module / script | Role |
|------------|-----------------|------|
| **Regression asymmetry** — `tstat_diff`, `beta_diff`: forward vs reverse predictive OLS at horizon \(h\), × \(w\) | `build_lead_lag_matrix_gvkey(..., score="tstat_diff" \| "beta_diff")` | Default data-driven \(C\) |
| **Cross-correlation / \(R^2\) / Granger / Lévy** — forward vs reverse pairwise metrics at `max_lag`, × \(w\) | `score="cross_corr" \| "regression_r2" \| "granger" \| "levy"`; helpers in `pairwise.py` | Alternative statistical \(C\) on the same edges |
| **Pure structure** \(C_{ij} = w_{j\to i}\) | `structural_C_from_edges` | \(C^{\text{supply}}\) without returns |
| **Hybrid** \(C = \alpha C^{\text{data}} + (1-\alpha) C^{\text{supply}}\) | `hybrid_matrix` | Blend statistical and economic structure |

### Step 2: From Pairwise to Global Structure

Given \(C\), we extract global structure using:

- **GlobalRank (spectral):** \(S = C - C^\top\), \(H = iS\); leading eigenvector of \(H\) ranks assets by “leadingness” — `supply_chain_leadlag.global_structure.global_rank_spectral_df`.
- **MetaCluster:** spectral clustering on \(|C| + |C^\top|\) — `meta_cluster_labels`.
- **ClusterRank:** local rank within clusters plus cluster net flow — `cluster_rank_series`.

Hermitian spectrum and permutation tests on \(\lambda_{\max}(iS)\): `supply_chain_leadlag.global_structure`, `scripts/spectral_analysis.py`.

### Step 3: Signal Construction

- **Structural panel (implemented):** \(y_{i,t} = \sum_j w_{j\to i,t}\, r_{j,t}\) for suppliers — `scripts/build_leadlag_panel.py` (forward direction).
- **Lagged linear signal:** \(y_{i,t} = \sum_j C_{ij}\, r_{j,t-k}\) — `supply_chain_leadlag.signals.lagged_linear_signal`.
- **Multi-hop:** \(A^k r\) — `multihop_signal`.

### Step 4: Evaluation

- **Predictability:** panel regressions with firm and time effects — `scripts/run_leadlag_tests.py` (requires optional `linearmodels`).
- **Portfolio / univariate:** Sharpe, drawdown — `portfolio_metrics`; rolling long–short — `run_rolling_long_short` / `run_rolling_comparison` with baselines (**random** ranks, **momentum**, **structural** weights-only, **equal_weight** long-only) — `scripts/backtest_leadlag.py`, `comparison_metrics_table`.
- **Structural:** \(\lambda_{\max}(H)\), Frobenius norm — `structural_summary`; compare \(C^{\text{data}}\) vs \(C^{\text{supply}}\) — `matrix_compare_frobenius`.

---

## 3. Key Research Questions (mapped to code)

| Question | How to explore |
|----------|----------------|
| Does economic structure help? | Compare panels / matrices from `build_leadlag_panel` vs `build_lead_lag_matrix`; hybrid \(\alpha\) sweep |
| What defines a “good” relationship? | Set `score=` (`tstat_diff`, `beta_diff`, `cross_corr`, `regression_r2`, `granger`, `levy`) in `build_lead_lag_matrix_gvkey` |
| When do methods disagree? | `matrix_compare_frobenius(C_data, C_supply)` and correlated rankings |
| Multi-hop / diffusion | `multihop_signal`, horizon loops in `run_leadlag_tests` |
| Local vs global ranking | `leadingness` (row-sum of \(S\)) vs `global_rank_spectral_df` vs `cluster_rank_series` |

---

## 4. Extensions (supported in code)

- **Hybrid matrix:** `hybrid_matrix`
- **Multi-hop:** `multihop_signal`
- **Spectral interpretation:** `spectral_analysis.py`, `structural_summary`

Regime dependence and full graph Laplacian dynamics are left as research extensions.

---

## 5. Contribution

This work reframes lead–lag detection as a comparison between **statistical inference** and **economic structure**, implemented as composable Python modules plus reproducible scripts.
