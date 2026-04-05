"""Supply chain lead–lag: matrices, global rankings, signals, metrics, backtests."""

from supply_chain_leadlag.backtest import (
    BacktestResult,
    filter_edges_pit,
    long_short_weights,
    run_rolling_long_short,
    scores_from_result,
)
from supply_chain_leadlag.global_structure import (
    cluster_rank_series,
    eigendecompose_hermitian,
    global_rank_spectral_df,
    hermitian_from_skew,
    meta_cluster_labels,
    permutation_test_max_eig,
)
from supply_chain_leadlag.matrix import (
    EdgeScoreMethod,
    LeadLagResult,
    build_lead_lag_matrix_gvkey,
    hybrid_matrix,
    load_edges,
    load_returns_wide_by_gvkey,
    structural_C_from_edges,
)
from supply_chain_leadlag.signals import (
    lagged_linear_signal,
    matrix_compare_frobenius,
    multihop_signal,
    portfolio_metrics,
    predictability_ols,
    structural_summary,
)

__all__ = [
    "load_edges",
    "load_returns_wide_by_gvkey",
    "EdgeScoreMethod",
    "LeadLagResult",
    "build_lead_lag_matrix_gvkey",
    "structural_C_from_edges",
    "hybrid_matrix",
    "global_rank_spectral_df",
    "meta_cluster_labels",
    "cluster_rank_series",
    "hermitian_from_skew",
    "permutation_test_max_eig",
    "eigendecompose_hermitian",
    "lagged_linear_signal",
    "multihop_signal",
    "matrix_compare_frobenius",
    "portfolio_metrics",
    "predictability_ols",
    "structural_summary",
    "BacktestResult",
    "filter_edges_pit",
    "long_short_weights",
    "run_rolling_long_short",
    "scores_from_result",
]
