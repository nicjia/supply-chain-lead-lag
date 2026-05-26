import pandas as pd

from supply_chain_leadlag.research_pipeline import (
    CLUSTER_SWEEP_EXCLUDE_METHODS,
    _exclude_cluster_sweep_methods,
    _filter_cluster_sweep_df,
)


def test_hybrid_prior_excluded_from_sweep_method_list():
    assert "hybrid_prior" in CLUSTER_SWEEP_EXCLUDE_METHODS
    out = _exclude_cluster_sweep_methods(["signed", "hybrid_prior", "sector"])
    assert out == ["signed", "sector"]


def test_filter_cluster_sweep_df_drops_hybrid_prior_rows():
    df = pd.DataFrame(
        {
            "strategy_family": ["clusterrank", "clusterrank"],
            "cluster_method": ["hybrid_prior", "signed"],
            "sharpe": [0.3, 0.18],
        }
    )
    f = _filter_cluster_sweep_df(df)
    assert len(f) == 1
    assert f["cluster_method"].iloc[0] == "signed"
