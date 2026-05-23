import pandas as pd
from pathlib import Path

from supply_chain_leadlag.research_pipeline import _write_cluster_sweep_plots


def test_cluster_sweep_plots_from_csv(tmp_path):
    df = pd.DataFrame(
        [
            {"strategy_family": "metacluster", "cluster_method": "signed", "sharpe": 0.4, "cluster_ari_mean": 0.7, "n_rebalances": 12},
            {"strategy_family": "clusterrank", "cluster_method": "signed", "sharpe": 0.3, "cluster_ari_mean": 0.7, "n_rebalances": 12},
            {"strategy_family": "metacluster", "cluster_method": "hermitian", "sharpe": -0.2, "cluster_ari_mean": 0.1, "n_rebalances": 12},
        ]
    )
    (tmp_path / "plots").mkdir()
    _write_cluster_sweep_plots(tmp_path, df)
    assert (tmp_path / "plots" / "cluster_sweep_sharpe.png").is_file()
    assert (tmp_path / "plots" / "cluster_sweep_dashboard.png").is_file()
    assert not (tmp_path / "plots" / "cluster_sweep_heatmap.png").exists()
