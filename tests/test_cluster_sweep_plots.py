import pandas as pd
from pathlib import Path

from supply_chain_leadlag.research_pipeline import (
    _write_cluster_sweep_cumulative_pnl_plots,
    _write_cluster_sweep_plots,
)


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


def test_cluster_sweep_cumulative_pnl_per_family(tmp_path):
    import numpy as np

    dates = pd.date_range("2020-01-01", periods=50, freq="B")
    rows = []
    for cm, drift in [("signed", 0.001), ("hermitian", -0.0005)]:
        for fam in ("metacluster", "clusterrank"):
            r = np.full(len(dates), drift)
            for i, d in enumerate(dates):
                rows.append(
                    {
                        "date": d,
                        "daily_return": r[i],
                        "strategy_family": fam,
                        "cluster_method": cm,
                    }
                )
    daily = pd.DataFrame(rows)
    daily.to_csv(tmp_path / "cluster_sweep_daily_returns.csv", index=False)
    (tmp_path / "plots").mkdir()
    _write_cluster_sweep_cumulative_pnl_plots(tmp_path, daily)
    assert (tmp_path / "plots" / "cluster_sweep_cumulative_pnl_metacluster.png").is_file()
    assert (tmp_path / "plots" / "cluster_sweep_cumulative_pnl_clusterrank.png").is_file()
