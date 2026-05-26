import pandas as pd

from supply_chain_leadlag.research_pipeline import _merge_cluster_sweep_artifacts


def test_merge_cluster_sweep_replaces_one_family(tmp_path):
    out = tmp_path / "out"
    out.mkdir()
    old_cmp = pd.DataFrame(
        {
            "strategy_family": ["metacluster", "metacluster", "clusterrank"],
            "cluster_method": ["sector", "signed", "signed"],
            "sharpe": [0.3, 0.2, 0.1],
        }
    )
    old_cmp.to_csv(out / "cluster_method_comparison.csv", index=False)
    old_daily = pd.DataFrame(
        {
            "date": pd.to_datetime(["2020-01-01"] * 2),
            "strategy_family": ["metacluster", "clusterrank"],
            "cluster_method": ["sector", "signed"],
            "daily_return": [0.01, 0.02],
        }
    )
    old_daily.to_csv(out / "cluster_sweep_daily_returns.csv", index=False)

    new_cmp = pd.DataFrame(
        {
            "strategy_family": ["clusterrank", "clusterrank"],
            "cluster_method": ["signed", "sic2"],
            "sharpe": [0.18, 0.16],
        }
    )
    new_daily = pd.DataFrame(
        {
            "date": pd.to_datetime(["2020-01-02"]),
            "strategy_family": ["clusterrank"],
            "cluster_method": ["signed"],
            "daily_return": [0.03],
        }
    )
    merged_cmp, merged_daily = _merge_cluster_sweep_artifacts(
        out, new_cmp, new_daily, ("clusterrank",)
    )
    assert len(merged_cmp) == 4
    assert set(merged_cmp["strategy_family"]) == {"metacluster", "clusterrank"}
    assert merged_cmp[merged_cmp["strategy_family"] == "clusterrank"]["sharpe"].max() == 0.18
    assert len(merged_daily) == 2
