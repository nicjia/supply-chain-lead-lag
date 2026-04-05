import numpy as np
import pandas as pd

from supply_chain_leadlag.backtest import (
    comparison_metrics_table,
    grid_search_main_backtest,
    long_short_weights,
    run_rolling_comparison,
    run_rolling_long_short,
)


def _synth_panel(n: int = 400, n_stock: int = 15):
    rng = np.random.default_rng(42)
    dates = pd.date_range("2018-01-01", periods=n, freq="B")
    gv = [f"{i:06d}" for i in range(1, n_stock + 1)]
    data = {g: rng.standard_normal(n) * 0.015 for g in gv}
    return pd.DataFrame(data, index=dates)


def _synth_edges(gvkeys: list[str]):
    rows = []
    for i in range(len(gvkeys) - 1):
        rows.append(
            {
                "srcdate": "2018-06-30",
                "supplier_gvkey": gvkeys[i],
                "customer_gvkey": gvkeys[i + 1],
                "weight_wji": 0.15,
            }
        )
    return pd.DataFrame(rows)


def test_long_short_weights_sum_zero():
    s = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0], index=list("abcde"))
    w = long_short_weights(s, q=0.4)
    assert abs(w.sum()) < 1e-9


def test_run_rolling_smoke():
    R = _synth_panel()
    e = _synth_edges([f"{i:06d}" for i in range(1, 16)])
    e["date"] = pd.to_datetime(e["srcdate"])
    res = run_rolling_long_short(
        R,
        e,
        lookback_rows=120,
        rebalance_freq="BQE",
        score="cross_corr",
        min_obs=40,
        max_lag=3,
        winsor_q=None,
    )
    assert len(res.daily_ret) == len(R)
    assert res.rebalance_log.shape[0] >= 0


def test_run_rolling_comparison_smoke():
    R = _synth_panel()
    e = _synth_edges([f"{i:06d}" for i in range(1, 16)])
    e["date"] = pd.to_datetime(e["srcdate"])
    comp = run_rolling_comparison(
        R,
        e,
        lookback_rows=120,
        rebalance_freq="BQE",
        score="cross_corr",
        min_obs=40,
        max_lag=3,
        winsor_q=None,
        max_rebalances=4,
        include_baselines=True,
    )
    tab = comparison_metrics_table(comp)
    assert "strategy" in tab.columns
    assert "main" in tab["strategy"].values


def test_grid_search_smoke():
    R = _synth_panel()
    e = _synth_edges([f"{i:06d}" for i in range(1, 16)])
    e["date"] = pd.to_datetime(e["srcdate"])
    df = grid_search_main_backtest(
        R,
        e,
        scores=["cross_corr"],
        rank_methods=["leadingness"],
        lookback_rows=120,
        rebalance_freq="BQE",
        min_obs=40,
        max_lag=3,
        winsor_q=None,
        max_rebalances=3,
    )
    assert "sharpe" in df.columns
    assert df.iloc[0]["score"] == "cross_corr"


def test_run_rolling_cluster_rank_smoke():
    R = _synth_panel()
    e = _synth_edges([f"{i:06d}" for i in range(1, 16)])
    e["date"] = pd.to_datetime(e["srcdate"])
    res = run_rolling_long_short(
        R,
        e,
        lookback_rows=120,
        rebalance_freq="BQE",
        score="cross_corr",
        rank_method="cluster",
        n_clusters=3,
        min_obs=40,
        max_lag=3,
        winsor_q=None,
        max_rebalances=2,
    )
    assert len(res.daily_ret) == len(R)
