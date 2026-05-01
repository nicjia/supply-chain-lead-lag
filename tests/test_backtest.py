import numpy as np
import pandas as pd

from supply_chain_leadlag.backtest import (
    apply_risk_overlays,
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


def test_hybrid_alpha_one_matches_no_hybrid():
    """α=1 is pure C_data; main leg should match omitting hybrid_alpha."""
    R = _synth_panel()
    e = _synth_edges([f"{i:06d}" for i in range(1, 16)])
    e["date"] = pd.to_datetime(e["srcdate"])
    kw = dict(
        lookback_rows=120,
        rebalance_freq="BQE",
        score="cross_corr",
        min_obs=40,
        max_lag=3,
        winsor_q=None,
        max_rebalances=4,
        include_baselines=False,
    )
    a = run_rolling_comparison(R, e, hybrid_alpha=None, **kw).main.daily_ret
    b = run_rolling_comparison(R, e, hybrid_alpha=1.0, **kw).main.daily_ret
    pd.testing.assert_series_equal(a, b, check_names=False)


def test_grid_search_smoke():
    R = _synth_panel()
    e = _synth_edges([f"{i:06d}" for i in range(1, 16)])
    e["date"] = pd.to_datetime(e["srcdate"])
    df = grid_search_main_backtest(
        R,
        e,
        scores=["cross_corr"],
        rank_methods=["leadingness"],
        show_progress=False,
        lookback_rows=120,
        rebalance_freq="BQE",
        min_obs=40,
        max_lag=3,
        winsor_q=None,
        max_rebalances=3,
    )
    assert "sharpe" in df.columns
    assert df.iloc[0]["score"] == "cross_corr"
    assert "n_clusters" in df.columns and "max_rebalances" in df.columns


def test_grid_search_n_clusters_and_mr_grids():
    R = _synth_panel()
    e = _synth_edges([f"{i:06d}" for i in range(1, 16)])
    e["date"] = pd.to_datetime(e["srcdate"])
    df = grid_search_main_backtest(
        R,
        e,
        scores=["cross_corr"],
        rank_methods=["leadingness"],
        n_clusters_grid=[3, 5],
        max_rebalances_grid=[2, 3],
        show_progress=False,
        lookback_rows=120,
        rebalance_freq="BQE",
        min_obs=40,
        max_lag=3,
        winsor_q=None,
        n_clusters=4,
        max_rebalances=3,
    )
    assert len(df) == 2 * 2
    pairs = set(zip(df["n_clusters"].tolist(), df["max_rebalances"].tolist()))
    assert pairs == {(3, 2), (3, 3), (5, 2), (5, 3)}


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


def test_cost_model_deducts_from_gross():
    R = _synth_panel(n=260, n_stock=30)
    e = _synth_edges([f"{i:06d}" for i in range(1, 31)])
    e["date"] = pd.to_datetime(e["srcdate"])
    res = run_rolling_long_short(
        R,
        e,
        lookback_rows=120,
        rebalance_freq="BME",
        score="cross_corr",
        min_obs=40,
        max_lag=3,
        winsor_q=None,
        max_rebalances=3,
        commission_bps=10.0,
        slippage_bps=5.0,
        borrow_bps_annual=100.0,
    )
    valid = res.daily_ret.notna()
    assert valid.any()
    assert (res.gross_daily_ret[valid] >= res.daily_ret[valid]).all()
    assert (res.daily_cost[valid] >= 0).all()
    assert res.turnover.sum() > 0


def test_apply_risk_overlays_sector_and_beta():
    w = pd.Series([0.6, 0.4, -0.6, -0.4], index=["a", "b", "c", "d"])
    betas = pd.Series([1.2, 0.8, 1.1, 0.9], index=w.index)
    sectors = pd.Series({"a": "tech", "b": "tech", "c": "ind", "d": "ind"})
    w2 = apply_risk_overlays(
        w,
        max_abs_weight=0.5,
        beta_neutralize=True,
        betas=betas,
        sector_neutralize=True,
        sector_map=sectors,
    )
    assert float(w2.abs().max()) <= 0.5 + 1e-12
    exp = float((w2 * betas).sum())
    assert abs(exp) < 1e-8
