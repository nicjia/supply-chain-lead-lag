import numpy as np
import pandas as pd

from supply_chain_leadlag.backtest import (
    apply_risk_overlays,
    comparison_metrics_table,
    filter_edges_pit,
    grid_search_main_backtest,
    long_short_weights,
    run_rolling_comparison,
    run_rolling_long_short,
    supplier_pressure_signal,
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


def test_grid_search_supplier_pressure_collapses_rank_methods():
    R = _synth_panel()
    e = _synth_edges([f"{i:06d}" for i in range(1, 16)])
    e["date"] = pd.to_datetime(e["srcdate"])
    df = grid_search_main_backtest(
        R,
        e,
        scores=["cross_corr"],
        rank_methods=["leadingness", "spectral", "cluster"],
        signal_method="supplier_pressure",
        show_progress=False,
        lookback_rows=120,
        rebalance_freq="BQE",
        min_obs=40,
        max_lag=3,
        winsor_q=None,
        max_rebalances=2,
    )
    assert len(df) == 1
    assert df.iloc[0]["rank_method"] == "supplier_pressure"


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


def test_filter_edges_pit_keeps_latest_and_optional_expiry():
    e = pd.DataFrame(
        {
            "customer_gvkey": ["000001", "000001", "000001", "000002"],
            "supplier_gvkey": ["000010", "000010", "000011", "000012"],
            "date": pd.to_datetime(["2020-01-01", "2020-06-01", "2018-01-01", "2020-05-01"]),
            "weight_wji": [0.1, 0.2, 0.3, 0.4],
        }
    )
    pit = filter_edges_pit(e, pd.Timestamp("2020-07-01"))
    row = pit[(pit["customer_gvkey"] == "000001") & (pit["supplier_gvkey"] == "000010")].iloc[0]
    assert abs(float(row["weight_wji"]) - 0.2) < 1e-12
    pit_exp = filter_edges_pit(e, pd.Timestamp("2020-07-01"), expiry_days=365)
    assert not ((pit_exp["customer_gvkey"] == "000001") & (pit_exp["supplier_gvkey"] == "000011")).any()


def test_supplier_pressure_signal_orientation():
    C = pd.DataFrame(
        [[0.0, 0.8], [0.0, 0.0]],
        index=["custA", "suppB"],
        columns=["custA", "suppB"],
    )
    r = pd.Series({"custA": 0.05, "suppB": 0.0})
    s = supplier_pressure_signal(C, r)
    assert abs(float(s["suppB"]) - 0.04) < 1e-12
    assert abs(float(s["custA"])) < 1e-12


def test_run_rolling_supplier_pressure_smoke():
    R = _synth_panel(n=280, n_stock=30)
    e = _synth_edges([f"{i:06d}" for i in range(1, 31)])
    e["date"] = pd.to_datetime(e["srcdate"])
    res = run_rolling_long_short(
        R,
        e,
        signal_method="supplier_pressure",
        lookback_rows=120,
        rebalance_freq="BME",
        score="cross_corr",
        min_obs=40,
        max_lag=3,
        winsor_q=None,
        max_rebalances=3,
    )
    assert len(res.daily_ret) == len(R)
    assert res.daily_ret.notna().sum() > 0


def test_supplier_pressure_positive_customer_shock_longs_supplier_next_day():
    dates = pd.date_range("2020-01-01", periods=8, freq="B")
    cols = ["CUST01", "SUPP01"] + [f"DUMMY{i:02d}" for i in range(30)]

    R = pd.DataFrame(0.0, index=dates, columns=cols)
    R.loc[dates[2], "CUST01"] = 0.10
    R.loc[dates[3], "SUPP01"] = 0.05

    C = pd.DataFrame(0.0, index=cols, columns=cols)
    C.loc["CUST01", "SUPP01"] = 1.0

    r_today = R.loc[dates[2]].reindex(C.index).fillna(0.0)
    supplier_signal = pd.Series(C.to_numpy(dtype=float).T @ r_today.to_numpy(dtype=float), index=C.columns)
    supplier_signal.loc[[c for c in cols if c != "SUPP01"]] = -1e-6

    w = long_short_weights(supplier_signal, q=0.1, long_high=True)
    gross_next_day = float((w * R.loc[dates[3]].reindex(w.index).fillna(0.0)).sum())

    assert supplier_signal.loc["SUPP01"] > 0
    assert w.loc["SUPP01"] > 0
    assert gross_next_day > 0
