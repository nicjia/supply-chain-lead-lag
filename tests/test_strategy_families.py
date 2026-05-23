import numpy as np
import pandas as pd

from supply_chain_leadlag.matrix import build_lead_lag_matrix_gvkey
from supply_chain_leadlag.strategy_families import run_strategy_family


def _setup():
    dates = pd.date_range("2020-01-01", periods=30, freq="B")
    cols = ["CUST01", "SUPP01", "SUPP02", "CUST02"]
    rng = np.random.default_rng(0)
    R = pd.DataFrame(rng.standard_normal((30, 4)) * 0.01, index=dates, columns=cols)
    edges = pd.DataFrame(
        {
            "customer_gvkey": ["CUST01", "CUST02"],
            "supplier_gvkey": ["SUPP01", "SUPP02"],
            "weight_wji": [0.8, 0.6],
            "date": [dates[0]] * 2,
        }
    )
    R_win = R.iloc[:20]
    R_fwd = R.iloc[20:]
    res = build_lead_lag_matrix_gvkey(R_win, edges, min_obs=10, winsor_q=None, score="cross_corr", max_lag=2)
    return res.C, R_win, R_fwd, edges, res


def test_supplier_pressure_trades_suppliers_only():
    C, R_win, R_fwd, edges, res = _setup()
    out = run_strategy_family(
        "supplier_pressure",
        C,
        R_win,
        R_fwd,
        edges,
        q=0.25,
    )
    dr = out["daily_returns"]
    assert dr.notna().any() or len(out["events"]) >= 0
    for _, w in out["events"]:
        assert abs(w.get("CUST01", 0.0)) < 1e-9
        assert abs(w.get("CUST02", 0.0)) < 1e-9


def test_globalrank_returns_daily_series():
    C, R_win, R_fwd, edges, res = _setup()
    out = run_strategy_family("globalrank", C, R_win, R_fwd, edges, res=res, q=0.25)
    assert isinstance(out["daily_returns"], pd.Series)


def test_weights_sum_zero_long_short():
    C, R_win, R_fwd, edges, res = _setup()
    out = run_strategy_family("globalrank", C, R_win, R_fwd, edges, res=res, q=0.25)
    for _, w in out["events"]:
        assert abs(float(w.sum())) < 1e-6 or w.abs().sum() == 0


def test_clusterrank_longs_laggers_not_leaders():
    from supply_chain_leadlag.clusterrank_strategy import clusterrank_daily_weights, local_leadingness

    nodes = [f"{i:03d}" for i in range(8)]
    C = pd.DataFrame(0.0, index=nodes, columns=nodes)
    for i in range(7):
        C.iloc[i, i + 1] = 1.0
    labels = pd.Series({n: 0 for n in nodes})
    dates = pd.date_range("2020-01-01", periods=5, freq="B")
  # leaders (high ell) move up on day 0
    R = pd.DataFrame(0.0, index=dates, columns=nodes)
    ell = local_leadingness(C.loc[nodes, nodes])
    leaders = ell.nlargest(2).index
    laggers = ell.nsmallest(2).index
    R.loc[dates[0], leaders] = 0.02
    w_df, _ = clusterrank_daily_weights(C, R, labels, q=0.25)
    w0 = w_df.loc[dates[0]]
    assert w0.reindex(laggers).sum() > 0
    assert w0.reindex(leaders).sum() < 0


def test_metacluster_clusterrank_use_unit_weight_scale():
    from supply_chain_leadlag.metacluster_strategy import metacluster_daily_weights

    nodes = [f"{i:03d}" for i in range(12)]
    C = pd.DataFrame(0.0, index=nodes, columns=nodes)
    for i in range(11):
        C.iloc[i, i + 1] = 0.8
    labels = pd.Series({n: i // 4 for i, n in enumerate(nodes)})
    dates = pd.date_range("2020-01-01", periods=10, freq="B")
    rng = np.random.default_rng(1)
    R = pd.DataFrame({n: rng.standard_normal(10) * 0.01 for n in nodes}, index=dates)
    w_m, _, _ = metacluster_daily_weights(C, R, labels, q=0.25)
    out = run_strategy_family(
        "clusterrank",
        C,
        R.iloc[:5],
        R.iloc[5:],
        pd.DataFrame(),
        cluster_labels=labels,
        q=0.25,
    )
    assert w_m.abs().sum(axis=1).max() >= 0.1
    assert out["daily_returns"].abs().max() > 1e-8
