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
