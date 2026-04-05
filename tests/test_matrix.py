import numpy as np
import pandas as pd
import pytest

from supply_chain_leadlag.matrix import (
    build_lead_lag_matrix_gvkey,
    hybrid_matrix,
    structural_C_from_edges,
)
from supply_chain_leadlag.global_structure import (
    global_rank_spectral_df,
    hermitian_from_skew,
    permutation_test_max_eig,
)


def _tiny_returns(n_dates: int = 200, gvkeys: list[str] | None = None) -> pd.DataFrame:
    if gvkeys is None:
        gvkeys = ["000001", "000002", "000003"]
    rng = np.random.default_rng(0)
    dates = pd.date_range("2020-01-01", periods=n_dates, freq="B")
    data = {g: rng.standard_normal(n_dates) * 0.01 for g in gvkeys}
    return pd.DataFrame(data, index=dates)


def _tiny_edges() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "srcdate": ["2020-01-31"] * 2,
            "supplier_gvkey": ["000001", "000002"],
            "customer_gvkey": ["000002", "000003"],
            "weight_wji": [0.2, 0.3],
        }
    )


def test_build_lead_lag_matrix_smoke():
    R = _tiny_returns()
    e = _tiny_edges()
    res = build_lead_lag_matrix_gvkey(R, e, horizon=1, min_obs=30, winsor_q=None)
    assert res.C.shape[0] >= 1
    assert np.allclose(res.S.values, (res.C - res.C.T).values)


@pytest.mark.parametrize(
    "score",
    ["cross_corr", "regression_r2", "granger", "levy"],
)
def test_build_lead_lag_matrix_pairwise_methods(score):
    R = _tiny_returns(n_dates=300)
    e = _tiny_edges()
    res = build_lead_lag_matrix_gvkey(
        R, e, min_obs=40, winsor_q=None, score=score, max_lag=5
    )
    assert res.C.shape[0] >= 1
    assert "score_method" in res.edge_scores.columns
    assert (res.edge_scores["score_method"] == score).all()


def test_hermitian_skew():
    S = np.array([[0.0, 1.0], [-1.0, 0.0]])
    H = hermitian_from_skew(S)
    ev = np.linalg.eigvalsh(H)
    assert np.allclose(ev, [-1.0, 1.0])


def test_perm_test_runs():
    rng = np.random.default_rng(1)
    n = 8
    S = rng.standard_normal((n, n))
    S = S - S.T
    obs, p, null = permutation_test_max_eig(S, n_perm=20, seed=2)
    assert len(null) == 20
    assert 0 <= p <= 1


def test_hybrid_and_structural():
    nodes = ["000001", "000002", "000003"]
    C1 = pd.DataFrame(np.eye(3), index=nodes, columns=nodes)
    e = _tiny_edges()
    C2 = structural_C_from_edges(e, nodes)
    H = hybrid_matrix(C1, C2, 0.5)
    assert H.shape == (3, 3)


def test_global_rank_finite():
    nodes = ["a", "b", "c"]
    C = pd.DataFrame([[0, 1, 0], [0, 0, 1], [0, 0, 0]], index=nodes, columns=nodes, dtype=float)
    s = global_rank_spectral_df(C)
    assert np.all(np.isfinite(s))
