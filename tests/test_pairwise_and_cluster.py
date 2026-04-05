"""Regression: no spurious warnings from sparse pairwise stats or disconnected spectral graphs."""

import warnings

import numpy as np

from supply_chain_leadlag.global_structure import meta_cluster_labels
from supply_chain_leadlag.pairwise import _corr


def test_corr_all_nan_pair_returns_nan_without_runtime_warning():
    with warnings.catch_warnings():
        warnings.simplefilter("error", RuntimeWarning)
        assert np.isnan(_corr(np.full(3, np.nan), np.arange(3, dtype=float)))


def test_corr_finite_overlap_matches_pearson():
    x = np.array([1.0, 2.0, 3.0, np.nan])
    y = np.array([1.0, 4.0, 3.0, 5.0])
    r = _corr(x, y)
    assert np.isfinite(r)
    mask = np.isfinite(x) & np.isfinite(y)
    want = float(np.corrcoef(x[mask], y[mask])[0, 1])
    assert abs(r - want) < 1e-9


def test_meta_cluster_no_disconnected_graph_warning():
    # Two directed components with no cross edges → previously triggered sklearn warning.
    C = np.zeros((4, 4))
    C[0, 1] = 0.5
    C[1, 0] = -0.1
    C[2, 3] = 0.3
    C[3, 2] = -0.2
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        labels = meta_cluster_labels(C, n_clusters=2, random_state=0)
    assert labels.shape == (4,)
    assert not any("not fully connected" in str(x.message).lower() for x in w)
