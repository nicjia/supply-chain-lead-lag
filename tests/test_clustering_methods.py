import numpy as np
import pandas as pd

from supply_chain_leadlag.clustering_methods import (
    ClusteringMethodError,
    cluster_hermitian,
    cluster_sector,
    get_cluster_labels,
)


def _tiny_C():
    nodes = ["000001", "000002", "000003", "000004"]
    C = pd.DataFrame(
        [[0, 0.5, 0.1, 0], [0.2, 0, 0.3, 0.1], [0, 0.4, 0, 0.2], [0.1, 0, 0.3, 0]],
        index=nodes,
        columns=nodes,
    )
    return C


def test_get_cluster_labels_hermitian_covers_all_nodes():
    C = _tiny_C()
    lab = get_cluster_labels(C, "hermitian", n_clusters=2, random_state=0)
    assert len(lab) == 4
    assert lab.notna().all()


def test_hermitian_deterministic_with_seed():
    C = _tiny_C().to_numpy()
    a = cluster_hermitian(C, n_clusters=2, random_state=42)
    b = cluster_hermitian(C, n_clusters=2, random_state=42)
    np.testing.assert_array_equal(a, b)


def test_sector_requires_map():
    C = _tiny_C()
    try:
        get_cluster_labels(C, "sector", sector_map=None)
        assert False, "expected ClusteringMethodError"
    except ClusteringMethodError:
        pass


def test_sector_with_map():
    C = _tiny_C()
    sm = pd.DataFrame({"gvkey": C.index, "sector": ["A", "A", "B", "B"]})
    lab = get_cluster_labels(C, "sector", sector_map=sm)
    assert len(lab) == 4


def test_gind_clustering_from_firm_map():
    C = _tiny_C()
    fm = pd.DataFrame(
        {
            "gvkey": C.index,
            "gsector": ["10", "10", "20", "20"],
            "gind": ["101010", "101010", "202020", "202020"],
        }
    )
    lab = get_cluster_labels(C, "gind", firm_map=fm)
    assert len(lab) == 4
    assert lab.iloc[0] == lab.iloc[1]
    assert lab.iloc[2] == lab.iloc[3]
    assert lab.iloc[0] != lab.iloc[2]
