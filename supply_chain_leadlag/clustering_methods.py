"""
Clustering methods for lead-lag research (sector, supply graph, spectral, Hermitian, signed, hybrid).
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
from sklearn.cluster import SpectralClustering

from supply_chain_leadlag.global_structure import meta_cluster_labels
from supply_chain_leadlag.matrix import hybrid_matrix, structural_C_from_edges


class ClusteringMethodError(ValueError):
    """Raised when a clustering method cannot run (e.g. missing sector map)."""


def _nodes(C: pd.DataFrame) -> list[str]:
    return sorted(set(C.index.astype(str)).union(set(C.columns.astype(str))))


def _align_C(C: pd.DataFrame, nodes: list[str]) -> np.ndarray:
    Cn = C.reindex(index=nodes, columns=nodes, fill_value=0.0)
    return Cn.to_numpy(dtype=float)


def _spectral_on_affinity(A: np.ndarray, n_clusters: int, random_state: int) -> np.ndarray:
    n = A.shape[0]
    if n <= 1:
        return np.zeros(n, dtype=int)
    k = min(n_clusters, n)
    off = ~np.eye(n, dtype=bool)
    scale = float(np.max(A)) if np.max(A) > 0 else 1.0
    eps = max(np.finfo(float).eps * 1e3, 1e-12 * scale)
    A = A.copy()
    A[off] = np.where(A[off] > 0, A[off], eps)
    sc = SpectralClustering(
        n_clusters=k,
        affinity="precomputed",
        random_state=random_state,
        assign_labels="kmeans",
        eigen_solver="arpack",
    )
    try:
        return sc.fit_predict(A)
    except Exception:
        return np.zeros(n, dtype=int)


def cluster_sector(
    nodes: list[str],
    sector_map: pd.DataFrame | pd.Series | None,
) -> np.ndarray:
    """
    Cluster by sector label. ``sector_map`` must index gvkey with sector values
    (columns ``gvkey``, ``sector`` or a Series indexed by gvkey).
    """
    if sector_map is None or (isinstance(sector_map, pd.DataFrame) and sector_map.empty):
        raise ClusteringMethodError("sector clustering requires sector_map")
    if isinstance(sector_map, pd.DataFrame):
        if "gvkey" in sector_map.columns and "sector" in sector_map.columns:
            sm = sector_map.set_index("gvkey")["sector"]
        else:
            raise ClusteringMethodError("sector_map DataFrame needs gvkey, sector columns")
    else:
        sm = sector_map
    sm = sm.astype(str)
    labels = []
    sectors = []
    for n in nodes:
        s = sm.get(n, sm.get(str(n).zfill(6), "UNKNOWN"))
        if s not in sectors:
            sectors.append(s)
        labels.append(sectors.index(s))
    return np.asarray(labels, dtype=int)


def cluster_supply_community(
    edges_pit: pd.DataFrame,
    nodes: list[str],
    n_clusters: int,
    random_state: int,
) -> np.ndarray:
    """Spectral clustering on structural supply adjacency |w| + |w'|."""
    C_sup = structural_C_from_edges(edges_pit, nodes)
    A = np.abs(C_sup.to_numpy(dtype=float)) + np.abs(C_sup.to_numpy(dtype=float).T)
    np.fill_diagonal(A, 0.0)
    return _spectral_on_affinity(A, n_clusters, random_state)


def cluster_symmetric_spectral(C: np.ndarray, n_clusters: int, random_state: int) -> np.ndarray:
    """A_sym = |C| + |C'|, direction-blind strength clustering."""
    A = np.abs(C) + np.abs(C.T)
    np.fill_diagonal(A, 0.0)
    return _spectral_on_affinity(A, n_clusters, random_state)


def _quantize_features_to_labels(X: np.ndarray, n_clusters: int) -> np.ndarray:
    """Assign cluster ids by ranking first principal feature (no sklearn KMeans)."""
    n = X.shape[0]
    k = min(n_clusters, n)
    if k <= 1:
        return np.zeros(n, dtype=int)
    feat = X[:, 0]
    ranks = np.argsort(np.argsort(feat))
    return (ranks * k // max(n, 1)).astype(int)


def cluster_hermitian(C: np.ndarray, n_clusters: int, random_state: int) -> np.ndarray:
    """
    Embed via leading eigenvectors of H = i(C - C^T); cluster on [Re(v), Im(v)] features.
    """
    del random_state
    S = C - C.T
    H = 1j * S
    w, V = np.linalg.eigh(H)
    idx = np.argsort(np.abs(w))[::-1]
    k_embed = min(max(2, n_clusters), C.shape[0])
    feats = []
    for j in range(k_embed):
        v = V[:, idx[j]]
        feats.append(np.real(v))
        feats.append(np.imag(v))
    X = np.column_stack(feats) if feats else np.zeros((C.shape[0], 1))
    X = (X - X.mean(axis=0)) / (X.std(axis=0) + 1e-12)
    return _quantize_features_to_labels(X, n_clusters)


def cluster_signed(C: np.ndarray, n_clusters: int, random_state: int) -> np.ndarray:
    """
    Simple signed spectral baseline: positive/negative parts of S = C - C^T,
    concatenate row features from S+ and S-, then quantile clustering.
    """
    del random_state
    S = C - C.T
    Sp = np.maximum(S, 0.0)
    Sn = np.maximum(-S, 0.0)
    X = np.hstack([Sp, Sn])
    X = (X - X.mean(axis=0)) / (X.std(axis=0) + 1e-12)
    return _quantize_features_to_labels(X, n_clusters)


def get_cluster_labels(
    C: pd.DataFrame,
    method: str,
    n_clusters: int = 10,
    side_info: pd.DataFrame | None = None,
    sector_map: pd.DataFrame | pd.Series | None = None,
    random_state: int = 42,
    *,
    edges_pit: pd.DataFrame | None = None,
    C_supply: pd.DataFrame | None = None,
    hybrid_prior_alpha: float = 0.5,
    **kwargs: Any,
) -> pd.Series:
    """
    Return cluster label for each node in ``C.index`` ∪ ``C.columns``.

    Methods: ``sector``, ``supply_community``, ``symmetric_spectral``, ``hermitian``,
    ``signed``, ``hybrid_prior`` (Hermitian on hybrid matrix).
    """
    del kwargs
    nodes = _nodes(C)
    Cn = _align_C(C, nodes)

    if method == "sector":
        lab = cluster_sector(nodes, sector_map)
    elif method == "supply_community":
        if edges_pit is None:
            raise ClusteringMethodError("supply_community requires edges_pit")
        lab = cluster_supply_community(edges_pit, nodes, n_clusters, random_state)
    elif method == "symmetric_spectral":
        lab = cluster_symmetric_spectral(Cn, n_clusters, random_state)
    elif method == "hermitian":
        lab = cluster_hermitian(Cn, n_clusters, random_state)
    elif method == "signed":
        lab = cluster_signed(Cn, n_clusters, random_state)
    elif method == "hybrid_prior":
        if C_supply is None:
            if edges_pit is None:
                raise ClusteringMethodError("hybrid_prior requires edges_pit or C_supply")
            C_supply = structural_C_from_edges(edges_pit, nodes)
        Ch = hybrid_matrix(C.reindex(index=nodes, columns=nodes, fill_value=0.0), C_supply, hybrid_prior_alpha)
        lab = cluster_hermitian(Ch.to_numpy(dtype=float), n_clusters, random_state)
    else:
        # Fallback: existing MetaCluster on |C|+|C'|
        lab = meta_cluster_labels(Cn, n_clusters=n_clusters, random_state=random_state)

    return pd.Series(lab, index=nodes, name="cluster_label")
