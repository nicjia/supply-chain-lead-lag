"""
Cluster stability: Adjusted Rand Index and Hermitian eigenspace drift.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.metrics import adjusted_rand_score

from supply_chain_leadlag.global_structure import hermitian_from_skew


def adjusted_rand_index(prev: np.ndarray, curr: np.ndarray) -> float:
    """ARI between two label vectors (same length)."""
    if len(prev) != len(curr) or len(prev) == 0:
        return float(np.nan)
    return float(adjusted_rand_score(prev, curr))


def hermitian_embedding(C: np.ndarray, n_components: int = 5) -> np.ndarray:
    """Top-k eigenvectors of H = i(C - C^T) as complex matrix (n x k)."""
    S = C - C.T
    H = hermitian_from_skew(S)
    w, V = np.linalg.eigh(H)
    idx = np.argsort(np.abs(w))[::-1][:n_components]
    return V[:, idx]


def eigenspace_drift(V_curr: np.ndarray, V_prev: np.ndarray) -> float:
    """
    d = ||V V^* - V_prev V_prev^*||_F using conjugate transpose for complex V.
    """
    if V_curr.size == 0 or V_prev.size == 0:
        return float(np.nan)
    k = min(V_curr.shape[1], V_prev.shape[1])
    Vc = V_curr[:, :k]
    Vp = V_prev[:, :k]
    Pc = Vc @ Vc.conj().T
    Pp = Vp @ Vp.conj().T
    return float(np.linalg.norm(Pc - Pp, ord="fro"))


def stability_row(
    rebalance_date: pd.Timestamp,
    cluster_method: str,
    strategy_family: str,
    labels: pd.Series,
    labels_prev: pd.Series | None,
    C: pd.DataFrame,
    C_prev: pd.DataFrame | None,
    n_clusters: int,
) -> dict:
    nodes = sorted(labels.index.astype(str))
    lab = labels.reindex(nodes).fillna(0).astype(int).to_numpy()
    ari = float(np.nan)
    if labels_prev is not None:
        lp = labels_prev.reindex(nodes).fillna(-1).astype(int).to_numpy()
        ari = adjusted_rand_index(lp, lab)

    drift = float(np.nan)
    Cn = C.reindex(index=nodes, columns=nodes, fill_value=0.0).to_numpy(dtype=float)
    if C_prev is not None:
        Cp = C_prev.reindex(index=nodes, columns=nodes, fill_value=0.0).to_numpy(dtype=float)
        Vc = hermitian_embedding(Cn)
        Vp = hermitian_embedding(Cp)
        drift = eigenspace_drift(Vc, Vp)

    sizes = pd.Series(lab).value_counts()
    return {
        "rebalance_date": rebalance_date,
        "cluster_method": cluster_method,
        "strategy_family": strategy_family,
        "n_clusters": n_clusters,
        "ari_prev": ari,
        "eigenspace_drift": drift,
        "n_assets": len(nodes),
        "largest_cluster_size": int(sizes.max()) if len(sizes) else 0,
        "smallest_cluster_size": int(sizes.min()) if len(sizes) else 0,
    }
