"""GlobalRank (spectral), MetaCluster, ClusterRank, Hermitian spectrum / permutation tests."""

from __future__ import annotations

import warnings

import numpy as np
import pandas as pd
from sklearn.cluster import SpectralClustering


def hermitian_from_skew(S: np.ndarray) -> np.ndarray:
    return 1j * np.asarray(S, dtype=float)


def eigendecompose_hermitian(A: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    w, V = np.linalg.eigh(A)
    idx = np.argsort(w)[::-1]
    return w[idx], V[:, idx]


def _permute_skew_upper_triangle(S: np.ndarray, rng: np.random.Generator) -> np.ndarray:
    n = S.shape[0]
    iu = np.triu_indices(n, k=1)
    vals = S[iu].copy()
    rng.shuffle(vals)
    Snull = np.zeros_like(S, dtype=float)
    Snull[iu] = vals
    Snull[(iu[1], iu[0])] = -vals
    return Snull


def permutation_test_max_eig(
    S: np.ndarray,
    *,
    n_perm: int = 500,
    seed: int = 0,
) -> tuple[float, float, np.ndarray]:
    rng = np.random.default_rng(seed)
    obs_maxeig = float(np.max(np.linalg.eigvalsh(hermitian_from_skew(S))).real)
    null_maxeig = np.empty(n_perm, dtype=float)
    for b in range(n_perm):
        Snull = _permute_skew_upper_triangle(S, rng)
        null_maxeig[b] = float(np.max(np.linalg.eigvalsh(hermitian_from_skew(Snull))).real)
    pval = (1.0 + np.sum(null_maxeig >= obs_maxeig)) / (n_perm + 1.0)
    return obs_maxeig, float(pval), null_maxeig


def global_rank_spectral_from_C(C: np.ndarray) -> np.ndarray:
    """Leading eigenvector scores of H = i(C − Cᵀ)."""
    C = np.asarray(C, dtype=float)
    S = C - C.T
    H = hermitian_from_skew(S)
    _, V = np.linalg.eigh(H)
    v = V[:, -1]
    s = np.real(v)
    if np.nanmax(np.abs(s)) < 1e-10:
        s = np.abs(v)
    return s


def global_rank_spectral_df(C: pd.DataFrame) -> pd.Series:
    idx = list(C.index)
    scores = global_rank_spectral_from_C(C.to_numpy(dtype=float))
    s = pd.Series(scores, index=idx, name="global_rank_spectral")
    anchor = (C - C.T).sum(axis=1).reindex(s.index)
    corr = float(s.corr(anchor)) if anchor.notna().sum() > 1 else np.nan
    if np.isfinite(corr) and corr < 0:
        s = -s
    return s


def meta_cluster_labels(C: np.ndarray, n_clusters: int = 4, random_state: int = 0) -> np.ndarray:
    """
    **MetaCluster** (not industry): partition the *same nodes* as in the lead–lag matrix `C`
    using sklearn `SpectralClustering` on the nonnegative affinity
    ``A = |C| + |C.T|`` (diagonal zeroed). So clusters are **endogenous** to the estimated
    directed lead–lag strengths at that rebalance—not GICS/NAICS.
    """
    C = np.asarray(C, dtype=float)
    n = C.shape[0]
    if n <= 1:
        return np.zeros(n, dtype=int)
    k = min(n_clusters, n)
    A = np.abs(C) + np.abs(C.T)
    np.fill_diagonal(A, 0.0)
    A_min = A.min()
    if A_min < 0:
        A = A - A_min
    # Spectral embedding warns (and is ill-posed) if the graph is disconnected. Add a
    # tiny weight only on zero off-diagonals so the support is connected without shifting positive edges.
    off = ~np.eye(n, dtype=bool)
    scale = float(np.max(A)) if np.max(A) > 0 else 1.0
    eps = max(np.finfo(float).eps * 1e3, 1e-12 * scale)
    A = A.copy()
    A[off] = np.where(A[off] > 0, A[off], eps)
    # Prefer ARPACK; sklearn may fall back to LOBPCG internally on ill-conditioned Laplacians.
    sc = SpectralClustering(
        n_clusters=k,
        affinity="precomputed",
        random_state=random_state,
        assign_labels="kmeans",
        eigen_solver="arpack",
    )
    try:
        # ARPACK can fall back to LOBPCG inside sklearn; LOBPCG then warns about early exit
        # vs tolerance (~1e-6). Embedding is still fine—only suppress those messages.
        with warnings.catch_warnings():
            for pat in (
                r".*not reaching the requested tolerance.*",
                r".*Exited at iteration.*",
                r".*Exited postprocessing.*",
            ):
                warnings.filterwarnings(
                    "ignore",
                    message=pat,
                    category=UserWarning,
                    module=r"sklearn\.manifold\._spectral_embedding",
                )
            return sc.fit_predict(A)
    except Exception:
        return np.zeros(n, dtype=int)


def cluster_flow_matrix(C: np.ndarray, labels: np.ndarray) -> np.ndarray:
    labs = np.asarray(labels, dtype=int)
    k = int(labs.max()) + 1
    F = np.zeros((k, k), dtype=float)
    idx_by_cluster = {c: np.where(labs == c)[0] for c in range(k)}
    for a in range(k):
        ia = idx_by_cluster[a]
        for b in range(k):
            ib = idx_by_cluster[b]
            if len(ia) == 0 or len(ib) == 0:
                continue
            F[a, b] = float(C[np.ix_(ia, ib)].sum()) / float(len(ia) * len(ib))
    return F


def cluster_net_influence(F: np.ndarray) -> np.ndarray:
    return F.sum(axis=1) - F.sum(axis=0)


def cluster_rank_scores(C: np.ndarray, labels: np.ndarray, local: str = "row_sum") -> np.ndarray:
    n = C.shape[0]
    labs = np.asarray(labels, dtype=int)
    scores = np.zeros(n, dtype=float)
    F = cluster_flow_matrix(C, labs)
    net = cluster_net_influence(F)
    net = (net - np.nanmean(net)) / (np.nanstd(net) + 1e-12)
    uniq = np.unique(labs)
    for c in uniq:
        idx = np.where(labs == c)[0]
        sub = C[np.ix_(idx, idx)]
        if local == "row_sum":
            local_s = sub.sum(axis=1)
        elif local == "eigen":
            local_s = global_rank_spectral_from_C(sub) if sub.size else np.zeros(len(idx))
        else:
            raise ValueError("local must be 'row_sum' or 'eigen'")
        local_s = (local_s - np.nanmean(local_s)) / (np.nanstd(local_s) + 1e-12)
        scores[idx] = local_s + net[c]
    return scores


def cluster_rank_series(C: pd.DataFrame, **kwargs) -> pd.Series:
    """
    ClusterRank: spectral clusters on |C|+|C'|, then local rank within cluster + cluster net flow.

    Pass ``local='row_sum'`` (default) or ``local='eigen'`` for local spectral rank inside each cluster.
    Remaining ``kwargs`` go to :func:`meta_cluster_labels` (e.g. ``n_clusters``, ``random_state``).
    """
    kw = dict(kwargs)
    local = kw.pop("local", "row_sum")
    labels = meta_cluster_labels(C.to_numpy(dtype=float), **kw)
    s = cluster_rank_scores(C.to_numpy(dtype=float), labels, local=local)
    return pd.Series(s, index=C.index, name="cluster_rank")
