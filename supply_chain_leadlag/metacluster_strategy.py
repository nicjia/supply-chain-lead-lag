"""
MetaCluster strategy: cluster meta-flow edges, trade lagging clusters from leading cluster returns.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def cluster_directed_flow_matrix(C: np.ndarray, labels: np.ndarray) -> np.ndarray:
    """
    F[a,b] = (1/|C_a||C_b|) sum_{i in C_a, j in C_b} (C_ij - C_ji).
    """
    labs = np.asarray(labels, dtype=int)
    k = int(labs.max()) + 1 if labs.size else 0
    F = np.zeros((k, k), dtype=float)
    S = C - C.T
    for a in range(k):
        ia = np.where(labs == a)[0]
        for b in range(k):
            ib = np.where(labs == b)[0]
            if len(ia) == 0 or len(ib) == 0:
                continue
            block = S[np.ix_(ia, ib)]
            F[a, b] = float(block.sum()) / float(len(ia) * len(ib))
    return F


def select_meta_flow_edges(
    F: np.ndarray,
    *,
    top_frac: float = 0.10,
    min_edges: int = 1,
) -> list[tuple[int, int, float]]:
    """Positive directed meta-flow pairs (a -> b), a != b."""
    k = F.shape[0]
    pairs: list[tuple[int, int, float]] = []
    for a in range(k):
        for b in range(k):
            if a == b:
                continue
            v = float(F[a, b])
            if v > 0:
                pairs.append((a, b, v))
    if not pairs:
        return []
    pairs.sort(key=lambda x: x[2], reverse=True)
    n_keep = max(min_edges, int(np.ceil(len(pairs) * top_frac)))
    return pairs[:n_keep]


def metacluster_daily_weights(
    C: pd.DataFrame,
    R_block: pd.DataFrame,
    labels: pd.Series,
    *,
    q: float = 0.2,
    top_frac: float = 0.10,
) -> tuple[pd.DataFrame, pd.DataFrame, np.ndarray]:
    """
    For each day in ``R_block``, build weights on lagging-cluster firms.

    Leading cluster return at t signals trade on lagging cluster b at t+1 (weights applied next day
    by caller). Returns (weights_df, scores_df, meta_flow_matrix).
    """
    nodes = [n for n in labels.index if n in C.index and n in C.columns]
    if not nodes:
        return pd.DataFrame(), pd.DataFrame(), np.zeros((0, 0))
    Cn = C.reindex(index=nodes, columns=nodes, fill_value=0.0)
    lab_arr = labels.reindex(nodes).fillna(0).astype(int).to_numpy()
    F = cluster_directed_flow_matrix(Cn.to_numpy(dtype=float), lab_arr)
    edges = select_meta_flow_edges(F, top_frac=top_frac)

    idx_by_cluster: dict[int, list[str]] = {}
    for n, c in zip(nodes, lab_arr):
        idx_by_cluster.setdefault(int(c), []).append(n)

    weight_rows: list[pd.Series] = []
    score_rows: list[pd.Series] = []

    for t in R_block.index:
        r_t = R_block.loc[t].reindex(nodes).fillna(0.0)
        w = pd.Series(0.0, index=nodes, dtype=float)
        sc = pd.Series(0.0, index=nodes, dtype=float)
        n_active = 0
        for a, b, flow in edges:
            members_a = idx_by_cluster.get(a, [])
            members_b = idx_by_cluster.get(b, [])
            if not members_a or not members_b:
                continue
            sig = float(r_t.reindex(members_a).mean())
            if not np.isfinite(sig) or abs(sig) < 1e-15:
                continue
            # Rank laggers in cluster b by local net lag (row-sum of S within cluster)
            sub = Cn.loc[members_b, members_b]
            local_lag = (sub - sub.T).sum(axis=1)
            order = local_lag.sort_values()
            k = max(1, int(len(order) * q))
            long_lag = order.head(k).index
            short_lag = order.tail(k).index
            sign = float(np.sign(sig))
            for ix in long_lag:
                w.loc[ix] += sign / k
            for ix in short_lag:
                w.loc[ix] -= sign / k
            for ix in members_b:
                sc.loc[ix] = sig * float(flow)
            n_active += 1
        if n_active > 0:
            w = w / n_active
            sc = sc / n_active
        weight_rows.append(w)
        score_rows.append(sc)

    weights_df = pd.DataFrame(weight_rows, index=R_block.index)
    scores_df = pd.DataFrame(score_rows, index=R_block.index)
    return weights_df, scores_df, F
