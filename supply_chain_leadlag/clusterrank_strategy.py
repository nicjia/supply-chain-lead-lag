"""
ClusterRank strategy: local leaders signal, trade local laggers only within each cluster.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def local_leadingness(C_sub: pd.DataFrame) -> pd.Series:
    """Row-sum of local skew S^{(c)} = C^{(c)} - (C^{(c)})^T."""
    S = C_sub - C_sub.T
    return S.sum(axis=1)


def clusterrank_daily_weights(
    C: pd.DataFrame,
    R_block: pd.DataFrame,
    labels: pd.Series,
    *,
    q: float = 0.2,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Per day t: Signal_{c,t} = mean leader return; long top laggers / short bottom laggers in cluster c.
    Caller applies weights at t+1 for PIT safety.
    """
    nodes = [n for n in labels.index if n in C.index and n in C.columns]
    if not nodes:
        return pd.DataFrame(), pd.DataFrame()

    Cn = C.reindex(index=nodes, columns=nodes, fill_value=0.0)
    labs = labels.reindex(nodes).astype(int)

    weight_rows: list[pd.Series] = []
    score_rows: list[pd.Series] = []

    for t in R_block.index:
        r_t = R_block.loc[t].reindex(nodes).fillna(0.0)
        w = pd.Series(0.0, index=nodes, dtype=float)
        sc = pd.Series(0.0, index=nodes, dtype=float)
        n_cl = 0
        for c, grp in labs.groupby(labs):
            members = list(grp.index)
            if len(members) < 4:
                continue
            sub = Cn.loc[members, members]
            ell = local_leadingness(sub)
            k = max(1, int(len(members) * q))
            leaders = ell.nlargest(k).index
            laggers = ell.nsmallest(k).index
            sig = float(r_t.reindex(leaders).mean())
            if not np.isfinite(sig):
                continue
            top_lag = ell.nlargest(k).index
            bot_lag = ell.nsmallest(k).index
            for ix in top_lag:
                w.loc[ix] += sig / k
            for ix in bot_lag:
                w.loc[ix] -= sig / k
            for ix in laggers:
                sc.loc[ix] = sig
            n_cl += 1
        if n_cl > 0:
            w = w / n_cl
            sc = sc / n_cl
        weight_rows.append(w)
        score_rows.append(sc)

    return pd.DataFrame(weight_rows, index=R_block.index), pd.DataFrame(score_rows, index=R_block.index)
