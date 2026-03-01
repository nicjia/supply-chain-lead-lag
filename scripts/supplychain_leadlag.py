# scripts/supplychain_leadlag.py
from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Optional, Tuple, Dict, List

import numpy as np
import pandas as pd


# ----------------------------
# IO + basic cleaning
# ----------------------------
def read_supplychain(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    # expected columns: supplier_gvkey, customer_gvkey, amount, date (and optional tic/srcdate)
    for c in ["supplier_gvkey", "customer_gvkey", "amount", "date"]:
        if c not in df.columns:
            raise ValueError(f"Missing column {c} in {csv_path}")
    df["supplier_gvkey"] = df["supplier_gvkey"].astype(int)
    df["customer_gvkey"] = df["customer_gvkey"].astype(int)
    df["amount"] = df["amount"].astype(float)
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date")


def read_returns(path: str) -> pd.DataFrame:
    """
    Expected schema: date, gvkey, ret (daily simple or log return).
    """
    if path.endswith(".parquet"):
        df = pd.read_parquet(path)
    else:
        df = pd.read_csv(path)

    for c in ["date", "gvkey", "ret"]:
        if c not in df.columns:
            raise ValueError(f"Returns file must contain columns: date, gvkey, ret. Missing {c}.")
    df["date"] = pd.to_datetime(df["date"])
    df["gvkey"] = df["gvkey"].astype(int)
    df["ret"] = df["ret"].astype(float)
    return df.sort_values(["date", "gvkey"])


def pivot_returns(ret_df: pd.DataFrame) -> pd.DataFrame:
    """
    Wide matrix R: index=date, columns=gvkey, values=ret
    """
    R = ret_df.pivot(index="date", columns="gvkey", values="ret").sort_index()
    return R


# ----------------------------
# Structural network from transactions
# ----------------------------
def exposure_matrix(
    tx_df: pd.DataFrame,
    firms: np.ndarray,
    start: pd.Timestamp,
    end: pd.Timestamp,
    normalize_rows: bool = True,
) -> np.ndarray:
    """
    Build W over [start,end] with W[i,j] = sum(amount) for supplier->customer.

    firms: array of gvkeys in fixed order.
    """
    sub = tx_df[(tx_df["date"] >= start) & (tx_df["date"] <= end)]
    idx = {g: k for k, g in enumerate(firms)}
    W = np.zeros((len(firms), len(firms)), dtype=float)

    for row in sub.itertuples(index=False):
        si = idx.get(int(row.supplier_gvkey), None)
        cj = idx.get(int(row.customer_gvkey), None)
        if si is None or cj is None:
            continue
        W[si, cj] += float(row.amount)

    if normalize_rows:
        row_sum = W.sum(axis=1, keepdims=True)
        row_sum[row_sum == 0.0] = 1.0
        W = W / row_sum
    return W


# ----------------------------
# Pairwise lead-lag scoring (cross-correlation)
# ----------------------------
def _corr(x: np.ndarray, y: np.ndarray) -> float:
    x = x - np.nanmean(x)
    y = y - np.nanmean(y)
    denom = (np.nanstd(x) * np.nanstd(y))
    if denom <= 0 or np.isnan(denom):
        return np.nan
    return float(np.nanmean(x * y) / denom)


def leadlag_score_corr(
    leader: np.ndarray,
    follower: np.ndarray,
    max_lag: int = 5,
) -> Tuple[float, int]:
    """
    Compute score = max_{lag=1..max_lag} corr( leader[t], follower[t+lag] )
    Return (best_score, best_lag).
    Arrays are aligned on the same base window.
    """
    best = -np.inf
    best_lag = 1
    T = len(leader)
    for lag in range(1, max_lag + 1):
        if T - lag <= 5:
            break
        x = leader[: T - lag]
        y = follower[lag:]
        c = _corr(x, y)
        if np.isnan(c):
            continue
        if c > best:
            best, best_lag = c, lag
    if best == -np.inf:
        return (np.nan, 1)
    return (best, best_lag)


# ----------------------------
# Lead-lag matrix construction (sparse on edges)
# ----------------------------
@dataclass
class LeadLagResult:
    dates: List[pd.Timestamp]
    firms: np.ndarray
    leadingness: pd.DataFrame  # index=date, columns=gvkey
    best_lag: Dict[pd.Timestamp, np.ndarray]  # date -> (n,n) int lag matrix (0 if no edge)
    C: Dict[pd.Timestamp, np.ndarray]         # date -> (n,n) score matrix (sparse)
    A: Dict[pd.Timestamp, np.ndarray]         # date -> skew-sym matrix


def compute_leadlag_matrices(
    R: pd.DataFrame,
    tx_df: pd.DataFrame,
    lookback_ret: int = 60,
    lookback_tx_days: int = 365,
    max_lag: int = 5,
    min_obs: int = 40,
    edge_weight_threshold: float = 0.0,
    step: int = 1,
) -> LeadLagResult:
    """
    Rolling estimation:
      - For each anchor date t:
          - build exposure W using tx in [t-lookback_tx_days, t]
          - compute C_ij on edges where W_ij > threshold
          - A = C - C^T
          - leadingness L(i)=sum_j A_ij
    """
    dates = list(R.index)
    firms = R.columns.to_numpy(dtype=int)
    n = len(firms)

    leadingness_rows = []
    out_dates = []
    C_map: Dict[pd.Timestamp, np.ndarray] = {}
    A_map: Dict[pd.Timestamp, np.ndarray] = {}
    lag_map: Dict[pd.Timestamp, np.ndarray] = {}

    for t_idx in range(lookback_ret + max_lag, len(dates), step):
        t = dates[t_idx]
        ret_window = R.iloc[t_idx - lookback_ret - max_lag : t_idx]  # include lag room
        if ret_window.shape[0] < lookback_ret:
            continue

        # Tx lookback window (calendar days)
        tx_start = pd.Timestamp(t) - pd.Timedelta(days=lookback_tx_days)
        W = exposure_matrix(tx_df, firms=firms, start=tx_start, end=pd.Timestamp(t), normalize_rows=True)

        # Determine edges to score
        edges = np.argwhere(W > edge_weight_threshold)  # (i,j)
        C = np.zeros((n, n), dtype=float)
        LAG = np.zeros((n, n), dtype=int)

        # Score only edges
        for (i, j) in edges:
            xi = ret_window.iloc[:, i].to_numpy()
            yj = ret_window.iloc[:, j].to_numpy()

            # Need enough non-NaN
            ok = np.isfinite(xi) & np.isfinite(yj)
            if ok.sum() < min_obs:
                continue

            # Use only the aligned window (keep NaNs handled by corr)
            score, lag = leadlag_score_corr(xi, yj, max_lag=max_lag)
            if np.isnan(score):
                continue

            # Optionally weight by exposure strength
            C[i, j] = score * W[i, j]
            LAG[i, j] = lag

        A = C - C.T
        lead = A.sum(axis=1)  # leadingness per node

        leadingness_rows.append(lead)
        out_dates.append(pd.Timestamp(t))
        C_map[pd.Timestamp(t)] = C
        A_map[pd.Timestamp(t)] = A
        lag_map[pd.Timestamp(t)] = LAG

    leadingness_df = pd.DataFrame(
        np.vstack(leadingness_rows),
        index=pd.DatetimeIndex(out_dates, name="date"),
        columns=firms,
    )
    return LeadLagResult(
        dates=out_dates,
        firms=firms,
        leadingness=leadingness_df,
        best_lag=lag_map,
        C=C_map,
        A=A_map,
    )


# ----------------------------
# Simple GlobalRank portfolio backtest
# ----------------------------
@dataclass
class BacktestResult:
    pnl: pd.Series
    daily_ret: pd.Series
    positions: Dict[pd.Timestamp, np.ndarray]  # date -> weights


def backtest_globalrank(
    R: pd.DataFrame,
    leadingness: pd.DataFrame,
    q: float = 0.2,
    long_short: bool = True,
) -> BacktestResult:
    """
    On each date t in leadingness index, form:
      - leaders = top q%
      - laggers = bottom q%
    Signal trades next-day return (t+1).
    """
    dates = leadingness.index
    firms = leadingness.columns.to_numpy(dtype=int)
    pos_map: Dict[pd.Timestamp, np.ndarray] = {}

    rets = []
    out_dates = []

    for t in dates:
        if t not in R.index:
            continue
        t_loc = R.index.get_loc(t)
        if t_loc + 1 >= len(R.index):
            continue  # no next day
        t_next = R.index[t_loc + 1]

        scores = leadingness.loc[t].to_numpy(dtype=float)
        valid = np.isfinite(scores)
        if valid.sum() < 10:
            continue

        order = np.argsort(scores[valid])  # ascending
        valid_idx = np.where(valid)[0]

        k = max(1, int(q * valid.sum()))
        laggers = valid_idx[order[:k]]
        leaders = valid_idx[order[-k:]]

        w = np.zeros(len(firms), dtype=float)
        # equal weight within buckets
        w[laggers] = 1.0 / k
        if long_short:
            w[leaders] = -1.0 / k

        # realized next-day return
        r_next = R.loc[t_next].to_numpy(dtype=float)
        daily = np.nansum(w * r_next)

        pos_map[pd.Timestamp(t)] = w
        rets.append(daily)
        out_dates.append(pd.Timestamp(t_next))

    daily_ret = pd.Series(rets, index=pd.DatetimeIndex(out_dates, name="date"), name="strategy_ret")
    pnl = (1.0 + daily_ret.fillna(0.0)).cumprod() - 1.0
    return BacktestResult(pnl=pnl, daily_ret=daily_ret, positions=pos_map)


def sharpe(x: pd.Series, ann: int = 252) -> float:
    x = x.dropna()
    if x.std() == 0 or len(x) < 10:
        return np.nan
    return float(np.sqrt(ann) * x.mean() / x.std())


# ----------------------------
# CLI
# ----------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tx_csv", required=True, help="Supply-chain transactions CSV")
    ap.add_argument("--returns", required=True, help="Returns file (parquet or csv) with date, gvkey, ret")
    ap.add_argument("--lookback_ret", type=int, default=60)
    ap.add_argument("--lookback_tx_days", type=int, default=365)
    ap.add_argument("--max_lag", type=int, default=5)
    ap.add_argument("--min_obs", type=int, default=40)
    ap.add_argument("--edge_weight_threshold", type=float, default=0.0)
    ap.add_argument("--q", type=float, default=0.2)
    ap.add_argument("--out_csv", default="results_supplychain_leadlag.csv")
    args = ap.parse_args()

    tx = read_supplychain(args.tx_csv)
    ret_df = read_returns(args.returns)
    R = pivot_returns(ret_df)

    res = compute_leadlag_matrices(
        R=R,
        tx_df=tx,
        lookback_ret=args.lookback_ret,
        lookback_tx_days=args.lookback_tx_days,
        max_lag=args.max_lag,
        min_obs=args.min_obs,
        edge_weight_threshold=args.edge_weight_threshold,
        step=1,
    )

    bt = backtest_globalrank(R, res.leadingness, q=args.q, long_short=True)

    summary = {
        "sharpe": sharpe(bt.daily_ret),
        "mean_daily": float(bt.daily_ret.mean()),
        "vol_daily": float(bt.daily_ret.std()),
        "start": str(bt.daily_ret.index.min()),
        "end": str(bt.daily_ret.index.max()),
        "n_days": int(bt.daily_ret.shape[0]),
    }
    print("===== SUMMARY =====")
    for k, v in summary.items():
        print(f"{k}: {v}")

    out = pd.DataFrame({
        "date": bt.daily_ret.index,
        "strategy_ret": bt.daily_ret.values,
        "pnl": bt.pnl.reindex(bt.daily_ret.index).values,
    })
    out.to_csv(args.out_csv, index=False)
    print(f"Wrote: {args.out_csv}")


if __name__ == "__main__":
    main()