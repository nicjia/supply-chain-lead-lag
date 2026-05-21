"""
Event flags for conditional panel and backtest analysis.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


CONDITIONS = [
    "all_days",
    "nonzero_exposure",
    "customer_earnings_window",
    "supplier_earnings_window",
    "large_customer_return",
    "high_vol_regime",
]


def _earnings_flags(
    dates: pd.DatetimeIndex,
    gvkeys: pd.Index,
    earnings_calendar: pd.DataFrame,
    role: str,
    window_days: list[int],
) -> pd.DataFrame:
    """Boolean matrix (date x gvkey) for earnings window."""
    out = pd.DataFrame(False, index=dates, columns=gvkeys, dtype=bool)
    if earnings_calendar is None or earnings_calendar.empty:
        return out
    ec = earnings_calendar.copy()
    gcol = "gvkey" if "gvkey" in ec.columns else ec.columns[0]
    dcol = "date" if "date" in ec.columns else "announce_date" if "announce_date" in ec.columns else None
    if dcol is None:
        return out
    ec["gvkey"] = ec[gcol].astype(str).str.zfill(6)
    ec["date"] = pd.to_datetime(ec[dcol]).dt.normalize()
    if role == "customer":
        ec = ec[ec["gvkey"].isin(gvkeys)]
    for _, row in ec.iterrows():
        g = row["gvkey"]
        if g not in gvkeys:
            continue
        ed = pd.Timestamp(row["date"])
        for off in window_days:
            d = ed + pd.Timedelta(days=int(off))
            if d in out.index:
                out.loc[d, g] = True
    return out


def add_event_flags(
    panel_or_signals: pd.DataFrame,
    earnings_calendar: pd.DataFrame | None,
    returns: pd.DataFrame,
    edges: pd.DataFrame,
    config: dict[str, Any],
) -> pd.DataFrame:
    """
    Add event-condition columns for conditional analysis.

    If ``panel_or_signals`` has a ``date`` column, flags are merged on date (and gvkey if present).
    Otherwise treats index as dates.
    """
    df = panel_or_signals.copy()
    if "date" in df.columns:
        dates = pd.to_datetime(df["date"]).unique()
    elif isinstance(df.index, pd.DatetimeIndex):
        dates = df.index.unique()
    else:
        dates = returns.index

    dates = pd.DatetimeIndex(pd.to_datetime(dates)).sort_values()
    window = config.get("earnings_window_days", [-1, 0, 1])
    large_q = float(config.get("large_customer_return_quantile", 0.95))
    vol_q = float(config.get("high_vol_quantile", 0.80))

    customers = edges["customer_gvkey"].astype(str).unique() if not edges.empty else []
    suppliers = edges["supplier_gvkey"].astype(str).unique() if not edges.empty else []

    cust_flags = _earnings_flags(dates, pd.Index(customers), earnings_calendar, "customer", window)
    sup_flags = _earnings_flags(dates, pd.Index(suppliers), earnings_calendar, "supplier", window)

    # Large customer return: any customer in top quantile cross-sectionally
    R = returns.reindex(dates).fillna(0.0)
    cust_cols = [c for c in customers if c in R.columns]
    large_cust = pd.Series(False, index=dates)
    if cust_cols:
        thresh = R[cust_cols].quantile(large_q, axis=1)
        large_cust = (R[cust_cols].gt(thresh, axis=0)).any(axis=1)

    # High vol: cross-sectional return dispersion
    vol = R.std(axis=1)
    high_vol = vol >= vol.quantile(vol_q) if vol.notna().sum() > 5 else pd.Series(False, index=dates)

    flags = pd.DataFrame(index=dates)
    flags["all_days"] = True
    flags["customer_earnings_window"] = cust_flags.any(axis=1) if not cust_flags.empty else False
    flags["supplier_earnings_window"] = sup_flags.any(axis=1) if not sup_flags.empty else False
    flags["large_customer_return"] = large_cust.reindex(dates).fillna(False).values
    flags["high_vol_regime"] = high_vol.reindex(dates).fillna(False).values

    if "exposure" in df.columns:
        flags["nonzero_exposure"] = df.set_index("date")["exposure"].abs() > 1e-12 if "date" in df.columns else False
    elif "weight" in df.columns:
        flags["nonzero_exposure"] = df.set_index("date")["weight"].abs() > 1e-12 if "date" in df.columns else False
    else:
        flags["nonzero_exposure"] = True

    if "date" in df.columns:
        return df.merge(flags.reset_index().rename(columns={"index": "date"}), on="date", how="left")
    return df.join(flags, how="left")


def mask_returns_by_condition(
    daily_returns: pd.Series,
    flags: pd.DataFrame,
    condition: str,
) -> pd.Series:
    """Zero returns on days not matching ``condition``; ``all_days`` keeps everything."""
    if condition == "all_days":
        return daily_returns
    if condition not in flags.columns:
        return daily_returns * np.nan
    m = flags[condition].reindex(daily_returns.index).fillna(False)
    out = daily_returns.copy()
    out.loc[~m] = 0.0
    return out
