"""
Clean Compustat / WRDS firm classification exports and build gvkey-level maps for clustering.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

# GICS columns from Compustat (coarse → fine). NAICS/SIC are US industry codes.
GICS_COLUMNS = ("gsector", "ggroup", "gind", "gsubind")
INDUSTRY_CODE_COLUMNS = ("naics", "sic")

CLASSIFICATION_CLUSTER_METHODS: dict[str, str] = {
    # Friendly aliases → column in firm_classification_map
    "sector": "gsector",
    "gsector": "gsector",
    "ggroup": "ggroup",
    "industry_group": "ggroup",
    "gind": "gind",
    "industry": "gind",
    "gsubind": "gsubind",
    "subindustry": "gsubind",
    "naics": "naics",
    "naics2": "naics2",
    "naics3": "naics3",
    "naics6": "naics",
    "sic": "sic",
    "sic2": "sic2",
    "sic4": "sic4",
}

CLASSIFICATION_METHOD_DESCRIPTIONS: dict[str, str] = {
    "sector": "GICS sector (gsector, ~11 groups)",
    "gsector": "GICS sector (gsector)",
    "ggroup": "GICS industry group (ggroup, ~27)",
    "gind": "GICS industry (gind, ~81)",
    "gsubind": "GICS sub-industry (gsubind, ~189)",
    "naics": "Full 6-digit NAICS",
    "naics2": "NAICS 2-digit sector",
    "naics3": "NAICS 3-digit subsector",
    "sic": "4-digit SIC",
    "sic2": "SIC 2-digit division",
    "sic4": "SIC 4-digit industry",
}


def _zfill_gvkey(s: pd.Series) -> pd.Series:
    return s.astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(6)


def _code_str(x) -> str:
    if pd.isna(x):
        return ""
    if isinstance(x, float) and x == int(x):
        return str(int(x))
    return str(x).strip()


def clean_wrds_classification(
    df: pd.DataFrame,
    *,
    keep_latest: bool = True,
) -> pd.DataFrame:
    """
    Drop duplicate rows and collapse to one row per ``gvkey``.

    WRDS exports are annual panels; GICS/NAICS/SIC are time-invariant per gvkey in practice.
    ``keep_latest=True`` keeps the row with the latest ``datadate`` per gvkey.
    """
    need = {"gvkey"}
    if not need.issubset(df.columns):
        raise ValueError(f"WRDS classification needs columns {need}, got {list(df.columns)}")

    out = df.copy()
    out["gvkey"] = _zfill_gvkey(out["gvkey"])
    out = out.drop_duplicates()

    if keep_latest and "datadate" in out.columns:
        out["datadate"] = pd.to_datetime(out["datadate"], errors="coerce")
        out = out.sort_values(["gvkey", "datadate"]).groupby("gvkey", as_index=False).last()
    else:
        out = out.groupby("gvkey", as_index=False).first()

    return out


def build_firm_classification_map(clean: pd.DataFrame) -> pd.DataFrame:
    """One row per gvkey with GICS, NAICS, SIC, and derived 2/3/4-digit buckets."""
    cols = ["gvkey"]
    for c in ["conm", "tic", "datadate", "fyear"] + list(GICS_COLUMNS) + list(INDUSTRY_CODE_COLUMNS):
        if c in clean.columns:
            cols.append(c)

    m = clean[cols].copy()
    m["gvkey"] = _zfill_gvkey(m["gvkey"])

    for c in GICS_COLUMNS:
        if c in m.columns:
            m[c] = m[c].apply(_code_str)

    if "naics" in m.columns:
        m["naics"] = m["naics"].apply(lambda x: _code_str(x).zfill(6)[:6] if _code_str(x) else "")
        m["naics2"] = m["naics"].str[:2].replace("", pd.NA)
        m["naics3"] = m["naics"].str[:3].replace("", pd.NA)

    if "sic" in m.columns:
        m["sic"] = m["sic"].apply(lambda x: _code_str(x).zfill(4)[:4] if _code_str(x) else "")
        m["sic2"] = m["sic"].str[:2].replace("", pd.NA)
        m["sic4"] = m["sic"].replace("", pd.NA)

    return m.sort_values("gvkey").reset_index(drop=True)


def build_sector_map(firm_map: pd.DataFrame) -> pd.DataFrame:
    """Legacy ``gvkey, sector`` table (sector = GICS ``gsector`` code)."""
    if "gsector" not in firm_map.columns:
        raise ValueError("firm_map must include gsector for sector_map")
    sm = firm_map[["gvkey", "gsector"]].rename(columns={"gsector": "sector"})
    sm["sector"] = sm["sector"].astype(str)
    sm.loc[sm["sector"].isin(("", "nan", "None")), "sector"] = "UNKNOWN"
    return sm


def write_classification_artifacts(
    wrds_path: str | Path,
    *,
    repo_root: Path | None = None,
    clean_name: str = "data/wrds_classification_clean.csv",
    firm_map_name: str = "data/firm_classification_map.csv",
    sector_map_name: str = "data/sector_map.csv",
) -> dict[str, Path]:
    """Read raw WRDS CSV, clean, and write derived maps."""
    root = repo_root or Path(__file__).resolve().parents[1]
    raw = pd.read_csv(wrds_path, low_memory=False)
    clean = clean_wrds_classification(raw)
    firm = build_firm_classification_map(clean)
    sector = build_sector_map(firm)

    paths = {
        "clean": root / clean_name,
        "firm_map": root / firm_map_name,
        "sector_map": root / sector_map_name,
    }
    for p in paths.values():
        p.parent.mkdir(parents=True, exist_ok=True)

    # Slim clean file for inspection
    slim_cols = [c for c in ["gvkey", "datadate", "conm", "tic"] + list(GICS_COLUMNS) + list(INDUSTRY_CODE_COLUMNS) if c in clean.columns]
    clean[slim_cols].to_csv(paths["clean"], index=False)
    firm.to_csv(paths["firm_map"], index=False)
    sector.to_csv(paths["sector_map"], index=False)

    return paths


def load_firm_classification_map(path: str | Path | None) -> pd.DataFrame | None:
    p = Path(path) if path else None
    if p is None or not p.is_file():
        return None
    df = pd.read_csv(p, low_memory=False)
    if "gvkey" not in df.columns:
        return None
    df["gvkey"] = _zfill_gvkey(df["gvkey"])
    return df.set_index("gvkey", drop=False)


def classification_column_for_method(method: str) -> str | None:
    return CLASSIFICATION_CLUSTER_METHODS.get(method)
