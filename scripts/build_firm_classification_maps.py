#!/usr/bin/env python3
"""Clean WRDS classification export and build firm/sector maps for clustering."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from supply_chain_leadlag.classification_data import (
    CLASSIFICATION_METHOD_DESCRIPTIONS,
    clean_wrds_classification,
    write_classification_artifacts,
)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--input",
        default=str(_ROOT / "data" / "wrds_classification.csv"),
        help="Raw WRDS / Compustat classification CSV",
    )
    ap.add_argument("--repo-root", default=str(_ROOT))
    args = ap.parse_args()

    paths = write_classification_artifacts(args.input, repo_root=Path(args.repo_root))

    import pandas as pd

    firm = pd.read_csv(paths["firm_map"])
    print(f"Wrote {paths['clean']} ({len(pd.read_csv(paths['clean']))} gvkeys, deduped)")
    print(f"Wrote {paths['firm_map']} ({len(firm)} gvkeys)")
    print(f"Wrote {paths['sector_map']} (gvkey → gsector)")
    print("\nClassification columns for clustering:")
    for method, desc in CLASSIFICATION_METHOD_DESCRIPTIONS.items():
        col = {"sector": "gsector", "industry": "gind", "industry_group": "ggroup", "subindustry": "gsubind"}.get(
            method, method
        )
        if col in firm.columns:
            n = firm[col].astype(str).replace("", pd.NA).dropna().nunique()
            print(f"  {method:12} → {col:8}  ({n} unique labels)")
    print("\nUse in config clustering.methods: sector, ggroup, gind, gsubind, naics2, naics, sic2, sic4, ...")


if __name__ == "__main__":
    main()
