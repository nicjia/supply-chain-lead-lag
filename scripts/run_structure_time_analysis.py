#!/usr/bin/env python3
"""Intra/inter cluster predictability and crisis-period analysis for presentations."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from supply_chain_leadlag.structure_time_analysis import run_structure_time_analysis


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--config", default=str(_ROOT / "config" / "research.yaml"))
    ap.add_argument(
        "--results-dir",
        default=str(_ROOT / "results" / "final_research"),
        help="Output directory (CSVs, plots, report/network_structure_time.md)",
    )
    ap.add_argument("--max-rebalances", type=int, default=None)
    ap.add_argument("--quick", action="store_true", help="Cap rebalances (~24) for smoke run")
    args = ap.parse_args()

    out = run_structure_time_analysis(
        _ROOT,
        Path(args.results_dir),
        max_rebalances=args.max_rebalances,
        quick=args.quick,
    )
    for name, df in out.items():
        print(f"{name}: {len(df)} rows")
    print(f"Wrote artifacts under {args.results_dir}")


if __name__ == "__main__":
    main()
