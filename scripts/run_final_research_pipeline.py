#!/usr/bin/env python3
"""Run the full final research pipeline (see docs/IMPLEMENTATION_SPEC.md)."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from supply_chain_leadlag.research_pipeline import run_final_research_pipeline


def main():
    ap = argparse.ArgumentParser(description="Final supply-chain lead-lag research pipeline")
    ap.add_argument("--config", default=str(_ROOT / "config" / "research.yaml"))
    ap.add_argument("--max-rebalances", type=int, default=None)
    ap.add_argument(
        "--quick",
        action="store_true",
        help="Smoke run (fewer rebalances, trimmed sweeps, no baseline backtest)",
    )
    ap.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="DEBUG logging (includes each rebalance date inside rolling loops)",
    )
    args = ap.parse_args()

    out = run_final_research_pipeline(
        args.config,
        max_rebalances=args.max_rebalances,
        quick=args.quick or (args.max_rebalances is not None and args.max_rebalances <= 3),
        repo_root=_ROOT,
        verbose=args.verbose,
    )
    print(f"Wrote results to {out}")


if __name__ == "__main__":
    main()
