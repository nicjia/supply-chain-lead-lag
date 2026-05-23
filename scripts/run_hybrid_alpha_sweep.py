#!/usr/bin/env python3
"""Hybrid alpha sweep only (see docs/IMPLEMENTATION_SPEC.md §8)."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from supply_chain_leadlag.research_pipeline import run_hybrid_alpha_sweep


def main():
    ap = argparse.ArgumentParser(description="Run hybrid alpha sweep from config/research.yaml")
    ap.add_argument("--config", default=str(_ROOT / "config" / "research.yaml"))
    ap.add_argument("--max-rebalances", type=int, default=None)
    ap.add_argument("--quick", action="store_true")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    out = run_hybrid_alpha_sweep(
        args.config,
        max_rebalances=args.max_rebalances,
        quick=args.quick,
        repo_root=_ROOT,
        verbose=args.verbose,
    )
    print(f"Wrote {out}")


if __name__ == "__main__":
    main()
