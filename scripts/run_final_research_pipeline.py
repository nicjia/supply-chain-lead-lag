#!/usr/bin/env python3
"""Run the full final research pipeline (see docs/IMPLEMENTATION_SPEC.md)."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from supply_chain_leadlag.research_pipeline import (
    ALL_PIPELINE_STEPS,
    PIPELINE_STEP_PRESETS,
    run_final_research_pipeline,
)


def main():
    ap = argparse.ArgumentParser(
        description="Final supply-chain lead-lag research pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Step names: {", ".join(sorted(ALL_PIPELINE_STEPS))}

Presets (--only):
  families  load + 4-family backtest + summary + minimal plots + report
  hybrid    load + hybrid alpha sweep + plots + report
  full      all steps (default)

Examples:
  # Re-run only the four strategy families (skip panel, baselines, sweeps)
  python scripts/run_final_research_pipeline.py --only families --max-rebalances 12

  # Custom subset
  python scripts/run_final_research_pipeline.py --steps load,families,plots,report

  # Full run but skip panel validation
  python scripts/run_final_research_pipeline.py --skip-steps panel,baselines,events,artifacts
""",
    )
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
    ap.add_argument(
        "--only",
        choices=sorted(PIPELINE_STEP_PRESETS.keys()),
        default=None,
        help="Run a preset bundle of steps (see epilog)",
    )
    ap.add_argument(
        "--steps",
        default=None,
        help="Comma-separated pipeline steps to run (overrides default full run)",
    )
    ap.add_argument(
        "--skip-steps",
        default=None,
        help="Comma-separated steps to exclude from the run",
    )
    ap.add_argument(
        "--plot-profile",
        choices=("minimal", "full"),
        default=None,
        help="minimal = dashboard + cumulative PnL only; full = all diagnostic plots",
    )
    args = ap.parse_args()

    out = run_final_research_pipeline(
        args.config,
        max_rebalances=args.max_rebalances,
        quick=args.quick or (args.max_rebalances is not None and args.max_rebalances <= 3),
        repo_root=_ROOT,
        verbose=args.verbose,
        only=args.only,
        steps=args.steps,
        skip_steps=args.skip_steps,
        plot_profile=args.plot_profile,
    )
    print(f"Wrote results to {out}")
    print(f"Start with: {out / 'START_HERE.md'}")


if __name__ == "__main__":
    main()
