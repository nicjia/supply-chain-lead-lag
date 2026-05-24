#!/usr/bin/env python3
"""Regenerate final_report.md / .tex from existing results."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from supply_chain_leadlag.research_config import load_research_config
from supply_chain_leadlag.research_pipeline import _write_methods_comparison_plots, generate_final_report


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--results-dir", default="results/final_research")
    ap.add_argument("--config", default=str(_ROOT / "config" / "research.yaml"))
    args = ap.parse_args()
    out_dir = Path(args.results_dir)
    if not out_dir.is_absolute():
        out_dir = _ROOT / out_dir
    params = load_research_config(args.config)
    _write_methods_comparison_plots(out_dir)
    generate_final_report(out_dir, params)
    print(f"Deliverable: {out_dir / 'report' / 'final_report.md'}")
    print(f"Comparison:   {out_dir / 'plots' / 'methods_comparison.png'}")
    print(f"Quick index:  {out_dir / 'START_HERE.md'}")


if __name__ == "__main__":
    main()
