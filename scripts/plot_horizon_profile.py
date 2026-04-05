"""Plot horizon profile from run_leadlag_tests CSV output."""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", type=str, required=True, help="CSV with columns h, beta, se (or beta_y, se_y)")
    ap.add_argument("--out", type=str, default="figures/leadlag_horizon.png")
    args = ap.parse_args()

    df = pd.read_csv(args.csv)
    if "beta_y" in df.columns:
        h, beta, se = df["h"], df["beta_y"], df["se_y"]
    else:
        h, beta, se = df["h"], df["beta"], df["se"]

    ci_upper = beta + 1.96 * se
    ci_lower = beta - 1.96 * se

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)

    plt.figure(figsize=(6, 4))
    plt.plot(h, beta, marker="o")
    plt.fill_between(h, ci_lower, ci_upper, alpha=0.2)
    plt.axhline(0, linestyle="--")
    plt.xlabel("Horizon (days)")
    plt.ylabel(r"$\hat{\beta}_h$")
    plt.title("Lead–lag horizon profile")
    plt.tight_layout()
    plt.savefig(out, dpi=300)
    plt.close()
    print(f"[saved] {out}")


if __name__ == "__main__":
    main()
