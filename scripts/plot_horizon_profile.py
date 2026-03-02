import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Load results (baseline or conditional)
df = pd.read_csv("data/leadlag_win_nonzeroy_horizon_results.csv")

h = df["h"]
beta = df["beta_y"]
se = df["se_y"]

ci_upper = beta + 1.96 * se
ci_lower = beta - 1.96 * se

plt.figure(figsize=(6,4))
plt.plot(h, beta, marker='o')
plt.fill_between(h, ci_lower, ci_upper, alpha=0.2)

plt.axhline(0, linestyle='--')
plt.xlabel("Horizon (days)")
plt.ylabel(r"$\hat{\beta}_h$")
plt.title("Supply-Chain Lead–Lag (Winsorized & Nonzero $y$)")

plt.tight_layout()
plt.savefig("figures/leadlag_win_nonzeroy_horizon_results.png", dpi=300)
plt.close()