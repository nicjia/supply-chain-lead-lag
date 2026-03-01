# supply-chain-lead-lag
Quantitative strategy exploiting lead-lag return predictability across supply chain networks. Uses Point-in-Time Compustat data to construct dynamic directed graphs, testing customer momentum via Granger causality and eigenvector centrality. Leverages network topology analysis to isolate structural alpha from information diffusion in central hubs.

## Layout
- **`data/`** — Input CSVs: `gqqatnce2nuvm7hl.csv` (segment/customer sales), `hwihvjjrzgyby1vf.csv` (company-period header).
- **`eda/`** — EDA scripts, reports, and outputs. Start with **`eda/EDA_SUMMARY.md`** for a one-page summary; full reports: `EDA_REPORT.md`, `EDA_CUSTOMER_MATCHING_REPORT.md`, `EDA_SRCDATE_REPORT.md`, `EDA_CSV2_REPORT.md`. Run scripts from repo root or from `eda/` (they read from `data/`).
