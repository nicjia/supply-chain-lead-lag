from pathlib import Path

import numpy as np
import pandas as pd

from supply_chain_leadlag.research_pipeline import run_final_research_pipeline
from supply_chain_leadlag.stability import adjusted_rand_index, eigenspace_drift
from supply_chain_leadlag.events import add_event_flags, CONDITIONS


def test_stability_metrics():
    ari = adjusted_rand_index(
        pd.Series([0, 0, 1, 1]).to_numpy(),
        pd.Series([0, 0, 1, 1]).to_numpy(),
    )
    assert ari > 0.99
    import numpy as np

    V = np.random.randn(5, 2) + 1j * np.random.randn(5, 2)
    d = eigenspace_drift(V, V)
    assert d < 1e-10


def test_event_flags_columns():
    dates = pd.date_range("2020-01-01", periods=10, freq="B")
    R = pd.DataFrame({"000001": np.random.randn(10) * 0.01}, index=dates)
    edges = pd.DataFrame(
        {"customer_gvkey": ["000001"], "supplier_gvkey": ["000002"], "weight_wji": [0.5]}
    )
    flags = add_event_flags(pd.DataFrame({"date": dates}), None, R, edges, {})
    for c in CONDITIONS:
        assert c in flags.columns or c == "nonzero_exposure"


def test_quick_pipeline_writes_outputs(tmp_path):
    root = Path(__file__).resolve().parents[1]
    cfg = {
        "paths": {"output_dir": str(tmp_path / "final_research")},
        "strategies": {"max_rebalances": 2},
    }
    import yaml

    cfg_path = tmp_path / "research.yaml"
    with open(cfg_path, "w") as f:
        yaml.safe_dump(cfg, f)

    out = run_final_research_pipeline(cfg_path, max_rebalances=2, quick=True, repo_root=root)
    required = [
        "summary_metrics.csv",
        "panel_forward_reverse.csv",
        "strategy_family_comparison.csv",
        "cluster_method_comparison.csv",
        "hybrid_alpha_sweep.csv",
        "event_conditioned_panel.csv",
        "cluster_stability.csv",
        "report/final_report.md",
        "report/final_report.tex",
    ]
    for name in required:
        assert (out / name).is_file(), f"missing {name}"

    sweep = pd.read_csv(out / "hybrid_alpha_sweep.csv")
    assert len(sweep) >= 3
    assert sweep["alpha"].min() <= 0.01
    assert sweep["alpha"].max() >= 0.99
    summary = pd.read_csv(out / "summary_metrics.csv")
    families = set(summary["strategy_family"].astype(str))
    for fam in ("supplier_pressure", "globalrank", "metacluster", "clusterrank"):
        assert fam in families
