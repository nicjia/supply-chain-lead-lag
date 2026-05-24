import pandas as pd

from supply_chain_leadlag.research_pipeline import _write_hybrid_alpha_plots


def test_hybrid_cumulative_pnl_plot(tmp_path):
    rows = [
        {"alpha": 0.0, "strategy_family": "supplier_pressure", "sharpe": 0.5},
        {"alpha": 1.0, "strategy_family": "supplier_pressure", "sharpe": 0.4},
    ]
    dates = pd.date_range("2020-01-01", periods=10, freq="B")
    daily = pd.DataFrame(
        {
            "date": list(dates) * 2,
            "daily_return": [0.001] * 10 + [0.002] * 10,
            "alpha": [0.0] * 10 + [1.0] * 10,
            "strategy_family": ["supplier_pressure"] * 20,
        }
    )
    (tmp_path / "plots").mkdir()
    _write_hybrid_alpha_plots(tmp_path, rows, daily)
    assert (tmp_path / "plots" / "hybrid_alpha_sweep.png").is_file()
    assert (tmp_path / "plots" / "hybrid_alpha_sweep_cumulative_pnl.png").is_file()
