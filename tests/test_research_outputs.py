import numpy as np
import pandas as pd

from supply_chain_leadlag.research_outputs import (
    drawdown_series,
    factor_exposure_alpha,
    spectral_summary_from_C,
)


def test_drawdown_and_factor():
    dr = pd.Series([0.01, -0.02, 0.015, -0.01], index=pd.date_range("2020-01-01", periods=4, freq="B"))
    dd = drawdown_series(dr)
    assert dd.min() <= 0
    daily = pd.DataFrame({"strat": dr})
    fa = factor_exposure_alpha(daily, dr * 0.5)
    assert "alpha_annual" in fa.columns


def test_spectral_summary():
    C = pd.DataFrame([[0, 1], [0.2, 0]], index=["a", "b"], columns=["a", "b"])
    summary, top = spectral_summary_from_C(C, n_perm=20, seed=0)
    assert "lambda_max_H" in summary.columns
    assert len(top) >= 1
