from pathlib import Path

import pytest

from supply_chain_leadlag.yaml_config import (
    flat_backtest_run_params,
    grid_search_bundle,
    load_yaml_config,
)


def test_flat_defaults_empty_config():
    f = flat_backtest_run_params({})
    assert f["score"] == "tstat_diff"
    assert f["rank_method"] == "leadingness"
    assert f["compare_baselines"] is True
    assert f["hybrid_alpha"] is None
    assert f["commission_bps"] == 0.0
    assert f["beta_neutralize"] is False
    assert f["sector_neutralize"] is False


def test_load_repo_config_if_present():
    root = Path(__file__).resolve().parents[1]
    p = root / "config" / "backtest.yaml"
    if not p.is_file():
        pytest.skip("config/backtest.yaml not in repo")
    cfg = load_yaml_config(p)
    assert "paths" in cfg
    b = grid_search_bundle(cfg)
    assert "lookback_rows" in b
