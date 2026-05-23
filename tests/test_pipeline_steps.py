import pytest

from supply_chain_leadlag.research_pipeline import resolve_pipeline_steps


def test_only_families_preset():
    steps = resolve_pipeline_steps(only="families")
    assert steps == {"load", "families", "summary", "plots", "report"}
    assert "panel" not in steps


def test_skip_panel():
    steps = resolve_pipeline_steps(skip_steps="panel,baselines,events")
    assert "panel" not in steps
    assert "families" in steps


def test_custom_steps():
    steps = resolve_pipeline_steps(steps="load,families,report")
    assert steps == {"load", "families", "report"}


def test_unknown_step_raises():
    with pytest.raises(ValueError, match="Unknown pipeline step"):
        resolve_pipeline_steps(steps="load,not_a_step")
