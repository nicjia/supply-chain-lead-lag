import pandas as pd

from supply_chain_leadlag.classification_data import (
    build_firm_classification_map,
    clean_wrds_classification,
    classification_column_for_method,
)


def test_clean_dedupes_to_one_row_per_gvkey():
    raw = pd.DataFrame(
        {
            "gvkey": ["13", "13", "45"],
            "datadate": ["2000-10-31", "2001-10-31", "2000-12-31"],
            "gsector": [45, 45, 20],
            "gind": [452010, 452010, 203020],
            "naics": [334210, 334210, 481111],
            "sic": [3661, 3661, 4512],
        }
    )
    clean = clean_wrds_classification(raw)
    assert len(clean) == 2
    assert clean.loc[clean.gvkey == "000013", "datadate"].iloc[0] == pd.Timestamp("2001-10-31")


def test_build_firm_map_derived_naics_sic():
    clean = pd.DataFrame(
        {
            "gvkey": ["000001"],
            "gsector": ["45"],
            "ggroup": ["4520"],
            "gind": ["452010"],
            "gsubind": ["45201020"],
            "naics": ["334210"],
            "sic": ["3661"],
        }
    )
    m = build_firm_classification_map(clean)
    assert m.loc[0, "naics2"] == "33"
    assert m.loc[0, "naics3"] == "334"
    assert m.loc[0, "sic2"] == "36"
    assert m.loc[0, "sic4"] == "3661"


def test_classification_method_columns():
    assert classification_column_for_method("sector") == "gsector"
    assert classification_column_for_method("industry") == "gind"
    assert classification_column_for_method("naics2") == "naics2"
