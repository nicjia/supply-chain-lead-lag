from supply_chain_leadlag.research_pipeline import cluster_method_for_family


def test_cluster_method_for_family_sweep_winners():
    params = {
        "default_cluster_method": "signed",
        "family_cluster_methods": {
            "metacluster": "sector",
            "clusterrank": "signed",
        },
    }
    assert cluster_method_for_family("metacluster", params) == "sector"
    assert cluster_method_for_family("clusterrank", params) == "signed"
    assert cluster_method_for_family("supplier_pressure", params) == "signed"
