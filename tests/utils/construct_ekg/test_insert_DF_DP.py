import os
import pytest

from rdflib import ConjunctiveGraph
from rdflib.compare import to_isomorphic

from aggregated_traces.utils.construct_ekg import insert_DF_DP

DATA_PATH = os.path.join(os.getcwd(), "tests", "data")

test_graph_files = [
    ("ekg1_in.ttl", "ekg1_DF_DP.ttl"),  # Object and Aggregation events
    ("ekg2_in.ttl", "ekg2_DF_DP.ttl"),  # Object, Aggregation, and Transformation events
]


@pytest.mark.parametrize("ekg_in_file,ekg_df_dp_file", test_graph_files)
class TestInsertDFDP:
    def test_insert_DF_DP(self, ekg_in_file, ekg_df_dp_file):
        ekg1_in = ConjunctiveGraph()
        ekg1_in.parse(os.path.join(DATA_PATH, ekg_in_file))
        ekg_df_dp = ConjunctiveGraph()
        ekg_df_dp.parse(os.path.join(DATA_PATH, ekg_df_dp_file))

        g_out = insert_DF_DP(ekg1_in)

        assert to_isomorphic(g_out) == to_isomorphic(ekg_df_dp)
