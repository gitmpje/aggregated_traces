import os

from rdflib import ConjunctiveGraph
from rdflib.compare import to_isomorphic

from aggregated_traces.utils.construct_ekg import insert_DF_DP

data_path = os.path.join(os.getcwd(), "tests", "data")

EKG1_IN = ConjunctiveGraph()
EKG1_IN.parse(os.path.join(data_path, "ekg1_in.ttl"))
EKG1_DF_DP = ConjunctiveGraph()
EKG1_DF_DP.parse(os.path.join(data_path, "ekg1_DF_DP.ttl"))

class TestInsertDFDP():

    def test_ekg1(_):
        g_out = insert_DF_DP(EKG1_IN)
        g_out.serialize("test.ttl")

        assert to_isomorphic(g_out) == to_isomorphic(EKG1_DF_DP)
