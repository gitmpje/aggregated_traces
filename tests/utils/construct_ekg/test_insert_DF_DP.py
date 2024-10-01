import os
import pytest

from rdflib import ConjunctiveGraph
from rdflib.compare import to_isomorphic

from aggregated_traces.utils.construct_ekg import insert_DF_DP

DATA_PATH = os.path.join(os.getcwd(), "tests", "data")

test_graph_files = [
    ("event_combinations/object_aggregation_ADD-log.ttl", "event_combinations/object_aggregation_ADD-DF_DP.ttl"),
    ("event_combinations/object_aggregation_DELETE-log.ttl", "event_combinations/object_aggregation_DELETE-DF_DP.ttl"),
    ("event_combinations/object_object-log.ttl", "event_combinations/object_object-DF_DP.ttl"),
    ("event_combinations/object_transformation-log.ttl", "event_combinations/object_transformation-DF_DP.ttl"),

    ("event_combinations/aggregation_ADD_aggregation_ADD-log.ttl", "event_combinations/aggregation_ADD_aggregation_ADD-DF_DP.ttl"),
    ("event_combinations/aggregation_ADD_aggregation_DELETE-log.ttl", "event_combinations/aggregation_ADD_aggregation_DELETE-DF_DP.ttl"),
    ("event_combinations/aggregation_ADD_object-log.ttl", "event_combinations/aggregation_ADD_object-DF_DP.ttl"),
    ("event_combinations/aggregation_ADD_transformation-log.ttl", "event_combinations/aggregation_ADD_transformation-DF_DP.ttl"),

    ("event_combinations/aggregation_DELETE_aggregation_ADD-log.ttl", "event_combinations/aggregation_DELETE_aggregation_ADD-DF_DP.ttl"),
    ("event_combinations/aggregation_DELETE_aggregation_DELETE-log.ttl", "event_combinations/aggregation_DELETE_aggregation_DELETE-DF_DP.ttl"),
    ("event_combinations/aggregation_DELETE_object-log.ttl", "event_combinations/aggregation_DELETE_object-DF_DP.ttl"),
    ("event_combinations/aggregation_DELETE_transformation-log.ttl", "event_combinations/aggregation_DELETE_transformation-DF_DP.ttl"),

    ("event_combinations/transformation_aggregation_ADD-log.ttl", "event_combinations/transformation_aggregation_ADD-DF_DP.ttl"),
    ("event_combinations/transformation_aggregation_DELETE-log.ttl", "event_combinations/transformation_aggregation_DELETE-DF_DP.ttl"),
    ("event_combinations/transformation_object-log.ttl", "event_combinations/transformation_object-DF_DP.ttl"),
    ("event_combinations/transformation_transformation-log.ttl", "event_combinations/transformation_transformation-DF_DP.ttl"),

    ("complete_trace/split_pack-log.ttl", "complete_trace/split_pack-DF_DP.ttl"),  # Object and Aggregation events
    ("complete_trace/product-log.ttl", "complete_trace/product-DF_DP.ttl"),  # Object and Aggregation events + Product
    ("complete_trace/material-log.ttl", "complete_trace/material-DF_DP.ttl"),  # Object, Aggregation, and Transformation events
]


@pytest.mark.parametrize("log_file,df_dp_file", test_graph_files)
class TestInsertDFDP:
    def test_insert_DF_DP(self, log_file, df_dp_file):
        g_log = ConjunctiveGraph()
        g_log.parse(os.path.join(DATA_PATH, log_file))
        ekg_df_dp = ConjunctiveGraph()
        ekg_df_dp.parse(os.path.join(DATA_PATH, df_dp_file))

        g_out = insert_DF_DP(g_log)

        if to_isomorphic(g_out) != to_isomorphic(ekg_df_dp):
            print("Expected - Output:\n", (ekg_df_dp-g_out).serialize())
            print("Output - Expected:\n", (g_out-ekg_df_dp).serialize())

        assert to_isomorphic(g_out) == to_isomorphic(ekg_df_dp)
