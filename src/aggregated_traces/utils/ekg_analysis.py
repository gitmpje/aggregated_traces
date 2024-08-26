from networkx import DiGraph, path_graph
from networkx.algorithms.simple_paths import all_simple_paths
from pandas import DataFrame
from pathlib import Path
from rdflib import Graph, Namespace, URIRef, Variable
from typing import List, Tuple

path_queries = Path(__file__).parent.parent.joinpath("sparql")

EKG_ID = Namespace("http://example.org/id/ekg/aggregated_traces/")
EDGE_TYPE_MAPPING = {
    "urn:ekg:directlyFollows": "http://example.org/def/ekg/aggregated_traces/DirectlyFollows",
    "urn:ekg:directlyPrecedes": "http://example.org/def/ekg/aggregated_traces/DirectlyPrecedes" 
}


def remove_subsets(lists: List[list]) -> List[list]:
    for l1 in lists:
        for l2 in lists:
            if l1 == l2:
                continue
            elif set(l1).issubset(set(l2)):
                lists.remove(l1)
    return lists

def compute_trace_probabilities(rdf_trace_graph: Graph, nx_trace_graph: DiGraph, source_entities: List[str]=[], trace_backward: bool=True, custom_target_query: str=None) -> Tuple[DataFrame, List[Tuple[str]]]:
    """
    `custom_target_query`: should return at least variables `node_source`, `node_target`, and `g` [<urn:ekg:directlyFollows>, <urn:ekg:directlyPrecedes>]
    Either `source_entities` or `custom_target_query` is expected to specified for the function to work properly.
    """

    # Load SPARQL query
    if not custom_target_query:
        query_file = "get_target_nodes_backward.rq" if trace_backward else "get_target_nodes_forward.rq"
        with open(path_queries.joinpath(query_file)) as f:
            target_query = f.read()

        if trace_backward:
            target_query += f"VALUES ?entity_source {{ {' '.join(e.n3() for e in source_entities) } }}"
        else:
            target_query += f"VALUES (?entity_source ?window_start ?window_end) {{ ({' '.join([' '.join([v.n3() for v in r]) for r in source_entities])}) }}"
    else:
        target_query = custom_target_query

    # Run SPARQL query to retrieve source-target node pairs
    query_result = rdf_trace_graph.query(target_query)

    # Iterate over source-target pairs and compute path probability
    records = []
    all_paths_edges = []
    for b in query_result.bindings:
        p = 0
        trace_graph_selected = nx_trace_graph.edge_subgraph([(u,v) for u,v,d in nx_trace_graph.edges(data=True) if d["type"]==EDGE_TYPE_MAPPING[b[Variable("g")].toPython()]])

        # Remove completely overlapping paths
        paths = remove_subsets([p for p in all_simple_paths(trace_graph_selected, source=b[Variable("node_source")], target=b[Variable("node_target")])])
        for path in paths:
            # print([n.toPython().split("/")[-1] for n in path])
            path_graph_ = path_graph(path)
            all_paths_edges.extend(path_graph_.edges())

            p_path = 1
            for edge in path_graph_.edges():
                # if trace_graph.get_edge_data(*edge).get("fraction", 1) != 1:
                #     print(edge[0], nx_graph.get_edge_data(*edge).get("fraction", 1))
                # print(edge, trace_graph_selected.get_edge_data(*edge).get(edge_label_key))
                p_path *= trace_graph_selected.get_edge_data(*edge).get("fraction", 1)

            p += p_path

        # TODO: dynamically map all variables returned by query to DataFrame
        records.append(
            {
                "entity_source": b.get(Variable("entity_source")),
                "node_source": b[Variable("node_source")],
                "entity_target": b.get(Variable("entity_target")),
                "node_target": b[Variable("node_target")],
                "validation_fraction": b.get(Variable("validation_fraction")).toPython(),
                "probability": p
            })

    # Construct DataFrame
    df = DataFrame(records)

    # Replace URIs with prefix
    for c in df.columns:
        if type(df[c].iloc[0]) == URIRef:
            df[c] = df[c].str.replace(EKG_ID, "ekg_id:")

    return df, all_paths_edges
