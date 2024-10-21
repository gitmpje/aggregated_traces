import logging

from networkx import DiGraph, get_node_attributes, path_graph
from networkx.algorithms.simple_paths import all_simple_paths
from pandas import DataFrame
from pathlib import Path
from rdflib import Graph, Literal, Namespace, URIRef, Variable
from time import time
from typing import List, Optional, Tuple

logging.addLevelName(logging.INFO + 1, "INFO (timing)")
logger = logging.getLogger(__name__)

path_queries = Path(__file__).parent.parent.joinpath("sparql")

EKG = Namespace("http://example.org/def/ekg/aggregated_traces/")
EKG_ID = Namespace("http://example.org/id/ekg/aggregated_traces/")
EDGE_TYPE_MAPPING = {
    URIRef("urn:ekg:directlyFollows"): EKG.DirectlyFollows,
    URIRef("urn:ekg:directlyPrecedes"): EKG.DirectlyPrecedes,
}


def get_graph_trace_type(graph: DiGraph, relation_type: URIRef) -> DiGraph:
    return graph.edge_subgraph(
        [
            (u, v)
            for u, v, d in graph.edges(data=True)
            if d["type"] == relation_type.toPython()
        ]
    )


def remove_subsets(lists: List[list]) -> List[list]:
    for l1 in lists:
        for l2 in lists:
            if l1 == l2:
                continue
            elif set(l1).issubset(set(l2)):
                lists.remove(l1)
    return lists


def get_paths(graph: DiGraph, source: URIRef, target: URIRef) -> list:
    simple_paths = all_simple_paths(
        graph,
        source=source,
        target=target,
    )

    # Remove completely overlapping paths
    paths = remove_subsets([p for p in simple_paths])

    return paths


def compute_trace_probabilities(
    rdf_trace_graph: Graph,
    nx_trace_graph: DiGraph,
    source_entities: List[URIRef] = [],
    source_entities_time: List[Tuple[URIRef, Tuple[Literal]]] = [],
    trace_backward: bool = True,
    custom_target_query: str = None,
) -> Tuple[DataFrame, List[Tuple[str]]]:
    """
    `custom_target_query`: should return at least variables `node_source`, `node_target`, and `g` [<urn:ekg:directlyFollows>, <urn:ekg:directlyPrecedes>]
    Either `source_entities` or `custom_target_query` is expected to specified for the function to work properly.
    """
    start_time = time()

    # Load SPARQL query
    if not custom_target_query:
        query_file = (
            "get_target_nodes_backward.rq"
            if trace_backward
            else "get_target_nodes_forward.rq"
        )
        with open(path_queries.joinpath(query_file)) as f:
            target_query = f.read()

        if trace_backward:
            target_query += f"VALUES ?entity_source {{ {' '.join(e.n3() for e in source_entities) } }}"
        else:
            if not source_entities_time:
                max_time = rdf_trace_graph.query(
                    "SELECT (max(?t) as ?max_time) { [] :timestamp ?t }"
                ).bindings[0][Variable("max_time")]
                source_entities_time = [
                    (e, (Literal(0), max_time)) for e in source_entities
                ]

            target_query += f"VALUES (?entity_source ?window_start ?window_end) {{ ({') ('.join([f'{e.n3()} {ws.n3()} {we.n3()}' for e, (ws, we) in source_entities_time])}) }}"
    else:
        target_query = custom_target_query

    logger.debug(target_query)

    # Run SPARQL query to retrieve source-target node pairs
    query_result = rdf_trace_graph.query(target_query)

    logger.debug(query_result.serialize(format="txt").decode())

    # Raise exception if no nodes can be found in the backward trace that match the given constraints
    if not query_result.bindings:
        raise RuntimeError(
            "No target nodes found for given source entities and constraints!"
        )

    if (Variable("flag_in_window") in query_result.vars) and not any(
        b.get(Variable("flag_in_window")) for b in query_result.bindings
    ):
        raise RuntimeError(
            "No target nodes found for given source entities and constraints!"
        )

    # Iterate over source-target pairs and compute path probability
    records = []
    all_paths_edges = []
    for b in query_result.bindings:
        # Skip when the source event is not in the provided time window
        if not b.get(Variable("flag_in_window"), Literal(True)).toPython():
            continue

        p = 0
        trace_graph_selected = get_graph_trace_type(
            nx_trace_graph, EDGE_TYPE_MAPPING[b[Variable("g")]]
        )

        paths = get_paths(
            graph=trace_graph_selected,
            source=b[Variable("node_source")],
            target=b[Variable("node_target")],
        )

        for path in paths:
            path_graph_ = path_graph(path)
            all_paths_edges.extend(path_graph_.edges())

            p_path = 1
            for edge in path_graph_.edges():
                # if trace_graph.get_edge_data(*edge).get("fraction", 1) != 1:
                #     print(edge[0], nx_graph.get_edge_data(*edge).get("fraction", 1))
                # print(edge, trace_graph_selected.get_edge_data(*edge).get(edge_label_key))
                p_path *= trace_graph_selected.get_edge_data(*edge).get("fraction", 1)

            logger.debug(
                " %s: path %s - probability %s"
                % (
                    b.get(Variable("entity_source")),
                    [n.toPython().split("/")[-1] for n in path],
                    p_path,
                )
            )

            p += p_path

        # TODO: dynamically map all variables returned by query to DataFrame
        records.append(
            {
                "entity_source": b.get(Variable("entity_source")),
                "node_source": b[Variable("node_source")],
                "entity_target": b.get(Variable("entity_target")),
                "node_target": b[Variable("node_target")],
                "product_model": b.get(Variable("product_model")),
                "probability": p,
            }
        )

    # Construct DataFrame
    df = DataFrame(records)

    logger.log(
        logging.INFO + 1, "compute_trace_probabilities: %.2f s", time() - start_time
    )

    return df, all_paths_edges


def compute_number_of_merges_in_trace_graph(
    trace_graph: DiGraph,
    source: Optional[URIRef] = None,
    target: Optional[URIRef] = None,
    backward: Optional[bool] = True,
) -> int:
    """
    Returns the number of merges found in the provided graph.
    A merge is a Aggregation node/event with more than one incoming directly follows relation.
    """
    start_time = time()
    if backward:
        trace_graph_selected = get_graph_trace_type(trace_graph, EKG.DirectlyPrecedes)
    else:
        trace_graph_selected = get_graph_trace_type(trace_graph, EKG.DirectlyFollows)

    if source and target:
        paths = get_paths(graph=trace_graph_selected, source=source, target=target)
        path_graph = trace_graph.subgraph(
            nodes=[node for path in paths for node in path]
        )
    else:
        path_graph = trace_graph

    aggregation_nodes = [
        n
        for n, v in get_node_attributes(path_graph, "types").items()
        if v == "http://example.org/def/ekg/aggregated_traces/Aggregation"
    ]

    logger.log(
        logging.INFO + 1,
        "compute_number_of_merges_in_trace_graph: %.2f s",
        time() - start_time,
    )

    trace_graph_DF = get_graph_trace_type(path_graph, EKG.DirectlyFollows)
    return len([n for n in aggregation_nodes if trace_graph_DF.in_degree(n) > 1])
