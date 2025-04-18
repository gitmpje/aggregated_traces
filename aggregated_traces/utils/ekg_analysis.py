import logging

from collections import defaultdict
from networkx import MultiDiGraph, get_node_attributes
from networkx.algorithms.simple_paths import all_simple_edge_paths
from pandas import DataFrame
from pathlib import Path
from rdflib import Graph, Literal, Namespace, URIRef, Variable
from scipy.stats import entropy
from time import time
from typing import Dict, List, Optional, Tuple

logging.addLevelName(logging.INFO + 1, "INFO (timing)")
logger = logging.getLogger(__name__)

path_queries = Path(__file__).parent.parent.joinpath("sparql")

EKG = Namespace("http://example.org/def/ekg/aggregated_traces/")
EKG_ID = Namespace("http://example.org/id/ekg/aggregated_traces/")
EDGE_TYPE_MAPPING = {
    URIRef("urn:ekg:directlyFollows"): EKG.DirectlyFollows,
    URIRef("urn:ekg:directlyPrecedes"): EKG.DirectlyPrecedes,
}


def get_graph_trace_type(graph: MultiDiGraph, relation_type: URIRef) -> MultiDiGraph:
    return graph.edge_subgraph(
        [
            (u, v, key)
            for u, v, key, d in graph.edges(data=True, keys=True)
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


def get_edge_paths(graph: MultiDiGraph, source: URIRef, target: URIRef) -> List[tuple]:
    simple_edge_paths = all_simple_edge_paths(
        graph,
        source=source,
        target=target,
    )

    # Remove completely overlapping paths
    edge_paths = remove_subsets([p for p in simple_edge_paths])

    return edge_paths


def compute_trace_probabilities(
    rdf_trace_graph: Graph,
    nx_trace_graph: MultiDiGraph,
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
            target_query += f"VALUES ?entity_source {{ {' '.join(e.n3() for e in source_entities)} }}"
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

    # Raise exception if no nodes can be found in the backward trace that match the given constraints
    if not hasattr(query_result, "bindings"):
        raise RuntimeError(
            "No target nodes found for given source entities and constraints!"
        )
    logger.debug(query_result.serialize(format="txt").decode())

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

        edge_paths = get_edge_paths(
            graph=trace_graph_selected,
            source=b[Variable("node_source")],
            target=b[Variable("node_target")],
        )

        for edge_path in edge_paths:
            all_paths_edges.extend(edge_path)

            p_path = 1
            debug_labels = []
            for edge in edge_path:
                p_edge = trace_graph_selected.get_edge_data(*edge).get("fraction", 1)
                if p_edge != 1.0:
                    debug_labels.append(
                        trace_graph_selected.get_edge_data(*edge).get(
                            "amountEntityFraction"
                        )
                    )
                p_path *= p_edge

            logger.debug(
                " %s: path %s - probability %s (%s)"
                % (
                    b.get(Variable("entity_source")),
                    [
                        f"{edge[0].toPython().split('/')[-1]}-{edge[1].toPython().split('/')[-1]}"
                        for edge in edge_path
                    ],
                    p_path,
                    debug_labels,
                )
            )

            p += p_path

        devices_quality = b.get(Variable("devices_quality"), [])
        if devices_quality:
            devices_quality = devices_quality.split(",")
            devices_quality = [float(d.split("|")[-1]) for d in devices_quality]

        # TODO: dynamically map all variables returned by query to DataFrame
        records.append(
            {
                "entity_source": b.get(Variable("entity_source")),
                "node_source": b[Variable("node_source")],
                "entity_target": b.get(Variable("entity_target")),
                "node_target": b[Variable("node_target")],
                "product_model": b.get(Variable("product_model")),
                "probability": p,
                "nx_trace_graph-n_nodes": len(nx_trace_graph.nodes()),
                "nx_trace_graph-n_edges": len(nx_trace_graph.edges()),
                "trace_graph_selected-n_nodes": len(trace_graph_selected.nodes()),
                "trace_graph_selected-n_edges": len(trace_graph_selected.edges()),
                "n_simple_paths": len(edge_paths),
                "devices_quality": devices_quality,
            }
        )

    # Construct DataFrame
    df = DataFrame(records)

    logger.log(
        logging.INFO + 1, "compute_trace_probabilities: %.2f s", time() - start_time
    )

    return df, all_paths_edges


def compute_number_of_merges_in_trace_graph(
    trace_graph: MultiDiGraph,
    source: Optional[URIRef] = None,
    target: Optional[URIRef] = None,
    backward: Optional[bool] = True,
) -> int:
    """
    Returns the number of merges found in the provided graph.
    Optionally limited to the subgraph covering the paths between provided source and target node.
    A merge is a Aggregation node/event with more than one incoming directly follows relation.
    """
    start_time = time()
    if backward:
        trace_graph_selected = get_graph_trace_type(trace_graph, EKG.DirectlyPrecedes)
    else:
        trace_graph_selected = get_graph_trace_type(trace_graph, EKG.DirectlyFollows)

    if source and target:
        edge_paths = get_edge_paths(
            graph=trace_graph_selected, source=source, target=target
        )
        nodes = []
        for path in edge_paths:
            for edge in path:
                nodes.extend([edge[0], edge[1]])
        path_graph = trace_graph.subgraph(nodes=nodes)
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

    trace_graph_DF = get_graph_trace_type(trace_graph, EKG.DirectlyFollows)

    return len([n for n in aggregation_nodes if trace_graph_DF.in_degree(n) > 1])


def compute_aggregation_uniformity(graph: Graph) -> Dict[URIRef, List[float]]:
    """
    For all split and merge aggregation events, calculate the uniformity of the aggregation.

    Returns:
        List[float]: list with KL divergence for each split/merge event.
    """

    results = graph.query(
        """
        PREFIX : <http://example.org/def/ekg/aggregated_traces/>
        SELECT DISTINCT
            ?Aggregation
            (group_concat(?fraction) as ?fractions)
        WHERE {
            ?Aggregation a :Aggregation .
            {
                VALUES (?action ?RelationType ?p_direction) {
                    ("DELETE" :DirectlyFollows :source)
                    ("ADD" :DirectlyPrecedes :source)
                }

                ?Aggregation :action ?action .
                [] a ?RelationType ;
                    ?p_direction ?Aggregation ;
                    :fraction ?fraction .

                # Exclude packing events
                MINUS { ?Aggregation :bizStep "packing" }
            } UNION {
                # Separately include 'split' due to packing
                [] a :DirectlyFollows ;
                    :source ?Aggregation ;
                    :target/:bizStep "packing" ;
                    :fraction ?fraction .
            }
        }
        GROUP BY ?Aggregation ?RelationType
            """
    )

    kl_div = defaultdict(list)
    for binding in results.bindings:
        fractions = binding.get(Variable("fractions"))
        if not fractions:
            continue

        probabilities = [float(n) for n in fractions.split(" ")]

        kl_div[binding[Variable("Aggregation")]].append(
            entropy(pk=probabilities, qk=[1 / len(probabilities)] * len(probabilities))
        )

    return kl_div


def compute_aggregation_average_kl_trace_graph(
    trace_graph: MultiDiGraph,
    aggregation_kl: Dict[URIRef, List[float]],
    source: Optional[URIRef] = None,
    target: Optional[URIRef] = None,
    backward: Optional[bool] = True,
) -> float:
    """
    Computes the average KL divergence for all aggregations in the trace graph.
    Optionally limited to the subgraph covering the paths between provided source and target node.

    Args:
        trace_graph (MultiDiGraph): Networkx graph with traceability events.
        aggregation_kl (Dict[URIRef, List[float]]): dictionary with the KL divergence for the aggregation nodes (events).
        source (Optional[URIRef]): source node of the trace path(s) to include.
        target (Optional[URIRef]): source node of the trace path(s) to include.
        backward (Optional[bool]): whether to trace backward or forward.

    Returns:
        float: average KL divergence of all the aggregation nodes in the (selected) trace graph.
    """
    start_time = time()
    if backward:
        trace_graph_selected = get_graph_trace_type(trace_graph, EKG.DirectlyPrecedes)
    else:
        trace_graph_selected = get_graph_trace_type(trace_graph, EKG.DirectlyFollows)

    if source and target:
        edge_paths = get_edge_paths(
            graph=trace_graph_selected, source=source, target=target
        )
        nodes = []
        for path in edge_paths:
            for edge in path:
                nodes.extend([edge[0], edge[1]])
        path_graph = trace_graph.subgraph(nodes=nodes)
    else:
        path_graph = trace_graph

    aggregation_nodes = [
        n
        for n, v in get_node_attributes(path_graph, "types").items()
        if v == "http://example.org/def/ekg/aggregated_traces/Aggregation"
    ]

    logger.log(
        logging.INFO + 1,
        "compute_aggregation_kl_trace_graph: %.2f s",
        time() - start_time,
    )

    kls = []
    for n in aggregation_nodes:
        kls.extend(aggregation_kl[n])

    if not kls:
        return None
    else:
        return sum(kls) / len(kls)


def compute_number_of_steps_at_aggregations(
    graph: Graph, events: List[URIRef]
) -> Dict[URIRef, List[int]]:
    """
    Computes the number of production steps that have been executed at all aggregations that are in the backward trace for the provided set of events.
    The number of executed production steps is determined based on the product model related to the aggregation event.

    Args:
        graph (Graph): RDFlib graph with traceability events and DF/DP relations between events.
        events (List[URIRef]): list of event URIs for which to find on the backward trace paths.

    Returns:
        Dict[URIRef, List[int]]: dictionary with number of steps executed at the different aggregation events for each event URI.
    """

    results = graph.query(
        """
PREFIX : <http://example.org/def/ekg/aggregated_traces/>
SELECT ?event (group_concat(?productModel) as ?aggregation_productModels)
WHERE {
    {
        SELECT DISTINCT ?event ?aggregation
        WHERE {
            ?event :directlyPrecedes+ ?aggregation .
            ?aggregation a :Aggregation .
        }
    }
    ?aggregation :childQuantity/:class ?_productModel .
    ?_productModel a :Product .

    BIND ( replace(str(?_productModel), ".+/([a-zA-Z0-9_-]+)$", "$1") as ?productModel )
}
GROUP BY ?event
VALUES ?event {
    %(values)s
}
"""
        % {"values": "\n".join([event_node.n3() for event_node in events])}
    )

    return {
        b[Variable("event")]: [
            len(model.split("-")) for model in b.get(Variable("aggregation_productModels"), [])
        ]
        for b in results.bindings
    }
