import logging

from pathlib import Path
from networkx import MultiDiGraph
from rdflib import Dataset, Graph, Variable
from rdflib.plugins.sparql.processor import SPARQLResult
from time import time
from typing import Dict, Optional

path_queries = Path(__file__).parent.parent.joinpath("sparql")

logging.addLevelName(logging.INFO + 1, "INFO (timing)")
logger = logging.getLogger(__name__)


RELATION_GRAPH_ID = [
    (
        "DF",
        "http://example.org/def/ekg/aggregated_traces/DirectlyFollows_AggregatedEntity",
        "urn:ekg:directlyFollows",
    ),
    (
        "DP",
        "http://example.org/def/ekg/aggregated_traces/DirectlyPrecedes_AggregatedEntity",
        "urn:ekg:directlyPrecedes",
    ),
]


def load_rdf_graph(file: str | Path, store: Optional[str] = "Memory") -> Dataset:
    start_time = time()

    graph = Dataset(default_union=True, store=store)
    graph.parse(file)

    logger.log(logging.INFO + 1, "load_rdf_graph: %.2f s", time() - start_time)

    return graph


def insert_DF_DP(g: Graph) -> Graph:
    start_time = time()

    # For Oxigraph construct graphs instead of update, as updates are not handled by pyoxigraph (see README on https://github.com/oxigraph/oxrdflib)
    if "Oxigraph" in str(g.store):
        with open(path_queries.joinpath("DF+DP_construct_temp.rq")) as f:
            g_temp = g.query(f.read())
        g += g_temp

        # Direct relations
        for relation_name, relation, graph_id in RELATION_GRAPH_ID:
            with open(
                path_queries.joinpath(f"DF+DP_construct_{relation_name}.rq")
            ) as f:
                r = g.query(f.read())
            g_r = g.graph(identifier=graph_id)
            g_r.parse(data=r.graph.serialize())

        logger.log(
            logging.INFO + 1,
            "Insert_DF_DP - direct relation: %.2f s",
            time() - start_time,
        )

        # Qualified relations
        for relation_name, relation, graph_id in RELATION_GRAPH_ID:
            with open(
                path_queries.joinpath(
                    f"DF+DP_construct_{relation_name}_qualified_relation.rq"
                )
            ) as f:
                r = g.query(f.read())
            g_r = g.graph(identifier=graph_id)
            g_r.parse(data=r.graph.serialize())

        logger.log(
            logging.INFO + 1,
            "Insert_DF_DP - qualified relation: %.2f s",
            time() - start_time,
        )
    else:
        with open(path_queries.joinpath("insert_DF+DP_aggregated_entity.ru")) as f:
            g.update(f.read())
        logger.log(
            logging.INFO + 1,
            "Insert_DF_DP - direct relation: %.2f s",
            time() - start_time,
        )

        start_time = time()
        with open(path_queries.joinpath("insert_DF+DP_qualified_relation.ru")) as f:
            g.update(f.read())
        logger.log(
            logging.INFO + 1,
            "Insert_DF_DP - qualified relation: %.2f s",
            time() - start_time,
        )

    return g


def check_quantities(g: Graph) -> SPARQLResult:
    start_time = time()
    with open(path_queries.joinpath("check_amount_in_vs_out.rq")) as f:
        r = g.query(f.read())

    if not all([b[Variable("equal")].toPython() for b in r.bindings]):
        logging.warning("Not all nodes have incoming amount equal to outgoing amount!")

    logger.log(logging.INFO + 1, "check_quantities: %.2f s", time() - start_time)

    return r


def insert_fractions(g: Graph) -> Graph:
    start_time = time()

    # For Oxigraph construct graphs instead of update, as updates are not handled by pyoxigraph (see README on https://github.com/oxigraph/oxrdflib)
    if "Oxigraph" in str(g.store):
        # Relation amount out
        with open(
            path_queries.joinpath("quantity_fraction_construct_amount_out.rq")
        ) as f:
            r = g.query(f.read())
        g_r = g.graph(identifier="urn:ekg:quantity_fraction_construct_amount_out")
        g_r.parse(data=r.graph.serialize())

        # Event sum amount out
        with open(
            path_queries.joinpath("quantity_fraction_construct_sum_amount_out.rq")
        ) as f:
            r = g.query(f.read())
        g_r = g.graph(identifier="urn:ekg:quantity_fraction_construct_sum_amount_out")
        g_r.parse(data=r.graph.serialize())

        # Fraction (per relation type)
        for relation_name, relation, graph_id in RELATION_GRAPH_ID:
            with open(
                path_queries.joinpath("quantity_fraction_construct_fraction.rq")
            ) as f:
                r = g.query(f.read().replace("?type", f"<{relation}>"))
            g_r = g.graph(identifier=graph_id)
            g_r.parse(data=r.graph.serialize())

    else:
        with open(path_queries.joinpath("insert_quantity_fraction.ru")) as f:
            g.update(f.read())

    logger.log(logging.INFO + 1, "insert_fractions: %.2f s", time() - start_time)

    return g


def get_attributes(b: dict, t: str) -> Dict[str, str]:
    return {
        k.toPython().replace(f"?{t}_", ""): v.toPython()
        for k, v in b.items()
        if k.toPython().startswith(f"?{t}_")
    }


def generate_networkx_di_graph(g: Graph) -> MultiDiGraph:
    start_time = time()
    nx_graph = MultiDiGraph()

    with open(path_queries.joinpath("select_nodes.rq")) as f:
        query_nodes = f.read()

    r = g.query(query_nodes)
    for b in r.bindings:
        nx_graph.add_node(b[Variable("node")], **get_attributes(b, "node"))

    with open(path_queries.joinpath("select_edges.rq")) as f:
        query_edges = f.read()

    r = g.query(query_edges)
    for b in r.bindings:
        nx_graph.add_edge(
            u_for_edge=b[Variable("nodeSource")],
            v_for_edge=b[Variable("nodeTarget")],
            key=b[Variable("Relation")],
            **get_attributes(b, "edge"),
        )
    logger.log(
        logging.INFO + 1, "generate_networkx_di_graph: %.2f s", time() - start_time
    )

    return nx_graph
