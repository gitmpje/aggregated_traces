import logging

from pathlib import Path
from networkx import MultiDiGraph
from rdflib import ConjunctiveGraph, Graph, Variable
from rdflib.plugins.sparql.processor import SPARQLResult
from time import time
from typing import Dict

path_queries = Path(__file__).parent.parent.joinpath("sparql")

logging.addLevelName(logging.INFO+1, "INFO (timing)")
logger = logging.getLogger(__name__)


def load_rdf_graph(file: str|Path) -> ConjunctiveGraph:
    start_time = time()

    graph = ConjunctiveGraph()
    graph.parse(file)

    logger.log(logging.INFO+1, "load_rdf_graph: %.2f s", time() - start_time)

    return graph


def insert_DF_DP(g: Graph) -> Graph:
    start_time = time()
    with open(path_queries.joinpath("insert_DF+DP_aggregated_entity.ru")) as f:
        g.update(f.read())
    logger.log(logging.INFO+1, "Insert_DF_DP - direct relation: %.2f s", time() - start_time)

    start_time = time()
    with open(path_queries.joinpath("insert_DF+DP_qualified_relation.ru")) as f:
        g.update(f.read())
    logger.log(logging.INFO+1, "Insert_DF_DP - qualified relation: %.2f s", time() - start_time)

    return g


def check_quantities(g: Graph) -> SPARQLResult:
    start_time = time()
    with open(path_queries.joinpath("check_amount_in_vs_out.rq")) as f:
        r = g.query(f.read())

    if not all([b[Variable("equal")].toPython() for b in r.bindings]):
        logging.warning("Not all nodes have incoming amount equal to outgoing amount!")

    logger.log(logging.INFO+1, "check_quantities: %.2f s", time() - start_time)

    return r


def insert_fractions(g: Graph) -> Graph:
    start_time = time()
    with open(path_queries.joinpath("insert_quantity_fraction.ru")) as f:
        g.update(f.read())
    logger.log(logging.INFO+1, "insert_fractions: %.2f s", time() - start_time)

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
    logger.log(logging.INFO+1, "generate_networkx_di_graph: %.2f s", time() - start_time)

    return nx_graph
