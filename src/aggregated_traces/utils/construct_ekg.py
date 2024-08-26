from pathlib import Path
from networkx import DiGraph
from rdflib import Graph, Variable
from rdflib.plugins.sparql.processor import SPARQLResult
from typing import Dict

path_queries = Path(__file__).parent.parent.joinpath("sparql")

def insert_DF_DP(g: Graph) -> Graph:
    with open(path_queries.joinpath("insert_DF+DP_aggregated_entity.ru")) as f:
        g.update(f.read())
    return g

def check_quantities(g: Graph) -> SPARQLResult:
    with open(path_queries.joinpath("check_amount_in_vs_out.rq")) as f:
        r = g.query(f.read())

    if not all([bool(b[Variable("equal")].toPython()) for b in r.bindings]):
        raise Exception("Not all nodes have incoming amount equal to outgoing amount!")

    return r

def insert_fractions(g: Graph) -> Graph:
    with open(path_queries.joinpath("insert_quantity_fraction.ru")) as f:
        g.update(f.read())
    return g

def getAttributes(b: dict, t: str) -> Dict[str, str]:
    return {k.toPython().replace(f"?{t}_", ""): v.toPython() for k,v in b.items() if k.toPython().startswith(f"?{t}_")}

def generateNetworkxDiGraph(g: Graph) -> DiGraph:
    nx_graph = DiGraph()

    with open(path_queries.joinpath("select_nodes.rq")) as f:
        query_nodes = f.read()

    r = g.query(query_nodes)
    for b in r.bindings:
        nx_graph.add_node(b[Variable("node")], **getAttributes(b, "node"))


    with open(path_queries.joinpath("select_edges.rq")) as f:
        query_edges = f.read()

    r = g.query(query_edges)
    for b in r.bindings:
        nx_graph.add_edge(b[Variable("nodeSource")], b[Variable("nodeTarget")], **getAttributes(b, "edge"))

    return nx_graph