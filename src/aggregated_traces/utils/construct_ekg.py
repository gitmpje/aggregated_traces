from pathlib import Path
from rdflib import Graph

path_queries = Path(__file__).parent.parent.joinpath("sparql")

def insert_DF_DP(g: Graph):
    with open(path_queries.joinpath("insert_DF+DP_aggregated_entity.ru")) as f:
        g.update(f.read())
    return g

def check_quantities(g: Graph):
    with open(path_queries.joinpath("check_amount_in_vs_out.rq")) as f:
        r = g.query(f.read())
        print(r.serialize(format="txt").decode())

def insert_fractions(g: Graph):
    with open(path_queries.joinpath("insert_quantity_fraction.ru")) as f:
        g.update(f.read())
    return g