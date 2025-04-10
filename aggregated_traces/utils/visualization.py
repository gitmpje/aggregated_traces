# Possibly implement more interactive visualizations using
# Bokeh https://docs.bokeh.org/en/latest/docs/user_guide/topics/graph.html

import logging
import matplotlib.pyplot as plt
import networkx as nx

from typing import List
from time import time

logging.addLevelName(logging.INFO + 1, "INFO (timing)")
logger = logging.getLogger(__name__)

NODE_LABEL_KEY = "entitiesLocationTime"
EDGE_LABEL_KEY = "amountEntityFraction"

NODE_LINEWIDTH_DICT = {"packing": 2}
NODE_COLOR_DICT = {
    "http://example.org/def/ekg/aggregated_traces/Aggregation": "orange",
    "http://example.org/def/ekg/aggregated_traces/Transformation": "yellow",
    "other": "white",
}


def generate_graph_visualization(
    graph: nx.Graph,
    base_figure_path: str = None,
    edges_backward: List[tuple] = [],
    edges_forward: List[tuple] = [],
) -> plt.Figure:
    start_time = time()

    # Node visual attributes
    for node in graph.nodes(data=True):
        node_attributes = node[1]
        node_attributes_visual = {
            "style": "filled",
            "fillcolor": NODE_COLOR_DICT.get(node_attributes["types"], "white"),
            "tooltip": node_attributes[NODE_LABEL_KEY],
            "penwidth": NODE_LINEWIDTH_DICT.get(node_attributes["bizStep"], 1),
        }

        node_attributes.update(node_attributes_visual)

    # Edge visual attributes
    for edge in graph.edges(data=True):
        edge_attributes = edge[2]
        edge_attributes_visual = {
            "tooltip": edge_attributes[EDGE_LABEL_KEY],
            "color": "red"
            if edge[:2] in edges_backward
            else "orange"
            if edge[:2] in edges_forward
            else "black",
        }
        if (
            edge_attributes["type"]
            != "http://example.org/def/ekg/aggregated_traces/DirectlyFollows"
        ):
            edge_attributes_visual["constraint"] = False

        edge_attributes.update(edge_attributes_visual)

    # Conver to PyGraphviz AGraph
    agraph = nx.nx_agraph.to_agraph(graph)

    # Legend
    legend = agraph.add_subgraph(name="cluster_legend", label="Legend")
    k_prev = None
    for k, c in NODE_COLOR_DICT.items():
        legend.add_node(k, label=k, fillcolor=c, style="filled")

        # Add edge to place entries underneath each other
        if k_prev:
            legend.add_edge(k, k_prev, style="invis")
        k_prev = k

    if base_figure_path:
        if edges_backward or edges_forward:
            agraph.draw(f"{base_figure_path}_paths.svg", prog="dot")
        else:
            agraph.draw(f"{base_figure_path}.svg", prog="dot")

    logger.log(
        logging.INFO + 1, "compute_trace_probabilities: %.2f s", time() - start_time
    )

    return agraph
