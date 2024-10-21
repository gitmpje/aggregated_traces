import logging
import matplotlib.pyplot as plt
import networkx as nx

from matplotlib.lines import Line2D
from typing import List
from time import time

logging.addLevelName(logging.INFO+1, "INFO (timing)")
logger = logging.getLogger(__name__)


def generate_graph_visualization(
    graph: nx.Graph,
    base_figure_path: str = None,
    edges_backward: List[tuple] = [],
    edges_forward: List[tuple] = [],
) -> plt.Figure:

    # General settings
    font_size = 20
    node_size = 800
    arrowsize = node_size / 20
    arc_rad = 0.25
    edge_width = 1

    node_label_key = "entitiesLocationTime"
    edge_label_key = "amountEntityFraction"

    node_linewidth_dict = {"packing": 2}
    # edge_style_dict = {}
    node_color_dict = {
        "http://example.org/def/ekg/aggregated_traces/Aggregation": "orange",
        "http://example.org/def/ekg/aggregated_traces/Transformation": "yellow",
        "other": "white",
    }

    start_time = time()

    # Create figure
    plt.figure(figsize=(100, 50))

    # Create layout using graphviz (using edges in one direction to get a tree structure)
    edges_df = [
        (e_s, e_t)
        for e_s, e_t, t in graph.edges(data="type")
        if t == "http://example.org/def/ekg/aggregated_traces/DirectlyFollows"
    ]
    graph_df = nx.edge_subgraph(graph, edges_df)

    _pos = nx.drawing.nx_agraph.graphviz_layout(graph_df, prog="dot")

    # Add nodes
    node_colors = [
        node_color_dict.get(o, "white")
        for o in nx.get_node_attributes(graph, "types").values()
    ]
    node_linewidths = [
        node_linewidth_dict.get(v, 1)
        for v in nx.get_node_attributes(graph, "bizStep").values()
    ]
    fig = nx.draw_networkx_nodes(
        graph,
        pos=_pos,
        node_shape="s",
        node_size=node_size,
        node_color=node_colors,
        linewidths=node_linewidths,
        edgecolors=["black"] * len(graph.nodes()),
    )

    node_labels = {
        n: f"{d['label']}: {d[node_label_key]}" for n, d in graph.nodes(data=True)
    }
    fig = nx.draw_networkx_labels(
        graph,
        pos=_pos,
        labels=node_labels,
        verticalalignment="top",
        font_size=font_size,
    )

    # Add edges
    # edge_styles = [edge_style_dict[t] for t in nx.get_edge_attributes(graph, "type").values()]
    fig = nx.draw_networkx_edges(
        graph,
        pos=_pos,
        node_size=node_size,
        width=edge_width,
        arrowsize=arrowsize,
        connectionstyle=f"arc3, rad = {arc_rad}",
    )

    # Color edges on paths
    fig = nx.draw_networkx_edges(
        graph,
        pos=_pos,
        edgelist=edges_backward,
        node_size=node_size,
        edge_color="red",
        width=edge_width * 4,
        connectionstyle=f"arc3, rad = {arc_rad}",
    )
    fig = nx.draw_networkx_edges(
        graph,
        pos=_pos,
        edgelist=edges_forward,
        node_size=node_size,
        edge_color="orange",
        width=edge_width * 4,
        connectionstyle=f"arc3, rad = {arc_rad}",
    )

    edge_labels = dict()
    for u, v, d in graph.edges(data=True):
        if _pos[u][0] > _pos[v][0]:
            edge_labels[
                (
                    u,
                    v,
                )
            ] = f"{d[edge_label_key]}\n\n{graph.edges[(v,u)][edge_label_key]}"
        elif _pos[u][1] < _pos[v][1]:
            edge_labels[
                (
                    u,
                    v,
                )
            ] = f"{graph.edges[(v,u)][edge_label_key]}\n\n{d[edge_label_key]}"

    fig = nx.draw_networkx_edge_labels(
        graph,
        pos=_pos,
        edge_labels=edge_labels,
        font_size=font_size,
        # rotate=False
    )

    # Add legend
    legend_elements = {
        k: Line2D(
            [0],
            [0],
            marker="s",
            markersize=20,
            markeredgewidth=1,
            markeredgecolor="black",
            linewidth=0,
            color=c,
        )
        for k, c in node_color_dict.items()
    }
    if edges_backward or edges_forward:
        legend_elements.update(
            {
                "backward": Line2D(
                    [0],
                    [0],
                    linewidth=edge_width * 4,
                    color="red",
                ),
                "forward": Line2D(
                    [0],
                    [0],
                    linewidth=edge_width * 4,
                    color="orange",
                ),
            }
        )

    plt.legend(
        legend_elements.values(),
        legend_elements.keys(),
        loc="upper left",
        fontsize="medium",
        labelspacing=2,
    )

    plt.rcParams.update({"text.usetex": False, "svg.fonttype": "none"})

    if base_figure_path:
        if edges_backward or edges_forward:
            plt.savefig(
                f"{base_figure_path}_paths.svg",
            )
        else:
            plt.savefig(f"{base_figure_path}.svg")

    logger.log(logging.INFO+1, "compute_trace_probabilities: %.2f s", time() - start_time)

    return fig
