from ast import operator
from typing import Set, List, Dict, Optional
import enum
import re
import networkx

from jobby.schemas import Model

import matplotlib.pyplot as plt


class Operator(enum.Enum):

    upstream = "upstream"
    downstream = "downstream"
    wildcard = "wildcard"


def prntPath(lst, node, df, lst_vst):
    for val in df.values:
        if val[0] == node:
            lst.append(val[1])
            prntPath(lst, val[1], df, lst_vst)
    if not lst[-1] in lst_vst:
        print("-".join(lst))
    for l in lst:
        lst_vst.add(l)
    lst.pop()
    return


class DAG:
    def __init__(
        self, graph: networkx.DiGraph, node_mapping: Optional[Dict[str, str]] = None
    ) -> None:
        self.graph: networkx.DiGraph = graph

        if node_mapping is None:
            node_mapping = {node: node for node in graph.nodes}

        self.node_mapping = node_mapping
        self.model_mapping = {value: key for key, value in node_mapping.items()}

    def evaluate_selector(
        self, nodes_of_interest: Set[str], operators: Set[Operator]
    ) -> Set[str]:
        """Evaluate a single selector on the graph."""

        # Get the node of interest.

        nodes = nodes_of_interest.copy()

        if Operator.wildcard in operators:
            for node in self.graph.nodes():
                nodes.add(node)
            return nodes

        for node_of_interest in nodes_of_interest:
            if Operator.upstream in operators:
                for node in networkx.ancestors(self.graph, node_of_interest):
                    nodes.add(node)

            if Operator.downstream in operators:
                for node in networkx.descendants(self.graph, node_of_interest):
                    nodes.add(node)

        return nodes

    def _get_tag_nodes(self, tag: str) -> Set[str]:
        """Get all nodes with a given tag."""
        selected_nodes = set()

        for node, tags in networkx.get_node_attributes(self.graph, "tags").items():

            if tag in tags:
                selected_nodes.add(node)

        return selected_nodes

    def _get_node_by_name(self, name: str) -> str:
        """Return a node's ID using the node tag attribute."""
        return self.node_mapping[name]

    def _get_source_by_name(self, name: str) -> Set[str]:
        """Return a node's ID using the node source_name attribute."""

        selected_nodes = set()

        for node, source_name in networkx.get_node_attributes(
            self.graph, "source_name"
        ).items():

            if name == source_name:
                selected_nodes.add(node)

        return selected_nodes

    def select(self, selector: str) -> Set[str]:
        """Select nodes from the graph using selectors."""
        nodes = set()

        operators = ""
        unioned_sections = selector.split(" ")
        for section in unioned_sections:

            if section == "":
                continue

            intersected_set: Set = set()
            intersections = section.split(",")

            for intersection in intersections:

                operators = set()
                if intersection == "*":
                    operators.add(Operator.wildcard)
                else:
                    if intersection.startswith("+"):
                        operators.add(Operator.upstream)

                    if intersection.endswith("+"):
                        operators.add(Operator.downstream)

                method = None
                value = None
                if ":" in intersection:
                    method, value = intersection.split(":")
                    value = re.sub(r"[^a-zA-Z\_0-9]", "", value)

                # In here, we need to find the operators assigned to
                # the selector nodes. Note that we should allow any
                # combination of operators to be applied!
                nodes_of_interest = set()

                if method == "tag":
                    nodes_of_interest = self._get_tag_nodes(tag=value)

                elif method == "source":
                    nodes_of_interest = self._get_source_by_name(name=value)

                elif method is None:
                    node_name = re.sub(r"[^a-zA-Z\_0-9]", "", intersection)
                    try:
                        nodes_of_interest.add(self._get_node_by_name(node_name))
                    except KeyError:
                        print(f"Warning! Unable to find node {node_name}.")

                elif method is not None:
                    raise NotImplemented(
                        "Unsupported selector method provided. {method}"
                    )

                node_set = self.evaluate_selector(nodes_of_interest, operators)

                if len(intersected_set) == 0:
                    intersected_set = node_set
                else:
                    intersected_set.intersection_update(node_set)

            nodes = nodes.union(intersected_set)

        return nodes

    def node_dependencies(self, source: str):
        return {
            child
            for parent, child in networkx.bfs_edges(
                self.graph, source, reverse=True, depth_limit=1
            )
        }

    def generate_selector(self, models: Dict[str, Model]) -> str:

        sections = set()
        singletons = set()

        # Make a graph!
        node_set = set(models.keys())

        inner_subgraph = networkx.dag_to_branching(self.graph.subgraph(node_set))
        original_nodes = networkx.get_node_attributes(inner_subgraph, "source")

        selected_end_points = set()

        for component in networkx.connected_components(
            networkx.to_undirected(inner_subgraph)
        ):
            print([self.model_mapping[original_nodes[node]] for node in component])

            # If there are two or fewer nodes, then simply add them.
            if len(component) <= 2:
                singletons = singletons.union(
                    {self.model_mapping[original_nodes[node]] for node in component}
                )

            # Do a topological sort, and find the intersection between the root and the leaf in the branching.

            component_subgraph = inner_subgraph.subgraph(component)

            sorted_nodes = list(networkx.topological_sort(component_subgraph))

            start_point = self.model_mapping[original_nodes[sorted_nodes[0]]]
            end_point = self.model_mapping[original_nodes[sorted_nodes[-1]]]

            selected_end_points = selected_end_points.union({start_point, end_point})

            sections.add(
                f"{start_point}+,+{end_point}"
            )

        sections = sections.union({singleton for singleton in singletons if singleton not in selected_end_points})

        return " ".join(sections)
