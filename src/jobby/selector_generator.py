from typing import List, Tuple, Callable, Set, Dict

import networkx
from dbt.graph import Graph, UniqueId
from loguru import logger

from jobby.types.job import Job
from jobby.types.manifest import Manifest


class SelectorGenerator:
    def __init__(
            self,
            manifest: Manifest,
            graph: Graph,
            selector_evaluator: Callable[[List[str], List[str]], Set[UniqueId]],
    ):
        self.manifest = manifest
        self.graph = graph
        self.digraph: networkx.DiGraph = graph.graph
        self.evaluate: Callable[
            [List[str], List[str]], Set[UniqueId]
        ] = selector_evaluator

    def identify_foundation_nodes(self, selected_graph: networkx.DiGraph):
        # Set a baseline
        networkx.set_node_attributes(selected_graph, False, 'foundation')

        # Add foundation attribute for foundational nodes.
        nodes_to_update: Dict[UniqueId, bool] = {}
        for unique_id, node in self.manifest.nodes.items():
            source_models = [
                    dependency.split(".")[0] == "source"
                    for dependency in node.depends_on_nodes
                ]
            foundation = all(source_models) and len(source_models) > 0
            nodes_to_update[unique_id] = foundation
            if foundation:
                logger.trace("{unique} is a foundation node. {deps}",
                               unique=unique_id,
                               deps=node.depends_on_nodes)
        networkx.set_node_attributes(selected_graph, nodes_to_update, 'foundation')

    def _select_foundation_selectors(self, selected_graph):

        sections = set()
        self.identify_foundation_nodes(selected_graph)
        nodes = list(networkx.topological_sort(selected_graph))

        for node in nodes:
            ancestor_foundation: Dict[UniqueId, bool] = {}
            for dependency in self.manifest.get_model(node).depends_on_nodes:
                if dependency not in selected_graph.nodes:
                    ancestor_foundation[dependency] = False
                else:
                    ancestor_foundation[dependency] = selected_graph.nodes[dependency]["foundation"] is True

            if selected_graph.nodes[node]["foundation"]:
                logger.trace("{node} has already been identified as foundational.", node=node)
                continue

            elif len(ancestor_foundation) == 0:
                selected_graph.nodes[node]["foundation"] = False

            elif all(ancestor_foundation.values()):
                logger.trace("{node} identified as foundational. {ancestors}", node=node, ancestors=ancestor_foundation)
                selected_graph.nodes[node]["foundation"] = True

            else:
                selected_graph.nodes[node]["foundation"] = False

        foundation_nodes = {
            node
            for node, value in networkx.get_node_attributes(
                selected_graph, "foundation"
            ).items()
            if value is True
        }

        logger.trace("Foundation nodes: {nodes}", nodes=foundation_nodes)

        colors = {}
        for node in selected_graph.nodes:
            if selected_graph.nodes[node]['foundation']:
                colors[node] = 'red'
            else:
                colors[node] = 'green'

        # import matplotlib.pyplot as plt
        # positions= networkx.nx_pydot.graphviz_layout(selected_graph, prog='dot')
        # networkx.draw_networkx(selected_graph, pos=positions,
        #                        with_labels=True, node_color=colors.values(),
        #                        font_size=8)
        # plt.show()

        removed_nodes = set()

        foundation_subgraph = selected_graph.subgraph(foundation_nodes)
        for component in networkx.connected_components(
                networkx.to_undirected(foundation_subgraph)
        ):

            if len(component) <= 1:
                continue

            component_subgraph = selected_graph.subgraph(component)

            removed_nodes.update(component)
            leaves = (
                x
                for x in component_subgraph.nodes()
                if component_subgraph.out_degree(x) == 0
            )
            sections = sections.union(
                {f"+{self.manifest.get_model_name(leaf)}" for leaf in leaves}
            )

        return sections, removed_nodes

    def _create_new_selector(self, job: Job) -> List[Tuple[List[str], List[str]]]:
        """Generate a new list of selectors"""

        sections: Set[str] = set()
        singletons = set()

        # Make a graph!
        node_set: Set[UniqueId] = set.union(*[self.evaluate(select, exclude) for select, exclude in job.selectors])

        selected_graph = self.digraph.subgraph(node_set).copy()

        known_nodes = set()

        # Find stable subgraphs and add them to the sections
        new_sections, foundation_nodes = self._select_foundation_selectors(
            selected_graph
        )

        sections.update(new_sections)
        selected_graph.remove_nodes_from(foundation_nodes)
        known_nodes.update(foundation_nodes)

        selected_end_points = set()

        max_iter = 10000000
        iter = 0
        while iter <= max_iter and len(selected_graph.nodes) > 0:
            logger.trace("Branching iteration {iteration}", iteration=iter)
            iter += 1

            # logger.debug('Nodes: {nodes}. Edges: {edges}', nodes=selected_graph.nodes, edges=selected_graph.edges)

            if len(selected_graph.nodes) <= 2:
                # logger.debug('Two nodes or fewer remaining. Adding them and moving on. {nodes}.', nodes=selected_graph.nodes)
                sections.update(
                    {self.manifest.get_model_name(node) for node in selected_graph.nodes}
                )
                selected_graph.remove_nodes_from(
                    {node for node in selected_graph.nodes}
                )
                continue

            longest_branch = networkx.dag_longest_path(selected_graph)

            # logger.debug("Branches: {branches}", branches=largest_branchs)

            if len(longest_branch) < 3:
                # logger.warning("Longest branch is short enough for direct selection.")
                sections.update({self.manifest.get_model_name(node) for node in longest_branch})
                # logger.warning("Directly adding the following nodes: {nodes}", nodes=longest_branch)
                selected_graph.remove_nodes_from(longest_branch)
                continue

            # largest_branch = largest_branchs[0]

            # Grab the start and end
            component_subgraph = selected_graph.subgraph(longest_branch).copy()

            sorted_nodes = [
                node for node in networkx.topological_sort(component_subgraph)
            ]

            start_point = self.manifest.get_model_name(sorted_nodes[0])
            end_point = self.manifest.get_model_name(sorted_nodes[-1])
            new_section = f"{start_point}+,+{end_point}"
            sections.add(new_section)

            # Remove the branching nodes from selected graph
            selected_graph.remove_nodes_from({node for node in sorted_nodes})

        return [(list(sections), [])]

    def _generate_trivial_selector(self, job: Job) -> List[Tuple[List[str], List[str]]]:
        """Generate a new list of selectors based on the trivial set of selectors possible"""
        return [([model.name for model in job.models.values()], [])]

    def _validate_selection(
            self, original_models: Set[UniqueId], new_models: Set[UniqueId]
    ):
        """Confirm that two model sets are identical. Raise exception if not."""

        difference = original_models.symmetric_difference(new_models)
        if len(difference) != 0:
            removed = original_models.difference(new_models)
            added = new_models.difference(original_models)

            raise Exception(
                f"Identified selector drift. Added: {added}. Removed {removed}"
            )

    @staticmethod
    def render_selector(selector: List[Tuple[List[str], List[str]]]) -> str:
        """Render a selection into an argument string"""
        select_list = []
        exclude_list = []
        for select, exclude in selector:
            select_list.extend(select)
            exclude_list.extend(exclude)

        select = f"--select {' '.join(select_list)}"
        exclude = f"--exclude {' '.join(exclude_list)}"

        return select + (exclude if len(exclude_list) > 0 else '')

    def generate(self, job: Job, optimize=False) -> List[Tuple[List[str], List[str]]]:
        """Generate a selector for a Job"""

        # Get the original list of models for future comparison.
        original_models: Set[UniqueId] = set(job.models.copy().keys())

        logger.info("Generating selector for {job}", job=job.name)

        if optimize:
            new_selector = self._create_new_selector(job)
        else:
            new_selector = self._generate_trivial_selector(job)

        new_model_lists = [
            self.evaluate(select_list, exclude_list)
            for select_list, exclude_list in new_selector
        ]
        new_models = set.union(*new_model_lists)

        try:
            self._validate_selection(original_models, new_models)
        except Exception as exception:
            logger.error(exception)
            logger.info("Identifying errant items.")

            removed = original_models.difference(new_models)
            added = new_models.difference(original_models)

            for select_list, exclude_list in new_selector:
                for select in select_list:
                    new_model_lists = self.evaluate([select], [])
                    if len(added.intersection(new_model_lists)) > 0:
                        logger.error("{selector} is responsible for adding {intersection}",
                                     selector=select, intersection = added.intersection(new_model_lists))


        logger.success("The new selector for {job} has been confirmed to be stable.", job=job.name)

        return new_selector
