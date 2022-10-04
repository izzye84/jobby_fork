import json
import re
from dataclasses import dataclass
from typing import Optional, Set, List, Tuple, Dict

import dbt.flags
import networkx
from dbt.compilation import Linker, Compiler
from dbt.graph import UniqueId, ResourceTypeSelector, parse_difference, Graph
from dbt.graph.selector_spec import IndirectSelection, SelectionSpec
from dbt.node_types import NodeType
from loguru import logger

from jobby.dbt_cloud import DBTCloud
from jobby.selector_generator import SelectorGenerator
from jobby.types.job import Job
from jobby.types.manifest import Manifest, GenericNode
from jobby.types.model import Model


@dataclass
class MockConfig:
    target_path = "target"
    packages_install_path = "dbt_packages"


class Jobby:
    def __init__(
        self,
        manifest_path: str,
        account_id: Optional[int] = None,
        api_key: Optional[str] = None,
    ):
        self.dbt_cloud_client = DBTCloud(account_id, api_key)
        self.manifest = Manifest(json.load(open(manifest_path)))

        # Compile a graph

        self.graph: Graph = self._compile_graph(self.manifest)

        self.node_mapping = {
            unique_id: node.name for unique_id, node in self.manifest.nodes.items()
        }
        self.model_mapping = {value: key for key, value in self.node_mapping.items()}
        self.selector_generator = SelectorGenerator(
            manifest=self.manifest,
            graph=self.graph,
            selector_evaluator=self.get_models_for_selector,
        )

        self.checkpoints: Dict[str, Set[UniqueId]] = {}

        dbt.flags.INDIRECT_SELECTION = IndirectSelection.Eager

    @staticmethod
    def _compile_graph(manifest: Manifest):
        _linker = Linker()
        compiler = Compiler(MockConfig())
        compiler.link_graph(_linker, manifest, add_test_edges=False)
        return Graph(_linker.graph)

    def get_models_for_selector_strings(
        self, select: List[str], exclude: List[str]
    ) -> Set[UniqueId]:
        """Get a set of models given a select and exclude statement"""
        spec = parse_difference(select, exclude)
        return self.get_models_for_selector_specification(specification=spec)

    def get_models_for_selector_specification(
            self, specification: SelectionSpec
    ) -> Set[UniqueId]:
        """Get a set of models given a select and exclude statement"""

        selector = ResourceTypeSelector(
            graph=self.graph,
            manifest=self.manifest,
            previous_state=None,
            resource_types=[NodeType.Model],
        )

        return selector.get_selected(spec=specification)

    def get_job(self, job_id: int):
        """Generate a Job based on a dbt Cloud job."""

        dbt_cloud_job = self.dbt_cloud_client.get_job(job_id)

        job = Job(
            job_id=job_id,
            name=dbt_cloud_job["name"],
            steps=dbt_cloud_job["execute_steps"],
        )

        for step in job.steps:

            matches = re.search("(\-\-select|\-s) ([@+a-zA-Z0-9\_ :,]*)", step)
            select = matches.groups()[1].rstrip().split(" ")

            matches = re.search("(\-\-exclude|\-e) ([@+a-zA-Z0-9\_ :,]*)", step)
            exclude = None
            if matches:
                exclude = matches.groups()[1].rstrip().split(" ")

            job.selectors.append((select, exclude))

            models = self.get_models_for_selector(select, exclude)
            job.models.update(
                {
                    model: Model(
                        unique_id=model,
                        name=self.manifest.nodes[model].name,
                        depends_on=self.manifest.nodes[model].depends_on_nodes,
                    )
                    for model in models
                }
            )

        return job

    def distribute_job(
        self, source_job: Job, target_jobs: List[Job]
    ) -> Tuple[dict[int, Job], Optional[Job]]:
        """Partition a job such that its responsibilities are added to the target jobs."""

        logger.debug(
            "Distributing models from {source} into {targets}",
            source=source_job.name,
            targets=", ".join([target.name or "Unknown" for target in target_jobs]),
        )

        # This is a bit like surgery. My idea is to extract out the
        # models that each of the target jobs need, leaving behind one last
        # Job for any remaining models

        for target_job in target_jobs:
            job_dependencies = target_job.model_dependencies()

            while len(job_dependencies) > 0:
                dependency = job_dependencies.pop()

                if dependency in source_job.models:
                    del source_job.models[dependency]

                    if dependency.split(".")[0] not in ["model", "snapshot"]:
                        continue

                    logger.trace(
                        "Adding {dependency} from {source} to {target}",
                        dependency=dependency,
                        source=source_job.name,
                        target=target_job.name,
                    )

                    target_job.models[dependency] = Model(
                        unique_id=dependency,
                        name=self.node_mapping[dependency],
                        depends_on=self.graph.select_parents(
                            set([dependency]), max_depth=1
                        ),
                    )
                    job_dependencies.update(target_job.models[dependency].depends_on)
                    target_job.selectors.append(([self.manifest.get_model(dependency).name], []))

        for job in target_jobs:
            logger.debug("Generating new selector for {job}", job=job.name)
            logger.trace(
                "Original selector for {job}: {selector}",
                job=job.name,
                selector=job.selectors,
            )
            job.selector = self.selector_generator.generate(job)
            logger.trace(
                "New selector for {job}: {selector}",
                job=job.name,
                selector=job.selectors,
            )

        if len(source_job.models) > 0:
            logger.debug("Generating new selector for {job}", job=source_job.name)
            source_job.selector = self.selector_generator.generate(source_job)

        else:
            source_job = None

        return {job.job_id: job for job in target_jobs}, source_job

    def _select_foundation_selectors(self, selected_graph):
        sections = set()
        nodes = list(networkx.topological_sort(selected_graph))

        for node in nodes:

            ancestor_foundation = [
                selected_graph.nodes[ancestor]["foundation"] is True
                for ancestor in networkx.ancestors(selected_graph, node)
            ]

            if len(ancestor_foundation) == 0:
                continue

            if all(ancestor_foundation):
                selected_graph.nodes[node]["foundation"] = True

        foundation_nodes = {
            node
            for node, value in networkx.get_node_attributes(
                selected_graph, "foundation"
            ).items()
            if value is True
        }

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
                {f"+{self.model_mapping[leaf]}" for leaf in leaves}
            )

        return sections, removed_nodes

    def transfer_models(
        self, model_names: Set[UniqueId], source_job: Job, target_job: Job
    ) -> None:
        """Move a model from one job and place it in another job."""
        for model_name in model_names:
            model: Model = source_job.pop_model(unique_id=model_name)
            target_job.add_model(model)

        source_job.selectors = self.selector_generator.generate(source_job)
        target_job.selectors = self.selector_generator.generate(target_job)

    def optimize(self, job: Job) -> None:
        """Optimize a Job's selectors and run steps"""
        job.selectors = self.selector_generator.generate(job, optimize=True)
        job.steps = [f'dbt build {self.selector_generator.render_selector(job.selectors)}']

    def save_job_checkpoint(self, jobs: List[Job], name: str):
        """Save a checkpoint of current Job model selection for future validation"""
        self.checkpoints[name] = {model for job in jobs for model in job.models.keys()}

    def validate_selection_stability(self, jobs: List[Job], checkpoint_name: str) -> Tuple[Set[UniqueId], Set[UniqueId]]:
        """Validate current job model selection against a checkpoint."""
        current_models = {model for job in jobs for model in job.models.keys()}
        original_models = self.checkpoints[checkpoint_name]
        missing = original_models.difference(current_models)
        added = current_models.difference(original_models)

        exceptions = []

        if len(missing) > 0:
            exception = Exception(f"The output job set is missing models! {missing}")
            logger.exception(exception)
            exceptions.append(exception)

        if len(added) > 0:
            exception = Exception(f"The output job set has added models! {added}")
            logger.exception(exception)
            exceptions.append(exception)

        if len(exceptions) > 0:
            raise exceptions[0]

        logger.success("New job matches original model selection.")

        return missing, added
