from __future__ import annotations

from typing import Set, List, Dict, Optional

from jobby.types.model import Model
from jobby.types.dag import DAG


class Job:
    def __init__(
        self,
        job_id: int,
        selector: str,
        dag: DAG,
        name: Optional[str] = None,
        exclude: Optional[str] = None,
    ) -> None:
        self.job_id = job_id
        self.selector = selector
        self.exclude = exclude
        self.dag = dag
        self.name: Optional[str] = name

        self.models: Dict[str, Model] = self._evaluate_models()

    def _evaluate_models(self) -> Dict:
        models = {}

        exclusions = set()
        if self.exclude:
            exclusions = self.dag.select(self.exclude)

        for node in self.dag.select(self.selector):
            if node.split(".")[0] not in ("model", "snapshot"):
                continue

            if node in exclusions:
                continue

            models[node] = Model(
                unique_id=node,
                depends_on=self.dag.node_dependencies(node),
                name=self.dag.model_mapping[node],
            )

        return models

    def model_dependencies(self) -> Set[str]:
        """Return a set of all of the external models that this job requires."""
        output = set()
        for _, model in self.models.items():
            output = output.union(model.depends_on)

        return output.difference(self.models.keys())

    def pop_model(self, model_name: str) -> Model:
        """Remove a model from the model dictionary by name, and return it."""
        unique_id: str = self.dag.node_mapping[model_name]
        model = self.models[unique_id].copy()
        del self.models[unique_id]

        self.selector = self.dag.generate_selector(self.models)

        return model

    def add_model(self, model: Model) -> None:
        """Add a model to the Job, and replan its selector"""
        self.models[model.unique_id] = model
        self.selector = self.dag.generate_selector(self.models)

    def union(self, other_jobs: List[Job]) -> Job:
        """Union two jobs such that they encompase the unioned set of models."""
        new_job = Job(job_id=self.job_id, selector=self.selector, dag=self.dag)

        for job in other_jobs:
            new_job.models.update(job.models)
            new_job.selector += f" {job.selector}"
            # new_job.depends_on.union(job.depends_on)
            # if job.job_id in new_job.depends_on:
            #     new_job.depends_on.remove(job.job_id)

        new_job.selector = new_job.dag.generate_selector(models=new_job.models)

        return new_job

    def __repr__(self) -> str:
        return "job_id: {job_id}, selector: {selector}, exclude: {exclude}\n\tmodels: {models}".format(
            job_id=self.job_id,
            selector=self.selector,
            exclude=self.exclude,
            models="\n\t\t".join([model.unique_id for _, model in self.models.items()]),
        )
