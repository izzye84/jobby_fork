from __future__ import annotations

from typing import Set, List, Dict, Optional, Tuple

from dbt.graph import UniqueId
from loguru import logger

from jobby.types.model import Model


class Job:
    def __init__(
        self,
        job_id: int,
        name: str,
        steps: List[str],
        selectors: List[Tuple[List[str], List[str]]] = None,
        models: Optional[Dict[UniqueId, Model]] = None,
    ) -> None:
        self.job_id = job_id
        self.name: Optional[str] = name
        self.steps = steps
        self.models: Dict[UniqueId, Model] = models if models is not None else {}
        self.selectors: List[Tuple[List[str], List[str]]] = (
            selectors if selectors is not None else []
        )

    def model_dependencies(self) -> Set[str]:
        """Return a set of all the external models that this job requires."""
        output = set()
        for _, model in self.models.items():
            output.update(model.depends_on)

        return output.difference(self.models.keys())

    def pop_model(self, unique_id: UniqueId) -> Model:
        """Remove a model from the model dictionary by name, and return it."""
        model = self.models[unique_id].copy()
        del self.models[unique_id]

        return model

    def add_model(self, model: Model) -> None:
        """Add a model to the Job, and replan its selector"""
        self.models[model.unique_id] = model

    def union(self, other_jobs: List[Job]) -> Job:
        """Union two jobs such that they encompass the unioned set of models."""

        logger.debug(
            "Performing union of {job_1_name} and {job_2_name}",
            job_1_name=self.name,
            job_2_name=", ".join([job.name or "Unknown" for job in other_jobs]),
        )

        new_job = Job(
            job_id=self.job_id,
            name=self.name,
            steps=self.steps,
            selectors=self.selectors,
            models=self.models,
        )

        for job in other_jobs:
            for unique_id, model in job.models.items():

                if unique_id in new_job.models:
                    continue

                new_job.models[unique_id] = model.copy()
            new_job.selectors.extend(job.selectors)
            new_job.steps.extend(job.steps)

        return new_job

    def generate_terraform_import(self) -> Tuple[str, str]:
        """Return a terraform resource and the import command to import this job."""
        # Write terraform resources and the import commands

        def generate_job_name(raw_name: str) -> str:
            import re

            name = re.sub(r"[^\w]", " ", raw_name)
            name = name.replace(" ", "_").lower()
            name = re.sub(r"\_{2,}", "_", name)
            return name

        resource = """
        resource "dbt_cloud_job" "{name}" {{

        }}
        """.format(
            name=generate_job_name(self.name)
        )

        command = "terraform import dbt_cloud_job.{name} {job_id}".format(
            name=generate_job_name(self.name),
            job_id=self.job_id,
        )

        return resource, command

    def __repr__(self) -> str:
        return "job_id: {job_id}, steps: {steps}\n\tmodels: {models}".format(
            job_id=self.job_id,
            steps=self.steps,
            models="\n\t\t".join([model.unique_id for _, model in self.models.items()]),
        )
