from typing import List, Dict, Tuple, Optional

from .types.model import Model
from .types.job import Job
from .types.dag import DAG


def distribute_job(
    dag: DAG, source_job: Job, target_jobs: List[Job]
) -> Tuple[dict[int, Job], Optional[Job]]:
    """Partition a job such that its responsitibilties are added to the target jobs."""

    # This is a bit like surgery. My idea is to extract out the
    # models that each of the target jobs need, leaving behind one last
    # Job for any remaining models

    for target_job in target_jobs:
        job_dependencies = target_job.model_dependencies()
        while len(job_dependencies) > 0:
            dependency = job_dependencies.pop()
            if dependency in source_job.models:
                del source_job.models[dependency]
                target_job.models[dependency] = Model(
                    unique_id=dependency,
                    name=dag.model_mapping[dependency],
                    depends_on=dag.node_dependencies(dependency),
                )
                job_dependencies = job_dependencies.union(
                    target_job.model_dependencies()
                )

    for job in target_jobs:
        job.selector = dag.generate_selector(job.models)

    source_job.selector = dag.generate_selector(source_job.models)

    return {job.job_id: job for job in target_jobs}, source_job if len(
        source_job.models
    ) > 0 else None


import pydot


def generate_dot_graph(jobs: List[Job], name):

    dot_graph = pydot.Dot(name, graph_type="digraph", rankdir="LR")

    for job in jobs:

        # job_graph.add_node(pydot.Node(job["name"]))

        subgraph = pydot.Cluster(f"job_{job.job_id}", label=job.name, simplify=True)

        if len(job.models) == 0:
            continue

        for unique_id, model in job.models.items():
            options = {}
            if model.unique_id.startswith("model"):
                options["fillcolor"] = "#4491b0"
                options["color"] = "#4491b0"
            elif model.unique_id.startswith("source"):
                options["fillcolor"] = "#66a138"
                options["color"] = "#66a138"

            subgraph.add_node(
                pydot.Node(
                    f"{unique_id}",
                    label=model.name,
                    shape="box",
                    style="filled",
                    fontcolor="white",
                    **options,
                )
            )

        dot_graph.add_subgraph(subgraph)

    for job in jobs:
        for unique_id, model in job.models.items():
            for depends_on in model.depends_on:

                if depends_on.split(".")[0] in ["macro", "operation", "test"]:
                    continue

                options = {}
                if depends_on.startswith("model"):
                    options["fillcolor"] = "#4491b0"
                    options["color"] = "#4491b0"
                elif depends_on.startswith("source"):
                    options["fillcolor"] = "#66a138"
                    options["color"] = "#66a138"

                dot_graph.add_node(
                    pydot.Node(
                        f"{depends_on}",
                        shape="box",
                        style="filled",
                        fontcolor="white",
                        **options,
                    )
                )

                dot_graph.add_edge(
                    pydot.Edge(f"{depends_on}", f"{unique_id}", color="#316c88")
                )

    return dot_graph
