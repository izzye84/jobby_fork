from typing import List

import pydot

from jobby.types.job import Job


def generate_dot_graph(jobs: List[Job], name):
    """Create a PyDot Graph for a list of Jobs"""

    dot_graph = pydot.Dot(name, graph_type="digraph", rankdir="LR")

    for job in jobs:

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
