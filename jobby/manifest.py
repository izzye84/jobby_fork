from typing import Dict
import json

import networkx

from jobby.dag import DAG


class Manifest:
    """Load a manifest JSON file and output a DAG."""

    def __init__(self, manifest_path: str) -> None:
        self.path = manifest_path
        self.data = json.load(open(self.path))
        self.graph: networkx.DiGraph = self.generate_graph(self.data)
        self.node_mapping = self.generate_node_mapping(self.data)
        self.dag = DAG(graph=self.graph, node_mapping=self.node_mapping)

    @staticmethod
    def generate_node_mapping(manifest_data: Dict) -> Dict[str, str]:
        """Generate a mapping between name and unique_id"""
        return {node["name"]: key for key, node in manifest_data["nodes"].items()}

    @staticmethod
    def generate_graph(manifest_data: Dict):
        """Generate the DiGraph from a manifest."""

        graph = networkx.DiGraph()

        for key, node in manifest_data["sources"].items():
            graph.add_node(
                key,
                tags=node["tags"],
                name=node["name"],
                source_name=node.get("source_name"),
                foundation=True,
            )

        for key, node in manifest_data["nodes"].items():

            if key.split(".")[0] in ["test", "operation"]:
                continue

            foundation = all(
                [
                    dependency.split(".")[0] == "source"
                    for dependency in node["depends_on"]["nodes"]
                ]
            )

            graph.add_node(
                key,
                tags=node["tags"],
                name=node["name"],
                source_name=node.get("source_name"),
                foundation=foundation,
            )

            for dependency in node["depends_on"]["nodes"]:
                if dependency.split(".")[0] in ["test", "operation"]:
                    continue

                graph.add_edge(dependency, key)

        return graph
