from typing import Dict, List, Optional, Set
import json

import networkx
from dbt.graph import UniqueId
from dbt.node_types import NodeType
from pydantic import BaseModel, Field

from jobby.types.dag import DAG


class GenericConfig(BaseModel):
    enabled: bool


class GenericNode(BaseModel):
    name: str
    unique_id: str
    fqn: List[str]
    config: Optional[GenericConfig]
    depends_on: Optional[Dict] = Field(default_factory=dict)
    empty: bool = False
    tags: Set[str]
    resource_type: NodeType
    package_name: str
    source_name: Optional[str]

    @property
    def depends_on_nodes(self):
        return self.depends_on["nodes"]


class Manifest:
    def __init__(self, data: Dict):
        self.sources: Dict[UniqueId, GenericNode] = {
            key: GenericNode(**value) for key, value in data["sources"].items()
        }
        self.nodes: Dict[UniqueId, GenericNode] = {
            key: GenericNode(**value) for key, value in data["nodes"].items()
        }
        self.exposures: Dict[UniqueId, GenericNode] = {
            key: GenericNode(**value) for key, value in data["exposures"].items()
        }
        self.metrics: Dict[UniqueId, GenericNode] = {
            key: GenericNode(**value) for key, value in data["metrics"].items()
        }

    def get_model(self, unique_id: UniqueId) -> GenericNode:
        """Get a model using the model's UniqueId"""
        return self.nodes[unique_id]

    def get_model_name(self, unique_id: UniqueId) -> str:
        """Get the human-friendly name of a model using the model's UniqueId"""
        return self.nodes[unique_id].name


class ManifestOld:
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
