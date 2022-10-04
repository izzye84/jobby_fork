from typing import Dict, List, Optional, Set

from dbt.graph import UniqueId
from dbt.node_types import NodeType
from pydantic import BaseModel, Field


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
    path: str

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
