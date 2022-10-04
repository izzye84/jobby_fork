from __future__ import annotations

from typing import Set

from dbt.graph import UniqueId
from pydantic import BaseModel


class Model(BaseModel):
    name: str
    unique_id: UniqueId
    depends_on: Set[str]
