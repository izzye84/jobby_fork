from __future__ import annotations
from typing import Set, List, Dict, Optional


from pydantic import BaseModel


class Model(BaseModel):
    name: str
    unique_id: str
    depends_on: Set[str]
