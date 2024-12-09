from __future__ import annotations

from dataclasses import dataclass
from typing import List


@dataclass
class Cat:
    id: str
    breeds: List | None
    url: str
    width: int
    height: int
