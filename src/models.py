from dataclasses import dataclass
from typing import List

@dataclass
class HierarchyRule:
    """Immutable rule defining a seniority tier."""
    level: int
    name: str
    keywords: List[str]