import re
from typing import List
from src.models import HierarchyRule

class RankerEngine:
    def __init__(self, rules: List[HierarchyRule], exclusions: List[str]):
        # Sort rules by priority (Level 1 first)
        self.rules = sorted(rules, key=lambda x: x.level)
        # Pre-compile regex for O(1) matching speed per row
        self.exclude_regex = re.compile(r'\b(' + '|'.join(exclusions) + r')\b', re.IGNORECASE)
        
    def assess_seniority(self, title: str) -> int:
        t = title.lower()
        if self.exclude_regex.search(t): return 999 
        
        for rule in self.rules:
            if any(k in t for k in rule.keywords):
                return rule.level
        return 999