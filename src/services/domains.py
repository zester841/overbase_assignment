import re
import logging
from typing import Dict
from src.utils import StringUtils

class DomainResolver:
    def __init__(self, mappings: Dict[str, str]):
        self.mappings = mappings
        self.legal_strip = re.compile(r'\b(inc|ltd|llc|corp|corporation|company|group|labs)\b', re.IGNORECASE)

    def resolve(self, company: str) -> str:
        clean = StringUtils.sanitize(company)
        
        # 1. Config Lookup
        if clean in self.mappings:
            val = self.mappings[clean]
            if "TODO" in val:
                logging.warning(f"⚠️  Config Warning: '{clean}' has TODO value. Using fallback.")
                return f"{clean.lower().replace(' ', '')}.com"
            return val

        # 2. Heuristics
        if re.search(r'\.(ai|io|com|net|org)$', clean.lower()):
            return clean.lower()

        c = clean.lower()
        c = re.sub(r"\(.*?\)", "", c)
        c = self.legal_strip.sub("", c)
        c = re.sub(r"[^a-z0-9]", "", c)
        return f"{c}.com"