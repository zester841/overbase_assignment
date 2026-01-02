import json
import logging
from pathlib import Path
from typing import List, Dict
from src.models import HierarchyRule

class ConfigLoader:
    @staticmethod
    def load_file(path: str) -> Dict:
        file_path = Path(path)
        if not file_path.exists():
            logging.error(f"🛑 FATAL: Configuration '{path}' missing.")
            logging.error("   -> Run 'python auto_generate_config.py' first.")
            exit(1)
        
        with open(file_path, 'r') as f:
            logging.info(f"⚙️  Loaded Config: {path}")
            return json.load(f)

    @staticmethod
    def parse_rules(raw_rules: List[Dict]) -> List[HierarchyRule]:
        return [HierarchyRule(**r) for r in raw_rules]