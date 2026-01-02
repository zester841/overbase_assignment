import pandas as pd
import re
import logging
import json
import time
from dataclasses import dataclass
from typing import List, Dict, Optional, Any, Tuple
from pathlib import Path
from unidecode import unidecode
from pandarallel import pandarallel

# --- 0. SYSTEM BOOT ---
# Initialize shared memory for parallel workers
pandarallel.initialize(progress_bar=True, verbose=0)

# Configure logging to look like a build tool
logging.basicConfig(level=logging.INFO, format='%(message)s')

# Constants
CONFIG_FILE = "pipeline_config.json"
INPUT_FILE = "cmo_videos_names.csv"
OUTPUT_FILE = "final_execs.csv"

# --- 1. DATA MODELS ---

@dataclass
class HierarchyRule:
    """Immutable rule defining a seniority tier."""
    level: int
    name: str
    keywords: List[str]

# --- 2. CONFIGURATION SERVICE ---

class ConfigLoader:
    """
    Handles the bridge between the JSON definition and Python Logic.
    This ensures the pipeline is strictly configuration-driven.
    """
    @staticmethod
    def load_config(path: str) -> Dict:
        file_path = Path(path)
        if not file_path.exists():
            logging.error(f"🛑 FATAL: Configuration '{path}' missing.")
            logging.error("   -> Run 'python auto_generate_config.py' first.")
            exit(1)
        
        with open(file_path, 'r') as f:
            return json.load(f)

    @staticmethod
    def parse_rules(raw_rules: List[Dict]) -> List[HierarchyRule]:
        """Hydrates JSON objects into Python Dataclasses."""
        return [HierarchyRule(**r) for r in raw_rules]

# --- 3. UTILITIES ---

class StringUtils:
    @staticmethod
    def sanitize(text: Any) -> str:
        if pd.isna(text) or str(text).strip() in ["—", "-", ""]:
            return ""
        return unidecode(str(text)).strip()

    @staticmethod
    def extract_nickname(full_name: str) -> Tuple[str, str, Optional[str]]:
        """
        Parses 'Jennifer "JJ" Johnson' -> ('jennifer', 'johnson', 'jj')
        """
        clean = StringUtils.sanitize(full_name)
        nick = None
        
        # Capture text inside quotes
        match = re.search(r'\"(.*?)\"', clean)
        if match:
            nick = match.group(1).lower().replace(" ", "")
            # Remove nickname from main string to get clean First/Last
            clean = clean.replace(f'"{match.group(1)}"', '').replace('  ', ' ')
            
        parts = clean.split()
        first = parts[0].lower() if parts else ""
        last = parts[-1].lower() if len(parts) > 1 else ""
        
        return first, last, nick

# --- 4. CORE ENGINES (Business Logic) ---

class RankerEngine:
    def __init__(self, rules: List[HierarchyRule], exclusions: List[str]):
        # Sort rules by priority (Level 1 first)
        self.rules = sorted(rules, key=lambda x: x.level)
        # Pre-compile exclusion regex for O(1) checking
        self.exclude_regex = re.compile(r'\b(' + '|'.join(exclusions) + r')\b', re.IGNORECASE)
        
    def assess_seniority(self, title: str) -> int:
        t = title.lower()
        
        # Fast fail on exclusion keywords
        if self.exclude_regex.search(t): 
            return 999 
        
        # Chain of Responsibility: Check tiers in order
        for rule in self.rules:
            if any(k in t for k in rule.keywords):
                return rule.level
                
        return 999 # No match

class DomainResolver:
    def __init__(self, mappings: Dict[str, str]):
        self.mappings = mappings
        # Regex to strip legal entities (Inc, LLC, etc)
        self.legal_strip = re.compile(r'\b(inc|ltd|llc|corp|corporation|company|group|labs)\b', re.IGNORECASE)

    def resolve(self, company: str) -> str:
        clean = StringUtils.sanitize(company)
        
        # 1. Config Lookup (Highest Priority)
        if clean in self.mappings:
            val = self.mappings[clean]
            # Safety check: If user forgot to remove "TODO" from JSON
            if "TODO" in val:
                logging.warning(f"⚠️  Config Warning: '{clean}' maps to a TODO value. Falling back to heuristic.")
            else:
                return val

        # 2. Heuristic: Is it already a domain?
        if re.search(r'\.(ai|io|com|net|org)$', clean.lower()):
            return clean.lower()

        # 3. Algorithmic Fallback
        c = clean.lower()
        c = re.sub(r"\(.*?\)", "", c) # Remove parentheticals
        c = self.legal_strip.sub("", c)
        c = re.sub(r"[^a-z0-9]", "", c) # Remove special chars
        return f"{c}.com"

class EmailEngine:
    """Strategy pattern for generating contact permutations."""
    def generate(self, first, last, nick, domain, level) -> List[str]:
        if not domain: return []
        opts = []
        
        # Strategy A: Nickname (High Confidence)
        if nick:
            if last: opts.append(f"{nick}.{last}@{domain}")
            opts.append(f"{nick}@{domain}")

        # Strategy B: Founder Vanity URL
        if level == 1 and first:
            opts.append(f"{first}@{domain}")

        # Strategy C: Standard Corporate
        if first and last:
            opts.append(f"{first}.{last}@{domain}")
            opts.append(f"{first[0]}{last}@{domain}")

        # Deduplicate while preserving priority order
        return list(dict.fromkeys(opts))

# --- 5. PIPELINE ORCHESTRATOR ---

class Pipeline:
    def __init__(self):
        logging.info(f"⚙️  Initializing Pipeline...")
        
        # 1. Load Dynamic Config
        raw_conf = ConfigLoader.load_config(CONFIG_FILE)
        
        # 2. Inject Dependencies
        self.ranker = RankerEngine(
            ConfigLoader.parse_rules(raw_conf["HIERARCHY_RULES"]), 
            raw_conf["EXCLUSION_KEYWORDS"]
        )
        self.resolver = DomainResolver(raw_conf["DOMAIN_MAPPINGS"])
        self.emailer = EmailEngine()

    def _worker(self, row) -> Optional[pd.Series]:
        """
        Pure function to be distributed across CPU cores.
        """
        # A. Clean
        title = StringUtils.sanitize(row.get('Title'))
        
        # B. Filter (Fast)
        rank = self.ranker.assess_seniority(title)
        if rank == 999: return None 

        # C. Enrich
        first, last, nick = StringUtils.extract_nickname(row.get('Name'))
        domain = self.resolver.resolve(row.get('Company'))
        
        # D. Generate
        emails = self.emailer.generate(first, last, nick, domain, rank)

        return pd.Series({
            "Name": row.get('Name'),
            "Title": title,
            "Company": row.get('Company'),
            "Rank": rank,
            "Email_1": emails[0] if len(emails) > 0 else "",
            "Email_2": emails[1] if len(emails) > 1 else "",
            "Domain_Source": domain
        })

    def run(self):
        logging.info(f"📂 Loading Source: {INPUT_FILE}")
        try:
            df = pd.read_csv(INPUT_FILE, on_bad_lines='skip')
        except FileNotFoundError:
            logging.error("🛑 Input file missing.")
            return

        logging.info(f"🚀 Spawning workers on available cores...")
        start_time = time.time()
        
        # --- PARALLEL EXECUTION ---
        results = df.parallel_apply(self._worker, axis=1)
        # --------------------------
        
        # Filter and Sort
        results = results.dropna()
        final_df = results.sort_values(by=['Rank', 'Company']).head(50)
        
        # Export
        final_df.to_csv(OUTPUT_FILE, index=False)
        duration = round(time.time() - start_time, 2)
        
        logging.info(f"💾 Success! Wrote {len(final_df)} records to {OUTPUT_FILE}")
        logging.info(f"⏱️  Execution Time: {duration}s")
        print("\n" + final_df[['Name', 'Email_1', 'Rank']].head().to_string())

if __name__ == "__main__":
    Pipeline().run()