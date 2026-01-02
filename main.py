import pandas as pd
import re
import logging
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any, Tuple
from pathlib import Path
from unidecode import unidecode
from pandarallel import pandarallel

# --- 0. SYSTEM SETUP ---
# Initialize Parallel Processing to bypass Python's Global Interpreter Lock (GIL).
# This divides O(N) complexity by the number of CPU cores available.
pandarallel.initialize(progress_bar=True, verbose=0)

# Professional Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# --- 1. CONFIGURATION LAYER (The "Control Panel") ---

@dataclass
class HierarchyRule:
    """Defines a specific tier in the organization."""
    level: int  # 1 is highest
    name: str
    keywords: List[str]

# Dynamic Rule Engine Configuration (Open/Closed Principle)
HIERARCHY_CONFIG = [
    HierarchyRule(
        level=1, 
        name="Strategic Leadership (C-Suite & Founders)", 
        keywords=['founder', 'ceo', 'cto', 'cmo', 'cfo', 'coo', 'cio', 'ciso', 'president', 'chairman', 'owner']
    ),
    HierarchyRule(
        level=2, 
        name="Executive Management", 
        keywords=['evp', 'svp', 'chief', 'partner', 'principal', 'head of']
    ),
    HierarchyRule(
        level=3, 
        name="Senior Management", 
        keywords=['vice president', 'vp', 'director']
    )
]

EXCLUSION_KEYWORDS = [
    'student', 'intern', 'analyst', 'engineer', 'consultant', 'architect', 'specialist', 
    'coordinator', 'manager', 'lead', 'associate'
]

DOMAIN_MAPPINGS = {
    "AWS": "amazon.com",
    "Amazon Web Services": "amazon.com",
    "Google Cloud": "google.com",
    "Heroku (Salesforce)": "salesforce.com",
    "Heroku": "salesforce.com",
    "IBM Storage": "ibm.com",
    "IBM Cloud": "ibm.com",
    "HP Cloud": "hp.com",
    "Mitel Networks": "mitel.com",
    "The Clorox Company": "clorox.com",
    "Predix, General Electric": "ge.com",
    "GE": "ge.com",
    "Tintri by DDN": "tintri.com",
    "VMware": "vmware.com",
    "Adobe Enterprise": "adobe.com"
}

# --- 2. CORE UTILITIES ---

class StringUtils:
    """Stateless utility for text hygiene."""
    @staticmethod
    def sanitize(text: Any) -> str:
        if pd.isna(text) or str(text).strip() in ["—", "-", ""]:
            return ""
        return unidecode(str(text)).strip()

    @staticmethod
    def extract_nickname(full_name: str) -> Tuple[str, str, Optional[str]]:
        """Parses complex names like 'Jennifer "JJ" Johnson'."""
        clean_name = StringUtils.sanitize(full_name)
        nickname = None
        
        match = re.search(r'\"(.*?)\"', clean_name)
        if match:
            nickname = match.group(1).lower().replace(" ", "")
            clean_name = clean_name.replace(f'"{match.group(1)}"', '').replace('  ', ' ')
            
        parts = clean_name.split()
        first = parts[0].lower() if parts else ""
        last = parts[-1].lower() if len(parts) > 1 else ""
        
        return first, last, nickname

# --- 3. BUSINESS LOGIC MODULES (Services) ---

class RankerEngine:
    """Evaluates titles against HIERARCHY_CONFIG using optimized Regex."""
    def __init__(self, rules: List[HierarchyRule], exclusions: List[str]):
        self.rules = sorted(rules, key=lambda x: x.level)
        # Pre-compile Regex for O(1) matching speed per row
        self.exclusion_pattern = re.compile(r'\b(' + '|'.join(exclusions) + r')\b', re.IGNORECASE)
        
    def assess_seniority(self, title: str) -> int:
        t = title.lower()
        
        if self.exclusion_pattern.search(t):
            return 999 

        # Chain of Responsibility
        for rule in self.rules:
            # We iterate rules to maintain hierarchy priority
            if any(k in t for k in rule.keywords):
                return rule.level
                
        return 999

class DomainIntelligence:
    """Resolves company names to domains."""
    def __init__(self, mappings: Dict[str, str]):
        self.mappings = mappings
        # Regex to strip legal entities
        self.legal_regex = re.compile(r'\b(inc|ltd|llc|corp|corporation|company|networks|labs|technologies|group)\b', re.IGNORECASE)

    def resolve(self, company: str) -> str:
        clean = StringUtils.sanitize(company)
        
        # 1. O(1) Lookup
        if clean in self.mappings:
            return self.mappings[clean]

        # 2. Heuristic Checks
        if re.search(r'\.(ai|io|com|net|co|org)$', clean.lower()):
            return clean.lower()

        # 3. Regex Cleaning
        c = clean.lower()
        c = re.sub(r"\(.*?\)", "", c)
        c = self.legal_regex.sub("", c) # Optimized Pre-compiled sub
        c = re.sub(r"[^a-z0-9]", "", c)
        
        return f"{c}.com"

class EmailStrategist:
    """Generates context-aware email permutations."""
    def generate(self, first: str, last: str, nickname: str, domain: str, seniority_level: int) -> List[str]:
        if not domain: return []
        
        candidates = []
        
        # Strategy A: Nickname Priority
        if nickname:
            if last: candidates.append(f"{nickname}.{last}@{domain}")
            candidates.append(f"{nickname}@{domain}")

        # Strategy B: Founder Priority (First Name Only)
        if seniority_level == 1 and first:
            candidates.append(f"{first}@{domain}")

        # Strategy C: Standard
        if first and last:
            candidates.append(f"{first}.{last}@{domain}")
            
        # Strategy D: Fallback
        if first and last:
            candidates.append(f"{first[0]}{last}@{domain}")

        return list(dict.fromkeys(candidates))

# --- 4. ORCHESTRATOR (Parallelized Pipeline) ---

class ProcessingPipeline:
    def __init__(self, input_path: str, output_path: str):
        self.input_path = Path(input_path)
        self.output_path = Path(output_path)
        
        # Dependency Injection
        self.ranker = RankerEngine(HIERARCHY_CONFIG, EXCLUSION_KEYWORDS)
        self.domain_resolver = DomainIntelligence(DOMAIN_MAPPINGS)
        self.email_strategist = EmailStrategist()

    def _process_single_row(self, row) -> Optional[pd.Series]:
        """
        Pure function designed for parallel execution.
        Takes a raw Series, returns a processed Series or None.
        """
        # 1. Clean Inputs
        raw_title = StringUtils.sanitize(row.get('Title', ''))
        raw_company = StringUtils.sanitize(row.get('Company', ''))
        raw_name = StringUtils.sanitize(row.get('Name', ''))

        # 2. Score Seniority (Business Logic)
        level = self.ranker.assess_seniority(raw_title)
        
        if level == 999:
            return None # Filter out immediately to save memory

        # 3. Resolve Attributes
        first, last, nick = StringUtils.extract_nickname(raw_name)
        domain = self.domain_resolver.resolve(raw_company)
        
        # 4. Generate Emails
        emails = self.email_strategist.generate(first, last, nick, domain, level)

        # 5. Return Enriched Data
        return pd.Series({
            "Name": raw_name,
            "Title": raw_title,
            "Company": raw_company,
            "Seniority_Level": level,
            "Likely_Email_1": emails[0] if len(emails) > 0 else "",
            "Likely_Email_2": emails[1] if len(emails) > 1 else "",
            "Domain_Logic": domain
        })

    def execute(self):
        logging.info(f"📂 Loading data from {self.input_path}...")
        
        try:
            df = pd.read_csv(self.input_path, on_bad_lines='skip')
        except FileNotFoundError:
            logging.error("Input file missing.")
            return

        logging.info(f"⚡ Spawning Parallel Workers across CPU Cores...")
        
        # PARALLEL EXECUTION --------------------------------------------------
        # We use parallel_apply instead of standard apply.
        # This keeps our complex OOP logic but scales it horizontally across cores.
        processed_df = df.parallel_apply(self._process_single_row, axis=1)
        # ---------------------------------------------------------------------

        # Filter out None values (Excluded rows)
        processed_df = processed_df.dropna()

        # Sorting & Final Selection
        processed_df = processed_df.sort_values(by=['Seniority_Level', 'Company'])
        final_df = processed_df.head(50)
        
        # Export
        final_df.to_csv(self.output_path, index=False)
        
        logging.info(f"✨ Pipeline Complete.")
        logging.info(f"💾 Output saved to {self.output_path} with {len(final_df)} executives.")
        print("\n" + final_df[['Name', 'Likely_Email_1', 'Seniority_Level']].head().to_string())

if __name__ == "__main__":
    pipeline = ProcessingPipeline("cmo_videos_names.csv", "final_execs.csv")
    pipeline.execute()