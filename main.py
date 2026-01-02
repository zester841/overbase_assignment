import pandas as pd
import re
import logging
import json
import time
import sys
from abc import ABC, abstractmethod
from typing import List, Dict, Optional, Any
from pathlib import Path
from unidecode import unidecode
from dataclasses import dataclass
from pandarallel import pandarallel

# --- 0. CONFIGURATION & LOGGING ---
logging.basicConfig(level=logging.INFO, format='%(message)s')

CONFIG_FILE = "pipeline_config.json"
INPUT_FILE = "cmo_videos_names.csv"
OUTPUT_FILE = "final_execs.csv"
PARALLEL_THRESHOLD = 1000

# --- 1. DATA CONTRACTS (DTOs) ---
@dataclass
class ExecutiveProfile:
    """Data Transfer Object representing a processed executive."""
    original_name: str
    clean_title: str
    company: str
    seniority_level: int
    emails: List[str]
    domain: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "Name": self.original_name,
            "Title": self.clean_title,
            "Company": self.company,
            "Seniority_Level": self.seniority_level,
            "Email_1": self.emails[0] if len(self.emails) > 0 else "",
            "Email_2": self.emails[1] if len(self.emails) > 1 else "",
            "Domain_Used": self.domain
        }

# --- 2. ABSTRACTIONS (INTERFACES) ---
class IConfigLoader(ABC):
    @abstractmethod
    def load(self) -> Dict[str, Any]: pass

class IDataSource(ABC):
    @abstractmethod
    def fetch_data(self) -> pd.DataFrame: pass

class IDataExporter(ABC):
    @abstractmethod
    def save(self, data: pd.DataFrame, path: str) -> None: pass

class IEnrichmentStrategy(ABC):
    @abstractmethod
    def process(self, df: pd.DataFrame, logic_handler: Any) -> pd.DataFrame: pass

# --- 3. IMPLEMENTATIONS: CORE LOGIC ---

class JsonConfigLoader(IConfigLoader):
    def __init__(self, filepath: str):
        self.filepath = filepath
        
    def load(self) -> Dict[str, Any]:
        if not Path(self.filepath).exists():
            sys.exit(f"🛑 Config file {self.filepath} missing.")
        try:
            with open(self.filepath, 'r') as f:
                return json.load(f)
        except json.JSONDecodeError:
            sys.exit(f"🛑 FATAL: {self.filepath} contains invalid JSON.")

class CsvDataSource(IDataSource):
    def __init__(self, filepath: str):
        self.filepath = filepath

    def fetch_data(self) -> pd.DataFrame:
        if not Path(self.filepath).exists():
            logging.error(f"❌ Input file {self.filepath} not found.")
            return pd.DataFrame()
        logging.info(f"📂 Loading CSV: {self.filepath}")
        df = pd.read_csv(self.filepath, on_bad_lines='skip')
        # Clean headers
        df.columns = df.columns.str.replace('ï»¿', '').str.strip()
        return df

class CsvExporter(IDataExporter):
    def save(self, df: pd.DataFrame, path: str) -> None:
        if df.empty:
            logging.warning("⚠️ No data to save.")
            return
        df.to_csv(path, index=False)
        logging.info(f"💾 Saved {len(df)} records to {path}")

# --- 4. DOMAIN LOGIC SERVICES ---

class RankerService:
    def __init__(self, config: Dict):
        self.rules = sorted(config["HIERARCHY_RULES"], key=lambda x: x['level'])
        self.exclusions = [k.lower() for k in config.get("EXCLUSION_KEYWORDS", [])]

    def get_rank(self, title: str) -> int:
        if pd.isna(title): return 999
        t = str(title).lower().strip()
        for exc in self.exclusions:
            if exc in t: return 999
        for rule in self.rules:
            for keyword in rule['keywords']:
                if keyword in t: return rule['level']
        return 999

class DomainService:
    def __init__(self, config: Dict):
        self.mapping = config.get("DOMAIN_MAPPINGS", {})
        self.legal_strip = re.compile(r'\b(inc|ltd|llc|corp|corporation|company|group|labs)\b', re.IGNORECASE)

    def resolve(self, company: str) -> str:
        if pd.isna(company): return "unknown.com"
        clean = unidecode(str(company)).strip()
        if clean in self.mapping: return self.mapping[clean]
        if re.search(r'\.(ai|io|com|net|org)$', clean.lower()): return clean.lower()
        
        # Heuristic cleaning
        c = clean.lower()
        c = re.sub(r"\(.*?\)", "", c)
        c = self.legal_strip.sub("", c)
        c = re.sub(r"[^a-z0-9]", "", c)
        return f"{c}.com"

class EmailGenService:
    def generate(self, name: str, domain: str, rank: int) -> List[str]:
        if not domain or domain == "unknown.com": return []
        
        # Name Parsing
        clean = unidecode(str(name)).strip()
        if clean in ["-", "—", ""]: return []
        
        nick = None
        match = re.search(r'\"(.*?)\"', clean)
        if match:
            nick = match.group(1).lower().replace(" ", "")
            clean = clean.replace(f'"{match.group(1)}"', '').replace('  ', ' ')
        
        parts = clean.split()
        first = parts[0].lower() if parts else ""
        last = parts[-1].lower() if len(parts) > 1 else ""

        options = []
        if nick:
            if last: options.append(f"{nick}.{last}@{domain}")
            options.append(f"{nick}@{domain}")
        if rank == 1 and first:
            options.append(f"{first}@{domain}")
        if first and last:
            options.append(f"{first}.{last}@{domain}")
            options.append(f"{first[0]}{last}@{domain}")
            
        return list(dict.fromkeys(options))

class ExecutiveProcessor:
    """
    Facade class that holds references to all domain services.
    Passed to the processing strategy.
    """
    def __init__(self, config: Dict):
        self.ranker = RankerService(config)
        self.domain_resolver = DomainService(config)
        self.email_gen = EmailGenService()

    def transform_row(self, row) -> Optional[pd.Series]:
        title = unidecode(str(row.get('Title', ''))).strip()
        rank = self.ranker.get_rank(title)
        
        if rank == 999: return None

        company = row.get('Company', '')
        domain = self.domain_resolver.resolve(company)
        emails = self.email_gen.generate(row.get('Name'), domain, rank)
        
        profile = ExecutiveProfile(
            original_name=row.get('Name'),
            clean_title=title,
            company=company,
            seniority_level=rank,
            emails=emails,
            domain=domain
        )
        return pd.Series(profile.to_dict())

# --- 5. STRATEGIES (The "Adaptive" Logic) ---

class SingleThreadStrategy(IEnrichmentStrategy):
    def process(self, df: pd.DataFrame, logic_handler: ExecutiveProcessor) -> pd.DataFrame:
        logging.info("⚡ Strategy: Single-Threaded (Optimized for small datasets)")
        return df.apply(logic_handler.transform_row, axis=1)

class ParallelStrategy(IEnrichmentStrategy):
    def process(self, df: pd.DataFrame, logic_handler: ExecutiveProcessor) -> pd.DataFrame:
        logging.info("🚀 Strategy: Multi-Core Parallel (Optimized for scale)")
        pandarallel.initialize(progress_bar=True, verbose=0)
        return df.parallel_apply(logic_handler.transform_row, axis=1)

# --- 6. ORCHESTRATOR (Dependency Injection Container) ---

class PipelineContext:
    """
    The High-Level Manager. 
    Notice it depends on Interfaces (Abstractions), not concrete classes.
    """
    def __init__(self, 
                 loader: IConfigLoader,
                 source: IDataSource, 
                 exporter: IDataExporter):
        self.loader = loader
        self.source = source
        self.exporter = exporter

    def execute(self):
        start_t = time.time()
        
        # 1. Load Config
        config = self.loader.load()
        
        # 2. Load Data
        df = self.source.fetch_data()
        if df.empty: return

        # 3. Initialize Logic
        processor_logic = ExecutiveProcessor(config)

        # 4. Select Strategy (Factory Logic)
        if len(df) > PARALLEL_THRESHOLD:
            strategy = ParallelStrategy()
        else:
            strategy = SingleThreadStrategy()

        # 5. Process
        results = strategy.process(df, processor_logic)

        # 6. Clean & Filter
        results = results.dropna()
        final_df = results.sort_values(by=['Seniority_Level', 'Company']).head(50)

        # 7. Export
        self.exporter.save(final_df, OUTPUT_FILE)
        
        # 8. Report
        duration = round(time.time() - start_t, 2)
        logging.info(f"✅ Pipeline Completed in {duration}s. Valid Leads: {len(results)}")
        if not final_df.empty:
            print("\n" + final_df[['Name', 'Title', 'Email_1']].head().to_string())

# --- 7. ENTRY POINT ---

if __name__ == "__main__":
    # Dependency Injection happens here.
    # We wire the implementations to the interfaces.
    
    config_loader = JsonConfigLoader(CONFIG_FILE)
    csv_source = CsvDataSource(INPUT_FILE)
    csv_exporter = CsvExporter()

    # Create the pipeline with dependencies injected
    pipeline = PipelineContext(
        loader=config_loader,
        source=csv_source,
        exporter=csv_exporter
    )
    
    pipeline.execute()