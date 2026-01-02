import pandas as pd
import re
import logging
import json
import time
import sys
from pathlib import Path
from unidecode import unidecode
from pandarallel import pandarallel

# --- 0. SETUP ---
# Initialize Parallel Processing
pandarallel.initialize(progress_bar=True, verbose=0)

# Logging Setup
logging.basicConfig(level=logging.INFO, format='%(message)s')

CONFIG_FILE = "pipeline_config.json"
INPUT_FILE = "cmo_videos_names.csv"
OUTPUT_FILE = "final_execs.csv"

# --- 1. THE LOGIC ENGINES ---

class RankerEngine:
    """Determines seniority based on JSON config."""
    def __init__(self, config):
        self.rules = sorted(config["HIERARCHY_RULES"], key=lambda x: x['level'])
        self.exclusions = [k.lower() for k in config.get("EXCLUSION_KEYWORDS", [])]

    def assess(self, title):
        if pd.isna(title): return 999
        t = str(title).lower().strip()
        
        # 1. Check Exclusions (Strict Filter)
        # Using simple string check instead of complex regex to prevent errors
        for exc in self.exclusions:
            # We look for whole words to avoid partial matches if possible, 
            # but for "manager" we want to be strict.
            if exc in t:
                return 999
            
        # 2. Check Hierarchy Levels
        for rule in self.rules:
            for keyword in rule['keywords']:
                if keyword.lower() in t:
                    return rule['level']
                
        return 999 # No match found

class DomainResolver:
    """Cleans company names into domains."""
    def __init__(self, config):
        self.mapping = config.get("DOMAIN_MAPPINGS", {})
        self.legal_strip = re.compile(r'\b(inc|ltd|llc|corp|corporation|company|group|labs)\b', re.IGNORECASE)

    def resolve(self, company):
        if pd.isna(company): return "unknown.com"
        clean_c = unidecode(str(company)).strip()
        
        # 1. Config Lookup
        if clean_c in self.mapping:
            val = self.mapping[clean_c]
            # CRITICAL FIX: If user forgot to update JSON, fallback to heuristic
            if "TODO" not in val:
                return val

        # 2. Heuristic Check (is it already a domain?)
        if re.search(r'\.(ai|io|com|net|org)$', clean_c.lower()):
            return clean_c.lower()

        # 3. Standard Cleaning
        return f"{self._heuristic_clean(clean_c)}.com"

    def _heuristic_clean(self, name):
        c = name.lower()
        c = re.sub(r"\(.*?\)", "", c) # Remove parens e.g. (Salesforce)
        c = self.legal_strip.sub("", c) # Remove Inc/LLC
        c = re.sub(r"[^a-z0-9]", "", c) # Remove spaces/special chars
        return c

class EmailEngine:
    """Generates email options."""
    def generate(self, name_data, domain, rank):
        first = name_data['first']
        last = name_data['last']
        nick = name_data['nick']
        
        if not domain or domain == "unknown.com":
            return []

        options = []
        
        # Priority 1: Nickname (Jennifer "JJ" -> jj.last@)
        if nick:
            if last: options.append(f"{nick}.{last}@{domain}")
            options.append(f"{nick}@{domain}")

        # Priority 2: Founder/C-Suite (First Name Only)
        if rank == 1 and first:
            options.append(f"{first}@{domain}")

        # Priority 3: Standard Corporate
        if first and last:
            options.append(f"{first}.{last}@{domain}")
            options.append(f"{first[0]}{last}@{domain}")

        # Deduplicate preserving order
        return list(dict.fromkeys(options))

# --- 2. THE PIPELINE (Orchestrator) ---

class ExecutivePipeline:
    def __init__(self):
        if not Path(CONFIG_FILE).exists():
            logging.error(f"🛑 Config file {CONFIG_FILE} missing.")
            sys.exit(1)
            
        with open(CONFIG_FILE, 'r') as f:
            self.config = json.load(f)
            
        self.ranker = RankerEngine(self.config)
        self.resolver = DomainResolver(self.config)
        self.emailer = EmailEngine()

    def _parse_name(self, raw_name):
        if pd.isna(raw_name): return {'first': '', 'last': '', 'nick': None}
        
        clean = unidecode(str(raw_name)).strip()
        if clean in ["-", "—", ""]: return {'first': '', 'last': '', 'nick': None}

        nick = None
        # Extract Nickname
        match = re.search(r'\"(.*?)\"', clean)
        if match:
            nick = match.group(1).lower().replace(" ", "")
            clean = clean.replace(f'"{match.group(1)}"', '').replace('  ', ' ')
            
        parts = clean.split()
        first = parts[0].lower() if parts else ""
        last = parts[-1].lower() if len(parts) > 1 else ""
        
        return {'first': first, 'last': last, 'nick': nick}

    def _worker_process_row(self, row):
        """
        Runs on CPU cores. Takes Row -> Returns Series or None.
        """
        # 1. Assess Seniority
        title = unidecode(str(row.get('Title', ''))).strip()
        rank = self.ranker.assess(title)
        
        # Filter non-execs
        if rank == 999: 
            return None

        # 2. Parse Name
        name_data = self._parse_name(row.get('Name'))
        
        # 3. Resolve Domain
        company = row.get('Company', '')
        domain = self.resolver.resolve(company)
        
        # 4. Generate Emails
        emails = self.emailer.generate(name_data, domain, rank)
        
        return pd.Series({
            "Name": row.get('Name'),
            "Title": title,
            "Company": company,
            "Seniority_Level": rank,
            "Email_1": emails[0] if len(emails) > 0 else "",
            "Email_2": emails[1] if len(emails) > 1 else "",
            "Domain_Used": domain
        })

    def run(self):
        logging.info(f"📂 Loading data: {INPUT_FILE}")
        try:
            df = pd.read_csv(INPUT_FILE, on_bad_lines='skip')
            
            # --- HEADER FIX ---
            # Remove Byte Order Mark (BOM) and whitespace from headers
            df.columns = df.columns.str.replace('ï»¿', '').str.strip()
            # ------------------
            
        except FileNotFoundError:
            logging.error("Input CSV not found.")
            return

        logging.info(f"⚡ Spawning Parallel Workers...")
        start_t = time.time()

        # --- PARALLEL PROCESSING ---
        results = df.parallel_apply(self._worker_process_row, axis=1)
        # ---------------------------

        # Check raw results before dropping
        total_rows = len(df)
        
        # Drop excluded rows (Nones)
        results = results.dropna()
        valid_rows = len(results)

        # Sort by Seniority (1 is highest)
        final_df = results.sort_values(by=['Seniority_Level', 'Company']).head(50)

        # Save
        final_df.to_csv(OUTPUT_FILE, index=False)
        
        duration = round(time.time() - start_t, 2)
        logging.info(f"📊 Processed {total_rows} rows.")
        logging.info(f"✅ Found {valid_rows} qualified executives.")
        logging.info(f"💾 Final List ({len(final_df)}) saved to {OUTPUT_FILE}")
        logging.info(f"⏱️  Time taken: {duration}s")
        
        if not final_df.empty:
            print("\n" + final_df[['Name', 'Email_1', 'Seniority_Level']].head().to_string())
        else:
            logging.error("❌ Output is empty! Check: 1. CSV Headers 2. Config exclusion keywords.")

if __name__ == "__main__":
    app = ExecutivePipeline()
    app.run()