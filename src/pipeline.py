import pandas as pd
import logging
import time
from typing import Optional
from pandarallel import pandarallel

# Import from our modular structure
from src.config import ConfigLoader
from src.utils import StringUtils
from src.services.ranker import RankerEngine
from src.services.domains import DomainResolver
from src.services.emails import EmailEngine

class Pipeline:
    def __init__(self, config_file: str, input_file: str, output_file: str):
        self.input_file = input_file
        self.output_file = output_file
        
        # 1. Hydrate Config
        raw_conf = ConfigLoader.load_file(config_file)
        
        # 2. Inject Dependencies
        self.ranker = RankerEngine(
            ConfigLoader.parse_rules(raw_conf["HIERARCHY_RULES"]), 
            raw_conf["EXCLUSION_KEYWORDS"]
        )
        self.resolver = DomainResolver(raw_conf["DOMAIN_MAPPINGS"])
        self.emailer = EmailEngine()

    def _worker(self, row) -> Optional[pd.Series]:
        """Parallel worker function"""
        title = StringUtils.sanitize(row.get('Title'))
        
        # Filter
        rank = self.ranker.assess_seniority(title)
        if rank == 999: return None 

        # Enrich
        first, last, nick = StringUtils.extract_nickname(row.get('Name'))
        domain = self.resolver.resolve(row.get('Company'))
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
        logging.info(f"📂 Loading Source: {self.input_file}")
        try:
            df = pd.read_csv(self.input_file, on_bad_lines='skip')
        except FileNotFoundError:
            logging.error("🛑 Input file missing.")
            return

        logging.info(f"🚀 Spawning parallel workers...")
        start_time = time.time()
        
        # Execute
        results = df.parallel_apply(self._worker, axis=1)
        
        # Finalize
        results = results.dropna()
        final_df = results.sort_values(by=['Rank', 'Company']).head(50)
        
        final_df.to_csv(self.output_file, index=False)
        
        duration = round(time.time() - start_time, 2)
        logging.info(f"💾 Success! Saved {len(final_df)} records to {self.output_file}")
        logging.info(f"⏱️  Time: {duration}s")
        print("\n" + final_df[['Name', 'Email_1', 'Rank']].head().to_string())