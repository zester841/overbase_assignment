import pandas as pd
import re
from unidecode import unidecode
from typing import Optional, Tuple, Any

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