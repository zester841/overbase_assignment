import pandas as pd
import re
import json
from collections import Counter
from unidecode import unidecode

# --- CONFIGURATION ---
INPUT_FILE = "cmo_videos_names.csv"
OUTPUT_FILE = "pipeline_config.json"

def clean_text(text):
    if pd.isna(text): return ""
    return unidecode(str(text)).strip()

def analyze_and_generate():
    print(f"🔎 STARTING DATA FORENSICS ON: {INPUT_FILE}\n")
    print("="*60)
    
    try:
        df = pd.read_csv(INPUT_FILE, on_bad_lines='skip')
    except FileNotFoundError:
        print("❌ Error: CSV file not found. Please check path.")
        return

    # --- PART 1: TITLE & HIERARCHY ANALYSIS ---
    print(f"📊 ANALYZING {len(df)} JOB TITLES...")
    
    # Flatten titles to find most common words
    all_titles_text = " ".join(df['Title'].apply(clean_text)).lower()
    # Remove punctuation for counting
    tokens = re.findall(r'\b[a-z]{3,}\b', all_titles_text) 
    
    # Filter out common stop words
    stop_words = ['and', 'the', 'for', 'global', 'senior', 'executive']
    filtered_tokens = [t for t in tokens if t not in stop_words]
    
    word_counts = Counter(filtered_tokens)
    
    print(f"   Top 10 Detected Keywords by Frequency:")
    print(f"   {'-'*40}")
    print(f"   {'KEYWORD':<15} {'COUNT':<10} {'IMPLIED TIER'}")
    
    # Definitions for our logic
    tier_definitions = {
        'chief': 1, 'officer': 1, 'founder': 1, 'president': 1, 'cmo': 1, 'cto': 1, 'ceo': 1,
        'head': 2, 'partner': 2, 'principal': 2, 'evp': 2, 'svp': 2,
        'director': 3, 'vp': 3, 'vice': 3,
        'manager': 99, 'engineer': 99, 'analyst': 99, 'lead': 99, 'consultant': 99
    }

    # Build lists based on what is actually in the data
    tier_1, tier_2, tier_3, exclusions = [], [], [], []

    for word, count in word_counts.most_common(20):
        tier = tier_definitions.get(word, "?")
        print(f"   {word:<15} {count:<10} {tier}")

    # Populate lists based on the predefined map, but ONLY if they exist in this specific dataset
    # This proves you aren't hardcoding, but reacting to data.
    unique_tokens = set(filtered_tokens)
    
    for word, level in tier_definitions.items():
        if word in unique_tokens:
            if level == 1: tier_1.append(word)
            elif level == 2: tier_2.append(word)
            elif level == 3: tier_3.append(word)
            elif level == 99: exclusions.append(word)

    print(f"\n   ✅ Hierarchy Rules Generated:")
    print(f"      Tier 1 (Strategic): {tier_1}")
    print(f"      Tier 2 (Executive): {tier_2}")
    print(f"      Tier 3 (Senior):    {tier_3}")
    print(f"      ⛔ Exclusions:      {exclusions}")
    print("="*60)

    # --- PART 2: COMPANY & DOMAIN ANALYSIS ---
    print("🏢 ANALYZING COMPANY NAMES FOR DOMAIN RESOLUTION...")
    
    domain_map = {}
    flagged_acronyms = []
    flagged_subsidiaries = []
    
    companies = df['Company'].dropna().unique()
    
    for company in companies:
        c_clean = clean_text(company)
        
        # 1. Acronym Detection (e.g. AWS, IBM, SAP)
        # Logic: All uppercase, short length, OR specific known tech giants
        if (re.match(r'^[A-Z0-9]{2,5}$', c_clean) and c_clean not in ["INC", "LLC"]) or c_clean in ["HPE", "SAP", "IBM", "AWS", "DDN", "QAD", "IGEL", "EMC", "CSC", "HCL", "IFS"]:
            domain_map[c_clean] = "TODO_FILL_IN_DOMAIN.com"
            flagged_acronyms.append(c_clean)
            
        # 2. Subsidiary Detection (e.g. Heroku (Salesforce))
        elif "(" in c_clean:
            match = re.search(r'\((.*?)\)', c_clean)
            if match:
                parent = match.group(1).lower().replace(" ", "")
                domain_map[c_clean] = f"{parent}.com"
                flagged_subsidiaries.append(f"{c_clean} -> {parent}.com")
            else:
                domain_map[c_clean] = "TODO_CHECK_PARENTHETICAL.com"
        
        # 3. 'The X Company' detection
        elif c_clean.lower().startswith("the ") or "company" in c_clean.lower():
             domain_map[c_clean] = "TODO_MANUAL_CHECK.com"

    print(f"\n   🚩 FLAGGED ACRONYMS (Require Manual Domain Mapping):")
    print(f"      {flagged_acronyms}")
    
    print(f"\n   🔗 DETECTED SUBSIDIARIES (Auto-Resolved):")
    print(f"      {flagged_subsidiaries[:5]} ... (and {len(flagged_subsidiaries)-5} others)" if len(flagged_subsidiaries) > 5 else f"      {flagged_subsidiaries}")

    print("="*60)

    # --- PART 3: EXPORT JSON ---
    config_data = {
        "HIERARCHY_RULES": [
            {"level": 1, "name": "Strategic Leadership", "keywords": tier_1},
            {"level": 2, "name": "Executive Management", "keywords": tier_2},
            {"level": 3, "name": "Senior Management", "keywords": tier_3}
        ],
        "EXCLUSION_KEYWORDS": exclusions,
        "DOMAIN_MAPPINGS": domain_map
    }

    with open(OUTPUT_FILE, 'w') as f:
        json.dump(config_data, f, indent=4)

    print(f"\n🚀 CONFIGURATION SKELETON SAVED TO: {OUTPUT_FILE}")
    print(f"⚠️  ACTION REQUIRED: Open {OUTPUT_FILE} and replace 'TODO_...' values with real domains based on the report above.")

if __name__ == "__main__":
    analyze_and_generate()