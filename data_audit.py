import pandas as pd
import re
from collections import Counter
from unidecode import unidecode

# Load the dirty data
FILE_PATH = "cmo_videos_names.csv"

def audit_data():
    print("🕵️‍♂️ STARTING DATA FORENSICS AUDIT...\n")
    
    try:
        df = pd.read_csv(FILE_PATH, on_bad_lines='skip')
    except FileNotFoundError:
        print("❌ File not found.")
        return

    # Standardize types for analysis
    df = df.fillna('')
    df['Title'] = df['Title'].astype(str)
    df['Company'] = df['Company'].astype(str)
    df['Name'] = df['Name'].astype(str)

    # --- 1. TITLE KEYWORD EXTRACTION ---
    print("--- 📊 ANALYSIS 1: MOST COMMON TITLE WORDS ---")
    print("Goal: Identify keywords for Tier 1 (C-Suite) vs Exclusion (Managers)")
    
    # Flatten all titles into a single list of words, lowercase, remove punctuation
    all_text = " ".join(df['Title']).lower()
    # Remove junk characters to isolate words
    clean_text = re.sub(r'[^a-z\s]', '', all_text)
    words = clean_text.split()
    
    # Filter out boring stop words
    stop_words = {'of', 'and', 'the', '&', '-', 'for', 'in', 'sr', 'global', 'vice'}
    filtered_words = [w for w in words if w not in stop_words and len(w) > 1]
    
    # Count frequency
    counts = Counter(filtered_words).most_common(20)
    
    print(f"{'WORD':<15} {'COUNT':<10} {'DECISION'}")
    print("-" * 40)
    for word, count in counts:
        decision = "✅ KEEP"
        if word in ['manager', 'analyst', 'engineer', 'lead']:
            decision = "❌ EXCLUDE RULE"
        elif word in ['chief', 'officer', 'president', 'founder', 'cmo', 'ceo']:
            decision = "⭐ TIER 1 RULE"
        print(f"{word:<15} {count:<10} {decision}")
    print("\n")

    # --- 2. COMPANY NAME ANOMALIES ---
    print("--- 🏢 ANALYSIS 2: COMPANY NAME EDGE CASES ---")
    print("Goal: Find companies that need Domain Mapping or Cleaning")
    
    # Check 1: Acronyms (e.g. AWS, IBM)
    acronyms = df[df['Company'].str.match(r'^[A-Z]{2,4}$', na=False)]['Company'].unique()
    print(f"\n[!] Detected Acronyms (Require Manual Mapping): {list(acronyms)}")
    
    # Check 2: Subsidiaries / Parentheticals
    subsidiaries = df[df['Company'].str.contains(r'\(', na=False)]['Company'].unique()
    print(f"[!] Detected Subsidiaries (Require Regex Cleaning): {list(subsidiaries)}")
    
    # Check 3: Companies that are already Domains
    domains = df[df['Company'].str.contains(r'\.(io|ai|com|net)', na=False)]['Company'].unique()
    print(f"[!] Detected Pre-existing Domains (Require Bypass Logic): {list(domains)}")
    print("\n")

    # --- 3. NAME PATTERN DETECTION ---
    print("--- 👤 ANALYSIS 3: NAME FORMATTING ---")
    print("Goal: Detect Nicknames for 'Out of the Box' Email Permutations")
    
    # Check for quotes in names (e.g. "JJ")
    nicknames = df[df['Name'].str.contains(r'\"', na=False)]['Name'].unique()
    
    if len(nicknames) > 0:
        print(f"[!] Detected Nicknames in Quotes: {len(nicknames)} instances")
        print(f"    Example: {nicknames[0]}")
        print("    -> ACTION: Implement Regex Parser to extract nickname.")
    else:
        print("    No nicknames detected.")
    
    print("\n✅ AUDIT COMPLETE. PIPELINE RULES GENERATED.")

if __name__ == "__main__":
    audit_data()