from typing import List

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

        return list(dict.fromkeys(opts))