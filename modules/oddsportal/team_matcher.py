import unicodedata
import re
import logging
from difflib import SequenceMatcher
from typing import List, Set, Dict, Optional

logger = logging.getLogger(__name__)

class TeamMatcher:
    # Common institutional suffixes/prefixes across different sports/languages
    INSTITUTIONAL_NOISE = {
        "fc", "cf", "if", "sc", "ac", "ud", "afc", "rc", "bk", "hk", "hc", "fk", 
        "as", "sd", "cd", "vfb", "vfl", "tsg", "ssv", "hsc", "fsv", "sv", "rb", 
        "spvg", "mtv", "vfr", "tus", "sg", "bsc", "aik", "hif", "dif", "ifk"
    }

    def __init__(self, team_aliases: Dict[str, List[str]] = None, noise_list: List[str] = None):
        """
        :param team_aliases: Dictionary mapping SofaScore name -> List of OddsPortal names
        :param noise_list: List of institutional noise tokens
        """
        self.team_aliases = team_aliases or {}
        if noise_list:
            self.INSTITUTIONAL_NOISE = set(noise_list)
        
        # Pre-normalize aliases for faster lookup
        self.norm_aliases = {}
        for sofa_name, op_names in self.team_aliases.items():
            if isinstance(op_names, str):
                op_names = [op_names]
            
            s_norm = self.normalize(sofa_name)
            self.norm_aliases[s_norm] = [self.normalize(n) for n in op_names]

    @staticmethod
    def normalize(text: str) -> str:
        """
        Lowercase, remove diacritics, remove special chars, collapse spaces.
        """
        if not text:
            return ""
        # Remove diacritics
        text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('utf-8')
        text = text.lower()
        # Remove special characters but keep alphanumeric and spaces
        text = re.sub(r'[^a-z0-9\s]', ' ', text)
        # Collapse multiple spaces
        return " ".join(text.split())

    def tokenize(self, name: str) -> List[str]:
        """Split into tokens and remove institutional noise."""
        tokens = self.normalize(name).split()
        return [t for t in tokens if t not in self.INSTITUTIONAL_NOISE]

    def generate_variants(self, name: str) -> Set[str]:
        """
        Generate multiple forms of a name for comparison.
        """
        norm_name = self.normalize(name)
        variants = {norm_name}
        
        tokens = norm_name.split()
        strong_tokens = [t for t in tokens if t not in self.INSTITUTIONAL_NOISE]
        
        if strong_tokens:
            # Full name without noise
            variants.add(" ".join(strong_tokens))
            # First strong token (e.g. "Djurgårdens IF" -> "djurgardens")
            variants.add(strong_tokens[0])
            # First two strong tokens
            if len(strong_tokens) > 1:
                variants.add(" ".join(strong_tokens[:2]))
        
        return variants

    def get_score(self, query: str, candidate: str) -> float:
        """
        Calculate a similarity score between 0 and 100.
        """
        # 1. Normalization
        q_norm = self.normalize(query)
        c_norm = self.normalize(candidate)
        
        # 2. Exact normalized match
        if q_norm == c_norm:
            return 100.0

        # 3. Alias match
        aliases = self.norm_aliases.get(q_norm, [])
        if c_norm in aliases:
            logger.debug(f"🎯 TeamMatcher: Alias hit! '{query}' (norm: '{q_norm}') matches '{candidate}' (norm: '{c_norm}')")
            return 98.0
            
        # 4. Jaccard fuzzy match (Token based)
        q_tokens = self.tokenize(q_norm)
        c_tokens = self.tokenize(c_norm)
        
        if not q_tokens or not c_tokens:
            return 0.0
            
        intersection = set(q_tokens).intersection(set(c_tokens))
        union = set(q_tokens).union(set(c_tokens))
        jaccard = (len(intersection) / len(union)) * 100
        
        # 5. Containment (one is inside the other)
        q_combined = "".join(q_tokens)
        c_combined = "".join(c_tokens)
        if q_combined and c_combined:
            if q_combined in c_combined or c_combined in q_combined:
                jaccard = max(jaccard, 85.0)
            
        # 6. SequenceMatcher (Character based)
        ratio = SequenceMatcher(None, q_norm, c_norm).ratio() * 100
        
        return max(jaccard, ratio)

    def find_best_match(self, query_home: str, query_away: str, candidates: List[Dict]) -> Optional[Dict]:
        """
        candidates: List of { "home": str, "away": str, "href": str, ... }
        Returns the best candidate if it exceeds thresholds.
        """
        scored_candidates = []
        for cand in candidates:
            # Direct score
            score_h = self.get_score(query_home, cand['home'])
            score_a = self.get_score(query_away, cand['away'])
            direct_total = score_h + score_a
            
            # Reverse score (in case they are swapped)
            score_rh = self.get_score(query_home, cand['away'])
            score_ra = self.get_score(query_away, cand['home'])
            reverse_total = score_rh + score_ra
            
            scored_candidates.append({
                **cand,
                "direct_score": direct_total,
                "reverse_score": reverse_total,
                "max_score": max(direct_total, reverse_total),
                "is_reversed": reverse_total > direct_total
            })

        # Sort by max score descending
        scored_candidates.sort(key=lambda x: x['max_score'], reverse=True)
        
        if not scored_candidates:
            return None

        best = scored_candidates[0]
        
        # Minimum threshold for a match (e.g. 150/200 total)
        # 160 means roughly 80% match on both teams
        if best['max_score'] < 150:
            return None

        # Check if it's clearly better than the reverse
        # If it's reversed but the difference is small, it might be ambiguous
        if best['is_reversed']:
            if best['reverse_score'] - best['direct_score'] < 20:
                # Ambiguous
                return None
        else:
            if best['direct_score'] - best['reverse_score'] < 20:
                # Ambiguous
                return None

        return best
