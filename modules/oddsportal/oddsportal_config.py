"""
Configuration for OddsPortal scraping.
Maps canonical competitions to OddsPortal URL slugs.
"""

import re
import unicodedata
from datetime import date
from typing import Any, Dict, List, Optional, Sequence


def get_current_date() -> date:
    """Return the local calendar date used for OddsPortal cache decisions."""
    from shared.timezone_utils import get_local_now

    return get_local_now().date()

# Provider routing only. This is deliberately separate from the business
# allowlist in modules.competition.tracked_competitions.
ODDSPORTAL_COMPETITION_ROUTES = {
    176: {"sport": "basketball", "country": "usa", "league": "nba"},
    318: {"sport": "football", "country": "brazil", "league": "serie-a"},
    129: {"sport": "baseball", "country": "usa", "league": "mlb"},
    167: {"sport": "football", "country": "spain", "league": "laliga"},
    88: {"sport": "football", "country": "italy", "league": "serie-a"},
    168: {"sport": "football", "country": "england", "league": "premier-league"},
    50: {
        "sport": "football",
        "country": "saudi-arabia",
        "league": "saudi-professional-league",
    },
    171: {"sport": "football", "country": "germany", "league": "bundesliga"},
    172: {"sport": "football", "country": "france", "league": "ligue-1"},
    2192: {"sport": "football", "country": "mexico", "league": "liga-mx"},
    328: {"sport": "football", "country": "mexico", "league": "liga-mx"},
    429: {"sport": "basketball", "country": "europe", "league": "euroleague"},
    525: {"sport": "hockey", "country": "finland", "league": "liiga"},
    526: {"sport": "hockey", "country": "sweden", "league": "shl"},
    527: {"sport": "hockey", "country": "usa", "league": "nhl"},
    317: {"sport": "football", "country": "netherlands", "league": "eredivisie"},
}

# Normalize bookie names to match DB exact names
BOOKIE_ALIASES = {
    "10Bet": "10bet",
    "1xBet": "1xbet",
    "888sport": "888 Sport",
    "bet365": "bet365",
    "Betfair": "Betfair",
    "Betsson": "Betsson",
    "BetVictor": "BetVictor",
    "Betway": "Betway",
    "Bwin": "bwin",
    "Coral": "Coral",
    "Ladbrokes": "Ladbrokes",
    "Marathonbet": "Marathonbet",
    "Pinnacle": "Pinnacle",
    "Unibet": "Unibet",
    "William Hill": "William Hill",
}

# Common institutional suffixes/prefixes and noise tokens to remove during matching
# These are used by TeamMatcher to identify the "strong" part of a team name.
# We only include short suffixes/abbreviations to avoid destroying the team name.
INSTITUTIONAL_NOISE = [
    "fc", "cf", "if", "sc", "ac", "ud", "afc", "rc", "bk", "hk", "hc", "fk", 
    "as", "sd", "cd", "vfb", "vfl", "tsg", "ssv", "hsc", "fsv", "sv", "rb", 
    "spvg", "mtv", "vfr", "tus", "sg", "bsc", "aik", "hif", "dif", "ifk"
]

# Team name fuzzy matching overrides
# Key: The name in OUR database (SofaScore)
# Value: One or more names displayed on OddsPortal
TEAM_ALIASES = {
    # --- Premier League / English Football Teams ---
    "Wolverhampton": ["Wolves", "Wolverhampton"],
    "Manchester United": ["Manchester Utd", "Man United", "Man Utd"],
    "Newcastle United": ["Newcastle"],
    "Nottingham Forest": ["Nottingham"],
    "West Ham United": ["West Ham"],
    "Brighton & Hove Albion": ["Brighton"],
    "Leeds United": ["Leeds"],
    "Tottenham Hotspur": ["Tottenham", "Spurs"],
    "Leicester City": ["Leicester"],
    "Norwich City": ["Norwich"],
    "Ipswich Town": ["Ipswich"],
    "Luton Town": ["Luton"],
    "Sheffield United": ["Sheffield Utd"],
    "West Bromwich Albion": ["West Brom"],
    "Queens Park Rangers": ["QPR"],
    "Blackburn Rovers": ["Blackburn"],
    "Coventry City": ["Coventry"],
    "Stoke City": ["Stoke"],
    "Hull City": ["Hull"],
    "Cardiff City": ["Cardiff"],
    "Swansea City": ["Swansea"],
    "Middlesbrough": ["Middlesbrough"], 
    "Preston North End": ["Preston"],
    "Sheffield Wednesday": ["Sheffield Wed", "Sheffield Weds"],
    "Plymouth Argyle": ["Plymouth"],
    "Birmingham City": ["Birmingham"],
    "Huddersfield Town": ["Huddersfield"],
    "Rotherham United": ["Rotherham"],
    "Sunderland": ["Sunderland"],
    "Watford": ["Watford"],
    "Bristol City": ["Bristol City"],
    "Millwall": ["Millwall"],
    "Southampton": ["Southampton"],

    # --- La Liga / Spanish Football Teams ---
    "Real Betis": ["Betis"],
    "Deportivo Alavés": ["Alaves"],
    "Atlético Madrid": ["Atl. Madrid", "Atletico Madrid"],
    "Athletic Club": ["Ath Bilbao", "Athletic Bilbao"],
    "Girona FC": ["Girona"],
    "Real Oviedo": ["Oviedo"], 
    "Levante UD": ["Levante"],
    "Real Valladolid": ["Valladolid"],
    "Sporting Gijón": ["Gijon"],
    "Leganés": ["Leganes"],
    "Almería": ["Almeria"], 
    "Málaga": ["Malaga"],
    "Real Zaragoza": ["Zaragoza"],
    "Real Racing Club": ["Racing Santander"],
    "Deportivo La Coruña": ["Dep. La Coruna"],
    "Espanyol": ["Espanyol"], 
    "Mallorca": ["Mallorca"],
    "Cádiz": ["Cadiz"],
    "CD Castellón": ["Castellon"],
    "Mirandés": ["Mirandes"],
    "FC Andorra": ["Andorra"],
    "Burgos Club de Fútbol": ["Burgos CF"],
    "AD Ceuta": ["Ceuta"], 
    "Cultural Leonesa": ["Leonesa"],
    "Córdoba": ["Cordoba"],
    "Huesca": ["Huesca"],
    "Eibar": ["Eibar"],
    "Albacete Balompié": ["Albacete"],
    "Racing de Ferrol": ["Ferrol"],
    "Eldense": ["Eldense"],
    "Cartagena": ["Cartagena"],
    "Tenerife": ["Tenerife"],

    # --- Bundesliga / German Football Teams ---
    "FC Bayern München": ["Bayern Munich", "Bayern"],
    "Borussia Dortmund": ["Dortmund"],  
    "TSG Hoffenheim": ["Hoffenheim"],
    "VfB Stuttgart": ["Stuttgart"],
    "Bayer 04 Leverkusen": ["Bayer Leverkusen", "Bayer"], 
    "SC Freiburg": ["Freiburg"],
    "1. FC Union Berlin": ["Union Berlin"],
    "FC Augsburg": ["Augsburg"],
    "1. FC Köln": ["FC Koln"],
    "Borussia M'gladbach": ["B. Monchengladbach", "Monchengladbach"],
    "1. FSV Mainz 05": ["Mainz"],
    "VfL Wolfsburg": ["Wolfsburg"], 
    "SV Werder Bremen": ["Werder Bremen", "Bremen"],
    "FC St. Pauli": ["St. Pauli"], 
    "1. FC Heidenheim": ["Heidenheim"],
    "Hamburger SV": ["Hamburger SV", "Hamburg"],
    "RB Leipzig": ["RB Leipzig", "Leipzig"],
    "Eintracht Frankfurt": ["Eintracht Frankfurt", "Frankfurt"],
    "VfL Bochum": ["Bochum"], 
    "SV Darmstadt 98": ["Darmstadt"],
    "Holstein Kiel": ["Holstein Kiel"],
    "Fortuna Düsseldorf": ["Dusseldorf"],
    "Hannover 96": ["Hannover"],
    "Karlsruher SC": ["Karlsruher"],
    "SC Paderborn 07": ["Paderborn"], 
    "Greuther Fürth": ["Greuther Furth"],
    "Hertha BSC": ["Hertha Berlin", "Hertha"],
    "1. FC Magdeburg": ["Magdeburg"],
    "Schalke 04": ["Schalke"],
    "1. FC Nürnberg": ["Nurnberg"],
    "SSV Jahn Regensburg": ["Regensburg"],
    "SV Elversberg": ["Elversberg"], 
    "VfL Osnabrück": ["Osnabruck"],
    "Hansa Rostock": ["Hansa Rostock"],
    "Eintracht Braunschweig": ["Braunschweig"],
    "1. FC Kaiserslautern": ["Kaiserslautern"],
    "Wehen Wiesbaden": ["Wehen"],

    # --- Serie A / Italian Football Teams ---
    "Hellas Verona": ["Verona"],
    "Milan": ["AC Milan", "Milan"], 
    "Roma": ["AS Roma", "Roma"],
    "Torino": ["Torino"],
    "Udinese": ["Udinese"],
    "Lecce": ["Lecce"],
    "Fiorentina": ["Fiorentina"],
    "Sassuolo": ["Sassuolo"],
    "Lazio": ["Lazio"],
    "Bologna": ["Bologna"],
    "Atalanta": ["Atalanta"],
    "Monza": ["Monza"],
    "Salernitana": ["Salernitana"],
    "Empoli": ["Empoli"],
    "Frosinone": ["Frosinone"],
    "Internazionale": ["Inter"],
    "Inter": ["Inter"],

    # --- League 1 / French Football Teams ---
    "Paris Saint-Germain": ["PSG"],
    "AS Monaco": ["Monaco"],
    "Olympique Lyonnais": ["Lyon"],
    "Olympique Marseille": ["Marseille"],
    "Stade Rennais FC": ["Rennes"],
    "Lille OSC": ["Lille"],
    "RC Strasbourg Alsace": ["Strasbourg"],
    "OGC Nice": ["Nice"],
    "FC Nantes": ["Nantes"],
    "FC Metz": ["Metz"],
    "FC Lorient": ["Lorient"],
    "Stade Brestois 29": ["Brest"],
    "Montpellier HSC": ["Montpellier"],
    "Angers SCO": ["Angers"],
    "AS Saint-Étienne": ["St-Etienne"],
    "Toulouse FC": ["Toulouse"],
    "RC Lens": ["Lens"],
    "Girondins de Bordeaux": ["Bordeaux"],

    # --- Saudi Professional League / Saudi Arabian Football Teams ---
    "Al-Hilal": ["Al Hilal"],
    "Al-Nassr": ["Al Nassr"],
    "Al-Ittihad": ["Al Ittihad"],
    "Al-Ahli": ["Al Ahli"],
    "Al-Shabab": ["Al Shabab"],
    "Al-Fateh": ["Al Fateh"],
    "Al-Raed": ["Al Raed"],
    "Al-Taawoun": ["Al Taawoun"],
    "Al-Fayha": ["Al Fayha"],
    "Al-Khaleej": ["Al Khaleej"],
    "Al-Okhdood": ["Al Okhdood"],
    "Al-Wehda": ["Al Wehda"],
    "Al-Tai": ["Al Tai"],
    "Al-Hazem": ["Al Hazem"],
    "Al-Riyadh": ["Al Riyadh"],
    "Al-Najma": ["Al Najma"],
    "Al-Qadsiah": ["Al Qadsiah"],
    "Al-Jabalain": ["Al Jabalain"],
    "Al-Orobah": ["Al Orobah"],

    # --- CBA / Chinese Basketball Association Teams ---
    "Shanghai Sharks": ["Shanghai"],
    "Guangsha": ["Zhejiang Guangsha"],
    "Guangdong Southern Tigers": ["Guangdong"],
    "Shandong Heroes": ["Shandong"],
    "Shenzhen Leopards": ["Shenzhen"],
    "Qingdao Eagles": ["Qingdao"],
    "Beijing Ducks": ["Beijing"],
    "Flying Leopards": ["Liaoning"],
    "Shanxi Loongs": ["Shanxi Zhongyu"],
    "Ningbo Rockets": ["Ningbo Rockets"],
    "Zhejiang Golden Bulls": ["Zhejiang Chouzhou"],
    "Beijing Royal Fighters": ["Beijing Royal Fighters"],
    "Flying Tigers": ["Xinjiang"],
    "Northeast Tigers": ["Jilin"],
    "Guangzhou": ["Guangzhou"],
    "Tianjin Pioneers": ["Tianjin"],
    "Fujian Sturgeons": ["Fujian"],
    "Nanjing Monkey Kings": ["Nanjing Tongxi"],
    "Jiangsu Dragons": ["Jiangsu Dragons"],
    "Sichuan Blue Whales": ["Sichuan"],

    # --- PFL / Philippines football league Teams ---
    "Taguig FC": ["One Taguig"],
    "DH Cebu FC": ["Cebu FC"],
    "Kaya": ["Kaya"],
    "Manila Digger FC": ["Manila Digger"],
    "Aguilas-UMak": ["Davao Aguilas"],
    "Stallion Laguna": ["Stallion"],
    "Maharlika Manila FC": ["Maharlika"],
    "Tuloy FC": ["Tuloy"],
    "Don Bosco Garelli United": ["Don Bosco Garelli"],
    "Valenzuela PB-Mendiola": ["Mendiola FC 1991"],
    "Philippine Army": ["Philippine Army"],

    # --- SHL / Swedish Hockey League Teams ---
    "Skellefteå AIK": ["Skelleftea"],
    "Frölunda HC": ["Frolunda"],
    "Växjö Lakers": ["Vaxjo"],
    "Rögle BK": ["Rogle"],
    "Brynäs IF": ["Brynas"],
    "Färjestad BK": ["Farjestad"],
    "Luleå HF": ["Lulea"],
    "Malmö Redhawks": ["Malmo", "Malmö"],
    "Djurgårdens IF": ["Djurgarden", "Djurgården"],
    "Örebro HK": ["Orebro"],
    "Timrå IK": ["Timra"],
    "Linköping HC": ["Linkoping"],
    "Leksands IF": ["Leksand"],
    "HV71": ["HV 71"],
}

def _normalize_bookie_identity(value: Any) -> str:
    """Normalize a source or configured bookmaker name for exact matching."""
    text = unicodedata.normalize("NFKD", str(value or ""))
    ascii_text = "".join(char for char in text if not unicodedata.combining(char))
    return re.sub(r"[^a-z0-9]+", "", ascii_text.casefold())


def bookie_name_matches(actual_name: Any, configured_name: Any) -> bool:
    """Match OddsPortal/DB aliases without permissive substring matching."""
    actual_key = _normalize_bookie_identity(actual_name)
    configured_key = _normalize_bookie_identity(configured_name)
    if not actual_key or not configured_key:
        return False
    if actual_key == configured_key:
        return True

    for source_name, canonical_name in BOOKIE_ALIASES.items():
        alias_keys = {
            _normalize_bookie_identity(source_name),
            _normalize_bookie_identity(canonical_name),
        }
        if actual_key in alias_keys and configured_key in alias_keys:
            return True
    return False


def build_bookie_identity_groups(
    configured_names: Sequence[str],
) -> List[List[str]]:
    """Build normalized alias groups for browser-side exact row selection."""
    groups: List[List[str]] = []
    for configured_name in configured_names or ():
        configured_key = _normalize_bookie_identity(configured_name)
        if not configured_key:
            continue

        identity_keys = {configured_key}
        for source_name, canonical_name in BOOKIE_ALIASES.items():
            alias_keys = {
                _normalize_bookie_identity(source_name),
                _normalize_bookie_identity(canonical_name),
            }
            if configured_key in alias_keys:
                identity_keys.update(alias_keys)
        groups.append(sorted(identity_keys))
    return groups


def select_configured_bookies(
    bookies: List[Any],
    configured_names: Optional[Sequence[str]],
    limit: Optional[int],
) -> List[Any]:
    """Select bookies deterministically by allowlist order and optional limit.

    ``configured_names=None`` keeps provider page order and means no allowlist.
    An empty list or a zero limit selects nothing. Each provider row can be
    selected at most once.
    """
    available = list(bookies or [])
    if limit == 0 or (configured_names is not None and not configured_names):
        return []

    if configured_names is None:
        selected = available
    else:
        selected = []
        selected_indexes = set()
        for configured_name in configured_names:
            for index, bookie in enumerate(available):
                if index in selected_indexes:
                    continue
                actual_name = getattr(bookie, "name", bookie)
                if bookie_name_matches(actual_name, configured_name):
                    selected.append(bookie)
                    selected_indexes.add(index)
                    break

    return selected if limit is None else selected[:limit]


from .oddsportal_routes import (
    OP_GROUPS,
    OP_GROUPS_DISPLAY,
    OP_PERIODS,
    SPORT_SCRAPING_ROUTES,
    build_op_fragment,
    build_match_url_with_fragment,
    flatten_sport_scraping_route,
)
