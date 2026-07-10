"""OddsPapi sport-level ingestion filters."""

from __future__ import annotations

from modules.oddspapi.format_utils import normalize_source_id

# Only these OddsPapi sportIds are mapped/imported from catalogs.
# Keep provider policy here — not in canonical_market_types.py.
ALLOWED_SPORT_IDS = frozenset(
    {
        "10",  # Soccer
        "11",  # Basketball
        "12",  # Tennis
        "13",  # Baseball
        "14",  # American Football
        "15",  # Ice Hockey
        # "16",  # ESport Dota
        # "17",  # ESport Counter-Strike
        # "18",  # ESport League of Legends
        # "19",  # Darts
         "22",  # Handball
         "23",  # Volleyball
        # "24",  # Snooker
         "26",  # Rugby
        # "28",  # Waterpolo
        # "29",  # Futsal
        # "30",  # Beach Volley
        # "31",  # Aussie Rules
        # "32",  # Field hockey
        # "33",  # Floorball
        # "34",  # Squash
        # "35",  # Basketball 3x3
        # "36",  # Beach Soccer
        # "37",  # Pesapallo
        # "38",  # Lacrosse
        # "39",  # Curling
        # "40",  # Padel
        # "41",  # Bandy
        # "42",  # Kabaddi
        # "43",  # Rink Hockey
        # "44",  # Soccer Specials
        # "45",  # Gaelic Football
        # "46",  # Netball
        # "47",  # Beach Handball
        # "48",  # Athletics
        # "49",  # Badminton
        # "50",  # Bowls
        # "51",  # Cross-Country
        # "52",  # Gaelic Hurling
        # "53",  # Softball
        # "54",  # eSoccer
        # "55",  # eBasketball
        # "56",  # ESport Call of Duty
        # "57",  # ESport Overwatch
        # "58",  # ESport Rainbow Six
        # "59",  # ESport Rocket League
        # "60",  # ESport StarCraft
        # "61",  # ESport Valorant
        # "62",  # ESport Arena of Valor
        # "63",  # ESport King of Glory
        # "64",  # Judo
        # "65",  # ESport Honor of Kings
        # "66",  # Speedway
        # "67",  # Golf
        # "68",  # Cycling
    }
)


def is_allowed_sport_id(sport_id) -> bool:
    normalized = normalize_source_id(sport_id)
    if normalized is None:
        return False
    return normalized in ALLOWED_SPORT_IDS
