"""Static team grouping mappings for matchup streak analysis."""

from __future__ import annotations

from typing import Optional

NBA_EASTERN_CONFERENCE = {
    "Atlanta Hawks",
    "Boston Celtics",
    "Brooklyn Nets",
    "Charlotte Hornets",
    "Chicago Bulls",
    "Cleveland Cavaliers",
    "Detroit Pistons",
    "Indiana Pacers",
    "Miami Heat",
    "Milwaukee Bucks",
    "New York Knicks",
    "Orlando Magic",
    "Philadelphia 76ers",
    "Toronto Raptors",
    "Washington Wizards",
}

NBA_WESTERN_CONFERENCE = {
    "Dallas Mavericks",
    "Denver Nuggets",
    "Golden State Warriors",
    "Houston Rockets",
    "Los Angeles Clippers",
    "Los Angeles Lakers",
    "Memphis Grizzlies",
    "Minnesota Timberwolves",
    "New Orleans Pelicans",
    "Oklahoma City Thunder",
    "Phoenix Suns",
    "Portland Trail Blazers",
    "Sacramento Kings",
    "San Antonio Spurs",
    "Utah Jazz",
}

NFL_AFC = {
    "Baltimore Ravens",
    "Buffalo Bills",
    "Cincinnati Bengals",
    "Cleveland Browns",
    "Denver Broncos",
    "Houston Texans",
    "Indianapolis Colts",
    "Jacksonville Jaguars",
    "Kansas City Chiefs",
    "Las Vegas Raiders",
    "Los Angeles Chargers",
    "Miami Dolphins",
    "New England Patriots",
    "New York Jets",
    "Pittsburgh Steelers",
    "Tennessee Titans",
}

NFL_NFC = {
    "Arizona Cardinals",
    "Atlanta Falcons",
    "Carolina Panthers",
    "Chicago Bears",
    "Dallas Cowboys",
    "Detroit Lions",
    "Green Bay Packers",
    "Los Angeles Rams",
    "Minnesota Vikings",
    "New Orleans Saints",
    "New York Giants",
    "Philadelphia Eagles",
    "San Francisco 49ers",
    "Seattle Seahawks",
    "Tampa Bay Buccaneers",
    "Washington Commanders",
}

MLB_AMERICAN_LEAGUE = {
    "Baltimore Orioles",
    "Boston Red Sox",
    "Chicago White Sox",
    "Cleveland Guardians",
    "Detroit Tigers",
    "Houston Astros",
    "Kansas City Royals",
    "Los Angeles Angels",
    "Minnesota Twins",
    "New York Yankees",
    "Athletics",
    "Oakland Athletics",
    "Seattle Mariners",
    "Tampa Bay Rays",
    "Texas Rangers",
    "Toronto Blue Jays",
}

MLB_NATIONAL_LEAGUE = {
    "Arizona Diamondbacks",
    "Atlanta Braves",
    "Chicago Cubs",
    "Cincinnati Reds",
    "Colorado Rockies",
    "Los Angeles Dodgers",
    "Miami Marlins",
    "Milwaukee Brewers",
    "New York Mets",
    "Philadelphia Phillies",
    "Pittsburgh Pirates",
    "San Diego Padres",
    "San Francisco Giants",
    "St. Louis Cardinals",
    "Washington Nationals",
}

NHL_EASTERN_CONFERENCE = {
    "Boston Bruins",
    "Buffalo Sabres",
    "Carolina Hurricanes",
    "Columbus Blue Jackets",
    "Detroit Red Wings",
    "Florida Panthers",
    "Montréal Canadiens",
    "MontrÃ©al Canadiens",
    "New Jersey Devils",
    "New York Islanders",
    "New York Rangers",
    "Ottawa Senators",
    "Philadelphia Flyers",
    "Pittsburgh Penguins",
    "Tampa Bay Lightning",
    "Toronto Maple Leafs",
    "Washington Capitals",
}

NHL_WESTERN_CONFERENCE = {
    "Anaheim Ducks",
    "Calgary Flames",
    "Chicago Blackhawks",
    "Colorado Avalanche",
    "Dallas Stars",
    "Edmonton Oilers",
    "Los Angeles Kings",
    "Minnesota Wild",
    "Nashville Predators",
    "San Jose Sharks",
    "Seattle Kraken",
    "St. Louis Blues",
    "Utah Mammoth",
    "Vancouver Canucks",
    "Vegas Golden Knights",
    "Winnipeg Jets",
}


# Current memberships verified against the league publications linked in
# docs/competition/standings-audit.md. Names match the stored event identities.
_CURRENT_GROUPS = {
    "nba_division": {
        "Atlantic": ("Boston Celtics", "Brooklyn Nets", "New York Knicks", "Philadelphia 76ers", "Toronto Raptors"),
        "Central": ("Chicago Bulls", "Cleveland Cavaliers", "Detroit Pistons", "Indiana Pacers", "Milwaukee Bucks"),
        "Southeast": ("Atlanta Hawks", "Charlotte Hornets", "Miami Heat", "Orlando Magic", "Washington Wizards"),
        "Northwest": ("Denver Nuggets", "Minnesota Timberwolves", "Oklahoma City Thunder", "Portland Trail Blazers", "Utah Jazz"),
        "Pacific": ("Golden State Warriors", "Los Angeles Clippers", "LA Clippers", "Los Angeles Lakers", "Phoenix Suns", "Sacramento Kings"),
        "Southwest": ("Dallas Mavericks", "Houston Rockets", "Memphis Grizzlies", "New Orleans Pelicans", "San Antonio Spurs"),
    },
    "mlb_division": {
        "AL East": ("Baltimore Orioles", "Boston Red Sox", "New York Yankees", "Tampa Bay Rays", "Toronto Blue Jays"),
        "AL Central": ("Chicago White Sox", "Cleveland Guardians", "Detroit Tigers", "Kansas City Royals", "Minnesota Twins"),
        "AL West": ("Houston Astros", "Los Angeles Angels", "Athletics", "Oakland Athletics", "Seattle Mariners", "Texas Rangers"),
        "NL East": ("Atlanta Braves", "Miami Marlins", "New York Mets", "Philadelphia Phillies", "Washington Nationals"),
        "NL Central": ("Chicago Cubs", "Cincinnati Reds", "Milwaukee Brewers", "Pittsburgh Pirates", "St. Louis Cardinals"),
        "NL West": ("Arizona Diamondbacks", "Colorado Rockies", "Los Angeles Dodgers", "San Diego Padres", "San Francisco Giants"),
    },
    "nfl_division": {
        "AFC East": ("Buffalo Bills", "Miami Dolphins", "New England Patriots", "New York Jets"),
        "AFC North": ("Baltimore Ravens", "Cincinnati Bengals", "Cleveland Browns", "Pittsburgh Steelers"),
        "AFC South": ("Houston Texans", "Indianapolis Colts", "Jacksonville Jaguars", "Tennessee Titans"),
        "AFC West": ("Denver Broncos", "Kansas City Chiefs", "Las Vegas Raiders", "Los Angeles Chargers"),
        "NFC East": ("Dallas Cowboys", "New York Giants", "Philadelphia Eagles", "Washington Commanders"),
        "NFC North": ("Chicago Bears", "Detroit Lions", "Green Bay Packers", "Minnesota Vikings"),
        "NFC South": ("Atlanta Falcons", "Carolina Panthers", "New Orleans Saints", "Tampa Bay Buccaneers"),
        "NFC West": ("Arizona Cardinals", "Los Angeles Rams", "San Francisco 49ers", "Seattle Seahawks"),
    },
    "mls_conference": {
        "Eastern": (
            "Atlanta United", "Charlotte FC", "Chicago Fire", "Columbus Crew", "DC United",
            "FC Cincinnati", "Inter Miami CF", "CF Montréal", "Nashville SC",
            "New England Revolution", "New York City FC", "New York Red Bulls",
            "Orlando City SC", "Philadelphia Union", "Toronto FC",
        ),
        "Western": (
            "Austin FC", "Colorado Rapids", "FC Dallas", "Houston Dynamo", "LA Galaxy",
            "Los Angeles FC", "Minnesota United", "Portland Timbers", "Real Salt Lake",
            "San Diego FC", "San Jose Earthquakes", "Seattle Sounders FC",
            "Sporting Kansas City", "St.Louis City", "Vancouver Whitecaps",
        ),
    },
    "ahl_division": {
        "Atlantic": (
            "Charlotte Checkers", "Hartford Wolf Pack", "Hershey Bears",
            "Lehigh Valley Phantoms", "Providence Bruins", "Springfield Thunderbirds",
            "Wilkes Barre-Scranton Penguins", "Wilkes-Barre/Scranton Penguins",
        ),
        "North": (
            "Belleville Senators", "Cleveland Monsters", "Hamilton Hammers", "Laval Rocket",
            "Rochester Americans", "Syracuse Crunch", "Toronto Marlies", "Utica Comets",
        ),
        "Central": (
            "Chicago Wolves", "Grand Rapids Griffins", "Iowa Wild", "Manitoba Moose",
            "Milwaukee Admirals", "Rockford IceHogs", "Texas Stars",
        ),
        "Pacific": (
            "Abbotsford Canucks", "Bakersfield Condors", "Calgary Wranglers",
            "Coachella Valley Firebirds", "Colorado Eagles", "Henderson Silver Knights",
            "Ontario Reign", "San Diego Gulls", "San Jose Barracuda", "Tucson Roadrunners",
        ),
    },
    "argentina_zones": {
        "Group A": (
            "Club Atlético Platense", "Defensa y Justicia", "Central Córdoba", "CA Lanús",
            "Deportivo Riestra", "CA Talleres", "Boca Juniors", "Estudiantes de La Plata",
            "Instituto De Córdoba", "Gimnasia y Esgrima Mendoza", "San Lorenzo",
            "CA Independiente", "Newell's Old Boys", "Club Atlético Unión de Santa Fe",
            "Vélez Sarsfield",
        ),
        "Group B": (
            "Argentinos Juniors", "Aldosivi", "Atlético Tucumán", "Banfield", "Barracas Central",
            "Club Atlético Belgrano", "River Plate", "Gimnasia y Esgrima",
            "Estudiantes de Río Cuarto", "Independiente Rivadavia", "Huracán", "Racing Club",
            "Rosario Central", "Sarmiento", "Tigre",
        ),
    },
}
_CURRENT_GROUP_INDEX = {
    method: {team.casefold(): group for group, teams in groups.items() for team in teams}
    for method, groups in _CURRENT_GROUPS.items()
}


def get_team_group(team_name: str, grouping_method: str) -> Optional[str]:
    """Return the grouping bucket for a team under the requested grouping method."""

    if grouping_method in _CURRENT_GROUP_INDEX:
        return _CURRENT_GROUP_INDEX[grouping_method].get(team_name.casefold())
    if grouping_method == "nba_conference":
        if team_name in NBA_EASTERN_CONFERENCE:
            return "Eastern"
        if team_name in NBA_WESTERN_CONFERENCE:
            return "Western"
    elif grouping_method == "nfl_conference":
        if team_name in NFL_AFC:
            return "AFC"
        if team_name in NFL_NFC:
            return "NFC"
    elif grouping_method == "mlb_league":
        if team_name in MLB_AMERICAN_LEAGUE:
            return "AL"
        if team_name in MLB_NATIONAL_LEAGUE:
            return "NL"
    elif grouping_method == "nhl_conference":
        if team_name in NHL_EASTERN_CONFERENCE:
            return "Eastern"
        if team_name in NHL_WESTERN_CONFERENCE:
            return "Western"
    elif grouping_method == "league_wide":
        return None
    return None
