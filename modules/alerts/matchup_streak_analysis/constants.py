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


def get_team_group(team_name: str, grouping_method: str) -> Optional[str]:
    """Return the grouping bucket for a team under the requested grouping method."""

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
