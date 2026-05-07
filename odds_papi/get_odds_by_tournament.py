import requests
import json
import os
from dotenv import load_dotenv
import argparse
from datetime import datetime

load_dotenv()

base_url = 'https://api.oddspapi.io/'
api_key = os.getenv('ODDSpapi_KEY')

def get_odds_by_tournaments(tournament_ids, folder, bookmakers=None, odds_format=None, language=None, verbosity=None):
    """Fetches odds for all events in specified tournaments"""
    params = {
        "tournamentIds": tournament_ids,
        "apiKey": api_key
    }

    if bookmakers: params["bookmakers"] = bookmakers
    if odds_format: params["oddsFormat"] = odds_format
    if language: params["language"] = language
    if verbosity: params["verbosity"] = verbosity

    # Create specific tournament folder
    safe_ids = tournament_ids.replace(",", "_")
    tournament_folder = f"{folder}/tournament_{safe_ids}"
    if not os.path.exists(tournament_folder):
        os.makedirs(tournament_folder)
        
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{tournament_folder}/{current_time}_odds.json"
    
    print(f"🚀 Fetching odds for tournaments {tournament_ids}...")
    response = requests.get(f"{base_url}v4/odds-by-tournaments", params=params)
    
    if response.status_code == 200:
        with open(filename, "w") as file:
            json.dump(response.json(), file, indent=4)
        print(f"✅ Success! Saved to {filename}")
    else:
        print(f"❌ API Error {response.status_code}: {response.text}")

def main():
    # 1. Create the CLI Parser
    parser = argparse.ArgumentParser(description="OddspAPI Odds By Tournaments Tool")
    subparsers = parser.add_subparsers(dest="command", help="The command to run")
    
    # 2. Setup the 'fetch' command with all flags
    odds_parser = subparsers.add_parser("fetch", help="Get odds for events in specified tournaments")
    odds_parser.add_argument("--tournament_ids", required=True, help="Comma-separated list of tournament IDs (e.g. 17,18)")
    odds_parser.add_argument("--bookmakers", help="Comma-separated list (e.g. pinnacle,bet365)")
    odds_parser.add_argument("--odds_format", choices=["decimal", "american", "fractional"], help="Desired odds format")
    odds_parser.add_argument("--language", help="Language code (e.g. en, es)")
    odds_parser.add_argument("--verbosity", type=int, help="Level of detail")

    args = parser.parse_args()

    # 3. Create the data folder
    folder = "odds_data"
    if not os.path.exists(folder):
        os.makedirs(folder)

    # 4. Run the logic
    if args.command == "fetch":
        get_odds_by_tournaments(
            args.tournament_ids, 
            folder, 
            bookmakers=args.bookmakers,
            odds_format=args.odds_format,
            language=args.language,
            verbosity=args.verbosity
        )
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
