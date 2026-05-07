import requests
import json
import os
from dotenv import load_dotenv
import argparse
from datetime import datetime

load_dotenv()

base_url = 'https://api.oddspapi.io/'
api_key = os.getenv('ODDSpapi_KEY')

def get_odds(fixture_id, folder, bookmakers=None, odds_format=None, language=None, verbosity=None, market_ids=None):
    """Fetches full odds with optional filters"""
    params = {
        "fixtureId": fixture_id,
        "apiKey": api_key
    }

    if bookmakers: params["bookmakers"] = bookmakers
    if odds_format: params["oddsFormat"] = odds_format
    if language: params["language"] = language
    if verbosity: params["verbosity"] = verbosity
    if market_ids: params["marketIds"] = market_ids

    filename = f"{folder}/odds_{fixture_id}.json"
    
    print(f"🚀 Fetching full odds for fixture {fixture_id}...")
    response = requests.get(f"{base_url}v4/odds", params=params)
    
    if response.status_code == 200:
        with open(filename, "w") as file:
            json.dump(response.json(), file, indent=4)
        print(f"✅ Success! Saved to {filename}")
    else:
        print(f"❌ API Error {response.status_code}: {response.text}")

def main():
    # 1. Create the CLI Parser
    parser = argparse.ArgumentParser(description="OddspAPI Odds Discovery Tool")
    subparsers = parser.add_subparsers(dest="command", help="The command to run")
    
    # 2. Setup the 'fetch' command with all flags
    odds_parser = subparsers.add_parser("fetch", help="Get odds for a single fixture")
    odds_parser.add_argument("--fixture_id", required=True, help="The unique ID of the fixture")
    odds_parser.add_argument("--bookmakers", help="Comma-separated list (e.g. pinnacle,bet365)")
    odds_parser.add_argument("--odds_format", choices=["decimal", "american", "fractional"], help="Desired odds format")
    odds_parser.add_argument("--language", help="Language code (e.g. en, es)")
    odds_parser.add_argument("--verbosity", type=int, help="Level of detail (1, 2, or 3)")
    odds_parser.add_argument("--market_ids", help="Comma-separated market IDs (e.g. 1,12,45)")

    args = parser.parse_args()

    # 3. Create the data folder
    folder = "odds_data"
    if not os.path.exists(folder):
        os.makedirs(folder)

    # 4. Run the logic
    if args.command == "fetch":
        get_odds(
            args.fixture_id, 
            folder, 
            bookmakers=args.bookmakers,
            odds_format=args.odds_format,
            language=args.language,
            verbosity=args.verbosity,
            market_ids=args.market_ids
        )
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
