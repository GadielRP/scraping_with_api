import requests
import json
import os
from dotenv import load_dotenv
import argparse
from datetime import datetime

load_dotenv()

base_url = 'https://api.oddspapi.io/'
api_key = os.getenv('ODDSpapi_KEY')

def get_historical_odds(fixture_id, bookmakers, folder, odd_id=None, player_id=None, outcome_id=None, active=None, if_none_match=None):
    """Fetches historical odds for a specific fixture and bookmakers"""
    params = {
        "fixtureId": fixture_id,
        "bookmakers": bookmakers,
        "apiKey": api_key
    }

    if odd_id: params["id"] = odd_id
    if player_id: params["playerId"] = player_id
    if outcome_id: params["outcomeId"] = outcome_id
    if active is not None: params["active"] = active

    headers = {}
    if if_none_match: headers["If-None-Match"] = if_none_match

    safe_bookmakers = bookmakers.replace(",", "_")
    filename = f"{folder}/historical_odds_{fixture_id}_{safe_bookmakers}.json"
    
    print(f"🚀 Fetching historical odds for fixture {fixture_id}...")
    response = requests.get(f"{base_url}v4/historical-odds", params=params, headers=headers)
    
    if response.status_code == 200:
        with open(filename, "w") as file:
            json.dump(response.json(), file, indent=4)
        print(f"✅ Success! Saved to {filename}")
        if 'ETag' in response.headers:
            print(f"ℹ️ ETag received: {response.headers['ETag']}")
    elif response.status_code == 304:
        print("✅ 304 Not Modified: Data has not changed since the last request (ETag matched).")
    else:
        print(f"❌ API Error {response.status_code}: {response.text}")

def main():
    # 1. Create the CLI Parser
    parser = argparse.ArgumentParser(
        description="OddspAPI Historical Odds Tool. Retrieves historical betting odds for a specific fixture across multiple bookmakers.",
        epilog="Example: python get_historical_odds_by_fixture.py fetch --fixture_id 12345 --bookmakers pinnacle,bet365"
    )
    subparsers = parser.add_subparsers(dest="command", help="The command to run")
    
    # 2. Setup the 'fetch' command
    odds_parser = subparsers.add_parser("fetch", help="Get historical odds for a fixture")
    odds_parser.add_argument("--fixture_id", required=True, help="Event ID to retrieve historical odds for")
    odds_parser.add_argument("--bookmakers", required=True, help="Comma-separated list of bookmaker slugs (max 3) (e.g. pinnacle,bet365)")
    odds_parser.add_argument("--id", type=int, help="The unique ID of a specific historical odds entry to filter by")
    odds_parser.add_argument("--player_id", type=int, help="The playerId associated with the odds to narrow down results")
    odds_parser.add_argument("--outcome_id", type=int, help="The outcomeId to filter odds for a specific outcome")
    odds_parser.add_argument("--active", type=lambda x: (str(x).lower() == 'true'), help="Filter based on whether the odds entry is currently active (true/false)")
    odds_parser.add_argument("--if_none_match", help="Echo back the ETag from a previous response to make a conditional request")

    args = parser.parse_args()

    # 3. Create the data folder
    folder = "odds_data"
    if not os.path.exists(folder):
        os.makedirs(folder)

    # 4. Run the logic
    if args.command == "fetch":
        get_historical_odds(
            args.fixture_id, 
            args.bookmakers,
            folder, 
            odd_id=args.id,
            player_id=args.player_id,
            outcome_id=args.outcome_id,
            active=args.active,
            if_none_match=args.if_none_match
        )
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
