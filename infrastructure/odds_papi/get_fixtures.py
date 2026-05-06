import requests
import json
import os
from dotenv import load_dotenv
import argparse
from datetime import datetime
import time

load_dotenv()

base_url = 'https://api.oddspapi.io/'
api_key = os.getenv('ODDSpapi_KEY')

def get_fixtures(sport_id, tournament_id, folder, from_date, to_date):
    """Fetches fixtures based on Sport and Tournament IDs"""
    params = {
        "sportId": sport_id,
        "tournamentId": tournament_id,
        "from": from_date,
        "to": to_date,
        "apiKey": api_key
    }

    # Create a unique filename based on the IDs and date
    filename = f"{folder}/fixtures_s{sport_id}_t{tournament_id}_{from_date}.json"
    
    print(f"🚀 Fetching fixtures for Sport {sport_id}, Tournament {tournament_id}...")
    response = requests.get(f"{base_url}v4/fixtures", params=params)
    
    if response.status_code == 200:
        with open(filename, "w") as file:
            json.dump(response.json(), file, indent=4)
        print(f"✅ Success! Saved to {filename}")
    else:
        print(f"❌ API Error {response.status_code}: {response.text}")

def main():
    # 1. Create the "Argument Parser" (The CLI Engine)
    parser = argparse.ArgumentParser(description="OddspAPI Fixtures Discovery Tool")
    
    # 2. Create "Sub-commands"
    subparsers = parser.add_subparsers(dest="command", help="The command to run")
    
    # Setup the 'fixtures' command
    fixtures_parser = subparsers.add_parser("fetch", help="Get fixtures for a tournament")
    fixtures_parser.add_argument("--sport_id", required=True, help="ID of the sport (e.g. Soccer=10)")
    fixtures_parser.add_argument("--tournament_id", required=True, help="ID of the tournament")
    fixtures_parser.add_argument("--from_date", help="Start date (YYYY-MM-DD)", default=None)
    fixtures_parser.add_argument("--to_date", help="End date (YYYY-MM-DD)", default=None)

    args = parser.parse_args()

    # 4. Shared Setup
    # If no dates provided, use today's date
    today = datetime.now().strftime("%Y-%m-%d")
    from_date = args.from_date if args.from_date else today
    to_date = args.to_date if args.to_date else today

    folder = "fixtures_data"
    if not os.path.exists(folder):
        os.makedirs(folder)

    # 5. Run the logic
    if args.command == "fetch":
        get_fixtures(args.sport_id, args.tournament_id, folder, from_date, to_date)
        # Note: If you loop this, remember to time.sleep(2.1) to respect the 2000ms cooldown!
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
