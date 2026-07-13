import requests
import json
import os
from dotenv import load_dotenv
import argparse
from datetime import datetime
import sys

# Ensure UTF-8 output encoding for emojis on Windows
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8')

load_dotenv()

base_url = 'https://api.oddspapi.io/'
api_key = os.getenv('ODDSpapi_KEY')

# 🌟 FULL SPORTS LIST (matching get_tournaments.py)
sports_map = {
    "soccer": 10, "basketball": 11, "tennis": 12, "baseball": 13, "american-football": 14,
    "ice-hockey": 15, "esport-dota": 16, "esport-counter-strike": 17, "esport-league-of-legends": 18,
    "darts": 19, "mma": 20, "boxing": 21, "handball": 22, "volleyball": 23, "snooker": 24,
    "table-tennis": 25, "rugby": 26, "cricket": 27, "waterpolo": 28, "futsal": 29,
    "beach-volley": 30, "aussie-rules": 31, "field-hockey": 32, "floorball": 33, "squash": 34,
    "basketball-3x3": 35, "beach-soccer": 36, "pesapallo": 37, "lacrosse": 38, "curling": 39,
    "padel": 40, "bandy": 41, "kabaddi": 42, "rink-hockey": 43, "soccer-specials": 44,
    "gaelic-football": 45, "netball": 46, "beach-handball": 47, "athletics": 48, "badminton": 49,
    "bowls": 50, "cross-country": 51, "gaelic-hurling": 52, "softball": 53, "esoccer": 54,
    "ebasketball": 55, "esport-call-of-duty": 56, "esport-overwatch": 57, "esport-rainbow-six": 58,
    "esport-rocket-league": 59, "esport-starcraft": 60, "esport-valorant": 61, "esport-arena-of-valor": 62,
    "esport-king-of-glory": 63, "judo": 64, "esport-honor-of-kings": 65, "speedway": 66, "golf": 67,
    "cycling": 68, "politics": 69, "elections": 70, "economics": 71, "finance": 72, "technology": 73,
    "health": 74, "science": 75, "cryptocurrency": 76, "weather": 77, "culture": 78
}

# Reverse sports map for filename generation when sport_id is used directly
reverse_sports_map = {v: k for k, v in sports_map.items()}

def save_to_file(response, filename):
    if response.status_code == 200:
        with open(filename, "w") as file:
            json.dump(response.json(), file, indent=4)
        print(f"✅ Success! Saved to {filename}")
    else:
        print(f"❌ API Error {response.status_code}: {response.text}")

def fetch_participants(folder, sport_id, sport_name, date, language=None):
    """Retrieves participants for a given sport ID"""
    params = {
        "sportId": sport_id,
        "apiKey": api_key
    }
    if language:
        params["language"] = language

    # Choose clean name for file
    name_part = sport_name if sport_name else f"sport_{sport_id}"
    filename = f"{folder}/participants_{name_part}_{date}.json"

    print(f"🚀 Fetching participants for {name_part} (ID: {sport_id})...")
    response = requests.get(f"{base_url}v4/participants", params=params)
    
    save_to_file(response, filename)

def main():
    parser = argparse.ArgumentParser(
        description="OddspAPI Participants Discovery Tool. Retrieves participants for a single sport and saves them to a local JSON file.",
        epilog="Examples: python get_participants.py fetch --sport basketball OR python get_participants.py fetch --sport_id 11 --language en"
    )
    subparsers = parser.add_subparsers(dest="command", help="The command to run")
    
    fetch_parser = subparsers.add_parser("fetch", help="Fetch participants for a sport")
    group = fetch_parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--sport", help="Name of the sport (e.g. soccer, basketball)")
    group.add_argument("--sport_id", type=int, help="Numeric ID of the sport (e.g. 10, 11)")
    fetch_parser.add_argument("--language", help="Language code (e.g. en, es)")

    args = parser.parse_args()

    if args.command == "fetch":
        folder = "participants_data"
        if not os.path.exists(folder):
            os.makedirs(folder)

        sport_id = None
        sport_name = None

        if args.sport:
            sport_name_cleaned = args.sport.lower().strip()
            if sport_name_cleaned in sports_map:
                sport_id = sports_map[sport_name_cleaned]
                sport_name = sport_name_cleaned
            else:
                print(f"❌ Error: '{args.sport}' is not in the sports map. Use --sport_id instead.")
                sys.exit(1)
        elif args.sport_id:
            sport_id = args.sport_id
            sport_name = reverse_sports_map.get(sport_id, None)

        today = datetime.now().strftime("%Y-%m-%d")
        fetch_participants(folder, sport_id, sport_name, today, language=args.language)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
