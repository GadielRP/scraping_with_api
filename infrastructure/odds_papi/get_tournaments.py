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

# 🌟 FULL SPORTS LIST
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

def save_to_file(response, filename):
    if response.status_code == 200:
        with open(filename, "w") as file:
            json.dump(response.json(), file, indent=4)
        print(f"✅ Success! Saved to {filename}")
    else:
        print(f"❌ API Error {response.status_code}: {response.text}")

def fetch_tournaments(sport_name, folder, date):
    """Logic to get tournaments for one sport"""
    sport_name = sport_name.lower().strip()
    
    if sport_name not in sports_map:
        print(f"❌ Error: '{sport_name}' is not in our list.")
        return
    params = {
        "sportId": sports_map[sport_name],
        "apiKey": api_key
        }

    filename = f"{folder}/{sport_name}_{date}.json"
    
    print(f"🚀 Fetching {sport_name} tournaments...")
    response = requests.get(f"{base_url}v4/tournaments", params=params)
    
    save_to_file(response, filename)

def fetch_all_tournaments(folder, date):
    """Logic to fetch tournaments for EVERY sport in our map"""
    print("🌍 STARTING GLOBAL TOURNAMENT FETCH...")
    for sport_name in sports_map.keys():
        fetch_tournaments(sport_name, folder, date)
        time.sleep(1.2) # Respect the rate limit!
    print("✨ GLOBAL FETCH COMPLETE!")

def main():
    parser = argparse.ArgumentParser(description="OddspAPI Tournament Discovery Tool")
    subparsers = parser.add_subparsers(dest="command", help="The command to run")

    # Command: single
    single_parser = subparsers.add_parser("single", help="Get tournaments for one sport")
    single_parser.add_argument("--sport", required=True, help="Name of the sport")

    # Command: all
    subparsers.add_parser("all", help="Fetch tournaments for all sports")

    args = parser.parse_args()

    today = datetime.now().strftime("%Y-%m-%d")
    folder = "tournaments_data"
    
    if not os.path.exists(folder):
        os.makedirs(folder)

    if args.command == "single":
        fetch_tournaments(args.sport, folder, today)
    elif args.command == "all":
        fetch_all_tournaments(folder, today)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
