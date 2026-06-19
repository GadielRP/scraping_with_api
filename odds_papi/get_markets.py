import requests
import json
import os
from dotenv import load_dotenv
import argparse
from datetime import datetime

load_dotenv()

base_url = 'https://api.oddspapi.io/'
api_key = os.getenv('ODDSpapi_KEY')

def get_markets(folder, language=None):
    """Fetches a list of markets available in the system"""
    params = {
        "apiKey": api_key
    }

    if language: params["language"] = language

    # Generate a timestamped filename
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join(folder, f"markets_{current_time}.json")
    
    print("🚀 Fetching markets...")
    response = requests.get(f"{base_url}v4/markets", params=params)
    
    if response.status_code == 200:
        with open(filename, "w") as file:
            json.dump(response.json(), file, indent=4)
        print(f"✅ Success! Saved to {filename}")
    else:
        print(f"❌ API Error {response.status_code}: {response.text}")

def main():
    # 1. Create the CLI Parser
    parser = argparse.ArgumentParser(
        description="OddspAPI Markets Tool. Fetches a complete list of betting markets supported by the API, with optional localization.",
        epilog="Example: python get_markets.py fetch --language es"
    )
    subparsers = parser.add_subparsers(dest="command", help="The command to run")
    
    # 2. Setup the 'fetch' command
    markets_parser = subparsers.add_parser("fetch", help="Get a list of available markets")
    markets_parser.add_argument("--language", help="Language code (e.g. en, es)")

    args = parser.parse_args()

    # 3. Create the data folder
    folder = "markets_data"
    if not os.path.exists(folder):
        os.makedirs(folder)

    # 4. Run the logic
    if args.command == "fetch":
        get_markets(
            folder, 
            language=args.language
        )
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
