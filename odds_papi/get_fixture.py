import requests
import json
import os
from dotenv import load_dotenv
import argparse
import sys

# Ensure UTF-8 output encoding for emojis on Windows
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8')

load_dotenv()

base_url = 'https://api.oddspapi.io/'
api_key = os.getenv('ODDSpapi_KEY')

def get_fixture(fixture_id, folder, language=None):
    """Retrieves a single fixture based on its fixture ID"""
    params = {
        "fixtureId": fixture_id,
        "apiKey": api_key
    }
    if language:
        params["language"] = language

    filename = f"{folder}/fixture_{fixture_id}.json"
    
    print(f"🚀 Fetching fixture details for ID: {fixture_id}...")
    response = requests.get(f"{base_url}v4/fixture", params=params)
    
    if response.status_code == 200:
        with open(filename, "w") as file:
            json.dump(response.json(), file, indent=4)
        print(f"✅ Success! Saved to {filename}")
    else:
        print(f"❌ API Error {response.status_code}: {response.text}")

def main():
    parser = argparse.ArgumentParser(
        description="OddspAPI Single Fixture Discovery Tool. Retrieves details for a single fixture based on its fixture ID.",
        epilog="Example: python get_fixture.py fetch --fixture_id id1000001761301153"
    )
    
    subparsers = parser.add_subparsers(dest="command", help="The command to run")
    
    fetch_parser = subparsers.add_parser("fetch", help="Get details of a single fixture")
    fetch_parser.add_argument("--fixture_id", required=True, help="The unique identifier for the fixture")
    fetch_parser.add_argument("--language", help="Language code for translated labels (e.g. en, es, de)")

    args = parser.parse_args()

    if args.command == "fetch":
        folder = "fixture_data"
        if not os.path.exists(folder):
            os.makedirs(folder)
            
        get_fixture(args.fixture_id, folder, language=args.language)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
