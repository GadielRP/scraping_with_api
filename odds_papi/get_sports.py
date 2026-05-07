import requests
import json
import os
from dotenv import load_dotenv
import argparse
from datetime import datetime

load_dotenv()

base_url = 'https://api.oddspapi.io/'
api_key = os.getenv('ODDSpapi_KEY')

def save_to_file(response, filename):
    if response.status_code == 200:
        with open(filename, "w") as file:
            json.dump(response.json(), file, indent=4)
        print(f"✅ Success! Saved to {filename}")
    else:
        print(f"❌ API Error {response.status_code}: {response.text}")

def fetch_sports(folder, date):
    params = {"apiKey": api_key}
    filename = f"{folder}/all_sports_{date}.json"
    print("🚀 Fetching all available sports...")
    response = requests.get(f"{base_url}v4/sports", params=params)
    save_to_file(response, filename)

def main():
    parser = argparse.ArgumentParser(description="OddspAPI Sports Discovery Tool")
    parser.add_argument("action", choices=["fetch"], help="The action to perform")
    args = parser.parse_args()

    today = datetime.now().strftime("%Y-%m-%d")
    folder = "sports_data"
    if not os.path.exists(folder):
        os.makedirs(folder)

    if args.action == "fetch":
        fetch_sports(folder, today)

if __name__ == "__main__":
    main()
