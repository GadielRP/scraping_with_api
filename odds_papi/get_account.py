import requests
import json
import os
import argparse
import sys
from datetime import datetime

try:
    from ._runtime import get_api_key, oddspapi_url
except ImportError:
    from _runtime import get_api_key, oddspapi_url

# Ensure UTF-8 output encoding for emojis on Windows
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8')

def fetch_account(folder, date):
    """Retrieves account information for the authenticated user."""
    params = {"apiKey": get_api_key()}
    filename = f"{folder}/account_info_{date}.json"
    
    print("🚀 Fetching account details...")
    response = requests.get(oddspapi_url("v4/account"), params=params)
    
    if response.status_code == 200:
        with open(filename, "w") as file:
            json.dump(response.json(), file, indent=4)
        print(f"✅ Success! Saved to {filename}")
    else:
        print(f"❌ API Error {response.status_code}: {response.text}")

def main():
    parser = argparse.ArgumentParser(
        description="OddspAPI Account Details Tool. Retrieves account information for the authenticated user and saves it to a JSON file.",
        epilog="Example: python get_account.py fetch"
    )
    parser.add_argument("action", choices=["fetch"], help="The action to perform. 'fetch' retrieves account details.")
    args = parser.parse_args()

    today = datetime.now().strftime("%Y-%m-%d")
    folder = "account_data"
    if not os.path.exists(folder):
        os.makedirs(folder)

    if args.action == "fetch":
        fetch_account(folder, today)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
