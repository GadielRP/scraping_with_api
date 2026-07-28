import requests
import json
import os
import argparse

try:
    from ._runtime import get_api_key, oddspapi_url
except ImportError:
    from _runtime import get_api_key, oddspapi_url

def fetch_bookmakers(folder):
    """Retrieves all available bookmakers and their unique slugs"""
    params = {"apiKey": get_api_key()}
    filename = f"{folder}/bookmakers_list.json"
    
    print("🚀 Fetching available bookmakers...")
    response = requests.get(oddspapi_url("v4/bookmakers"), params=params)
    
    if response.status_code == 200:
        bookmakers = response.json()
        with open(filename, "w") as file:
            json.dump(bookmakers, file, indent=4)
        
        print(f"✅ Success! Saved to {filename}")
        
    else:
        print(f"❌ API Error {response.status_code}: {response.text}")

def main():
    parser = argparse.ArgumentParser(
        description="OddspAPI Bookmaker Discovery Tool. Fetches all available bookmakers and their unique slugs from OddspAPI and saves them to a local JSON file.",
        epilog="Example: python get_bookmakers.py fetch"
    )
    parser.add_argument("action", choices=["fetch"], help="The action to perform. 'fetch' retrieves the list of bookmakers.")
    
    args = parser.parse_args()

    folder = "bookmakers_data"
    if not os.path.exists(folder):
        os.makedirs(folder)

    if args.action == "fetch":
        fetch_bookmakers(folder)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
