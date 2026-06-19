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

def get_fixtures(folder, **kwargs):
    """Fetches fixtures based on API parameters"""
    # Filter out None values
    params = {k: v for k, v in kwargs.items() if v is not None}
    params["apiKey"] = api_key

    # Create a unique filename based on the provided IDs and dates
    parts = []
    if kwargs.get('sportId'): parts.append(f"s{kwargs['sportId']}")
    if kwargs.get('tournamentId'): parts.append(f"t{kwargs['tournamentId']}")
    if kwargs.get('participantId'): parts.append(f"p{kwargs['participantId']}")
    if kwargs.get('from'): parts.append(f"from_{kwargs['from'].replace(':', '').replace('-', '').split('T')[0]}")
    if kwargs.get('to'): parts.append(f"to_{kwargs['to'].replace(':', '').replace('-', '').split('T')[0]}")
    
    if not parts:
        parts.append(datetime.now().strftime("%Y%m%d%H%M%S"))
        
    filename = f"{folder}/fixtures_{'_'.join(parts)}.json"
    
    print(f"🚀 Fetching fixtures with params: { {k:v for k,v in params.items() if k != 'apiKey'} }...")
    response = requests.get(f"{base_url}v4/fixtures", params=params)
    
    if response.status_code == 200:
        with open(filename, "w") as file:
            json.dump(response.json(), file, indent=4)
        print(f"✅ Success! Saved to {filename}")
    else:
        print(f"❌ API Error {response.status_code}: {response.text}")

def main():
    # 1. Create the "Argument Parser" (The CLI Engine)
    parser = argparse.ArgumentParser(
        description="OddspAPI Fixtures Discovery Tool. Fetches upcoming and past fixtures based on tournament, sport, participant, or date filters.",
        epilog="Note: At least one core parameter (tournament_id, participant_id, sport_id, from_date, to_date) must be provided."
    )
    
    # 2. Create "Sub-commands"
    subparsers = parser.add_subparsers(dest="command", help="The command to run")
    
    # Setup the 'fetch' command
    fixtures_parser = subparsers.add_parser("fetch", help="Get fixtures based on various parameters")
    fixtures_parser.add_argument("--tournament_id", type=int, help="The unique identifier for the tournament")
    fixtures_parser.add_argument("--sport_id", type=int, help="The unique identifier for the sport (e.g. Soccer=10)")
    fixtures_parser.add_argument("--participant_id", type=int, help="The unique identifier for the participant")
    fixtures_parser.add_argument("--from_date", dest="from_date", help="Start date in ISO 8601 (e.g., YYYY-MM-DDTHH:MM:SSZ or YYYY-MM-DD)")
    fixtures_parser.add_argument("--to_date", dest="to_date", help="End date in ISO 8601 (e.g., YYYY-MM-DDTHH:MM:SSZ or YYYY-MM-DD)")
    fixtures_parser.add_argument("--language", help="The language in which the tournament information should be returned (e.g. en)")
    fixtures_parser.add_argument("--status_id", type=int, choices=[0, 1, 2, 3], help="0: Not yet started, 1: Live, 2: Finished, 3: Cancelled")
    fixtures_parser.add_argument("--has_odds", choices=['true', 'false'], help="Whether the fixture has odds available")
    fixtures_parser.add_argument("--bookmakers", help="Comma-separated list of bookmaker slugs")

    args = parser.parse_args()

    # 4. Run the logic
    if args.command == "fetch":
        # Condition Validation based on documentation
        if not any([args.tournament_id, args.participant_id, args.sport_id, args.from_date, args.to_date]):
            parser.error("At least one core parameter (tournament_id, participant_id, sport_id, from_date, to_date) must be provided.")
            
        if args.sport_id and not (args.tournament_id or args.participant_id or (args.from_date and args.to_date)):
            parser.error("sport_id must be accompanied by tournament_id, participant_id, or both from_date and to_date.")
            
        if args.from_date and not args.to_date and not (args.tournament_id or args.participant_id):
            parser.error("If only dates are provided (no tournament or participant), both from_date and to_date must be provided.")
            
        if args.to_date and not args.from_date and not (args.tournament_id or args.participant_id):
            parser.error("If only dates are provided (no tournament or participant), both from_date and to_date must be provided.")

        folder = "fixtures_data"
        if not os.path.exists(folder):
            os.makedirs(folder)

        kwargs = {
            "tournamentId": args.tournament_id,
            "sportId": args.sport_id,
            "participantId": args.participant_id,
            "from": args.from_date,
            "to": args.to_date,
            "language": args.language,
            "statusId": args.status_id,
            "hasOdds": args.has_odds,
            "bookmakers": args.bookmakers
        }
        
        get_fixtures(folder, **kwargs)
        # Note: If you loop this, remember to time.sleep(2.1) to respect the 2000ms cooldown!
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
