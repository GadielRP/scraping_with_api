import argparse
import json

from modules.sofascore import api_client


def main() -> None:
    parser = argparse.ArgumentParser(description="Get event comments by event ID")
    parser.add_argument("event_id", type=int, help="The SofaScore ID of the event")
    args = parser.parse_args()

    response = api_client.get_event_comments(args.event_id)
    print(json.dumps(response, indent=4))


if __name__ == "__main__":
    main()
