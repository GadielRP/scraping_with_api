import json
from pathlib import Path

# Read the file (without open)
data = Path("dropping_odds.json").read_text(encoding="utf-8")
response = json.loads(data)

# Print nicely formatted JSON
print(json.dumps(response, indent=4, ensure_ascii=False))
