import json
import os

json_path = r'c:\Users\gadie\Documents\projects\sofascore\odds_papi\bookmakers_data\bookmakers_list.json'
output_path = r'c:\Users\gadie\Documents\projects\sofascore\docs\canonical_bookies.md'

with open(json_path, 'r', encoding='utf-8') as f:
    data = json.load(f)

with open(output_path, 'w', encoding='utf-8') as f:
    f.write('# Canonical Bookies\n\n')
    f.write('| bookie_name | bookie_slug |\n')
    f.write('|---|---|\n')
    for d in data:
        f.write(f'| {d["bookmakerName"]} | {d["slug"]} |\n')

print("Markdown file created at:", output_path)
