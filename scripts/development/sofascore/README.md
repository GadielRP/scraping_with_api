# SofaScore scripts

Run these commands from the repository root with Python's module form:

```powershell
python -m scripts.sofascore.get_event_details 12345
```

The other scripts use the same pattern, replacing `get_event_details` with
their filename without `.py`. Event-specific scripts interpret `event_id` as a
SofaScore ID by default. Pass `--source canonical` when the input is an
internal canonical event ID.

Every script saves its response as a debug JSON file by default using
`SofaScoreDebugResponseWriter` and prints the absolute file path after the response,
so the path can be opened directly from the terminal output. Use `--no-save-debug`
to skip this file. To explicitly enable it, pass `--save-debug`.

Responses are saved into dedicated folders under `debug/sofascore/`:
- `events_responses/`: Event details
- `results_responses/`: Event results
- `odds_responses/`: Event odds
- `winning_odds_responses/`: Event winning odds
- `comments_responses/`: Event comments
- `lineups_responses/`: Event lineups
- `statistics_responses/`: Event statistics
- `standings_responses/`: Tournament standings
- `team_last_results_responses/`: Team results
- `team_streaks_responses/`: Team streaks
- `team_nearest_event_responses/`: Nearest team events
- `h2h_events_responses/`: Event H2H
- `h2h_all_responses/`: Global H2H
- `today_events_responses/`: Scheduled day events
- `live_events_responses/`: Live sport events
- `scheduled_odds_responses/`: Scheduled odds by sport
- `dropping_odds_responses/`: Dropping odds
- `high_value_streaks_responses/`: High value streaks
- `winning_odds_discovery_responses/`: Winning odds discovery
