# SofaScore scripts

Run these commands from the repository root with Python's module form:

```powershell
python -m scripts.sofascore.get_event_details 12345
```

The other scripts use the same pattern, replacing `get_event_details` with
their filename without `.py`. Event-specific scripts interpret `event_id` as a
SofaScore ID by default. Pass `--source canonical` when the input is an
internal canonical event ID.

Every script saves its response as a debug JSON file by default and prints the
absolute file path after the response, so the path can be opened directly from
the terminal output. Use `--no-save-debug` to skip this file. To explicitly
enable it, pass `--save-debug`.

New generic script responses are saved under
`debug/sofascore/scripts/<script-name>/`. Event comments, lineups, and statistics
keep their existing event-specific debug folders.
