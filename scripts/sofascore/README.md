# SofaScore scripts

Run these commands from the repository root with Python's module form:

```powershell
python -m scripts.sofascore.get_event_details 12345
```

The other scripts use the same pattern, replacing `get_event_details` with
their filename without `.py`. Event-specific scripts interpret `event_id` as a
SofaScore ID by default. Pass `--source canonical` when the input is an
internal canonical event ID.
