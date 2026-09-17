# SofaScore Challenge Handling

## 1. Diagnosis

The SofaScore API still exists, but some environments now receive useful JSON while others receive an explicit `403` challenge response.

This strongly suggests access conditioning at the edge, likely WAF or Cloudflare risk scoring, rather than a simple schema change in the API.

## 2. Technical Decision

We do not implement an automated bypass.

We do not automate Cloudflare challenge resolution.

We do not extract `cf_clearance`.

Instead, the code now detects challenge responses, stores safe evidence, and degrades in a controlled way so the pipeline can fail clearly without spiraling into useless retries.

## 3. New Components

- `modules/sofascore/challenge.py`
- `SofaScoreChallengeException`
- `logs/sofascore_challenge_evidence.jsonl`
- `scratch/diagnose_sofascore_access_state.py`
- `tests/test_sofascore_challenge.py`
- `tests/test_sofascore_client_challenge.py`

## 4. How to Run the Diagnosis

```bash
python scratch/diagnose_sofascore_access_state.py
```

## 5. How to Run Tests

```bash
pytest tests/test_sofascore_challenge.py tests/test_sofascore_client_challenge.py
```

## 6. Result Interpretation

- `OK_JSON`: the current route is working.
- `CHALLENGE`: the current environment is being challenged.
- `RATE_LIMIT`: a real rate limit, different from challenge.
- `ERROR`: an unclassified failure.
