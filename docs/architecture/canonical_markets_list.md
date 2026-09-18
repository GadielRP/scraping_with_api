# Canonical Market Types Catalog

Source of truth: [`infrastructure/persistence/catalogs/canonical_market_types.py`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/catalogs/canonical_market_types.py)  
Resolver: [`modules/odds_ingestion/canonical_market_resolver.py`](file:///c:/Users/gadie/Documents/projects/sofascore/modules/odds_ingestion/canonical_market_resolver.py)  
Normalizer: [`modules/odds_ingestion/choice_normalization.py`](file:///c:/Users/gadie/Documents/projects/sofascore/modules/odds_ingestion/choice_normalization.py)  
Database model: `CanonicalMarketType` in [`infrastructure/persistence/models.py`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/models.py)

---

## Overview

The canonical market catalog defines the normalized reference types used across all sports and data providers (SofaScore, OddspAPI, OddsPortal).

* **Total canonical market types:** 39
* **Markets enabled for ingestion:** 15 core market types (`1X2`, `Home/Away`, `Over/Under`, `Asian Handicap`, `Handicap` across full-time, 1st half, 1st-to-5th inning, and OT periods) have `enabled_for_ingestion = True`. All specials, props, team totals, quarters, and 1st periods are set to `False`.
* **Trajectory-tracked markets:** 21 markets have `enabled_for_trajectory = True` for price velocity and line drift analysis (Pillar 4 and trajectory repositories).
* **Renamed keys:** Historical key migrations are managed automatically by `CANONICAL_MARKET_KEY_RENAMES`.

---

## 1. Side Markets (1X2, Home/Away, Sets, Draw No Bet)

| Key | Name | Group | Period | Family | Group Req. | Trajectory | Order |
| :--- | :--- | :--- | :--- | :--- | :---: | :---: | :---: |
| `1x2_full_time` | 1X2 Full Time | 1X2 | Full Time | `side_3way` | No | Yes | 10 |
| `1x2_1st_half` | 1X2 1st Half | 1X2 | 1st Half | `side_3way` | No | Yes | 11 |
| `1x2_1st_quarter` | 1X2 1st Quarter | 1X2 | 1st Quarter | `side_3way` | No | No | 12 |
| `1x2_first_to_fifth_inning` | 1X2 First To Fifth Inning | 1X2 | 1st to 5th Inning | `side_3way` | No | Yes | 13 |
| `home_away_full_time` | Home/Away Full Time | Home/Away | Full Time | `side_2way` | No | Yes | 20 |
| `home_away_1st_half` | Home/Away 1st Half | Home/Away | 1st Half | `side_2way` | No | Yes | 21 |
| `home_away_1st_quarter` | Home/Away 1st Quarter | Home/Away | 1st Quarter | `side_2way` | No | No | 22 |
| `home_away_full_time_including_overtime` | Home/Away Full Time Including Overtime | Home/Away | Full Time Including Overtime | `side_2way` | No | Yes | 23 |
| `first_set_winner_1st_set` | First Set Winner 1st Set | First Set Winner | 1st Set | `side_2way` | No | No | 24 |
| `current_set_winner_current_set` | Current Set Winner Current Set | Current Set Winner | Current Set | `side_2way` | No | No | 25 |
| `home_away_first_to_fifth_inning` | Home/Away First To Fifth Inning | Home/Away | 1st to 5th Inning | `side_2way` | No | Yes | 26 |
| `draw_no_bet_full_time` | Draw No Bet Full Time | Draw No Bet | Full Time | `side_2way` | No | No | 60 |

---

## 2. Totals (Over/Under, Cards, Corners, Sets)

All total markets require `choice_group` to specify the line threshold (e.g. `2.5`, `215.5`, `9.5`).

| Key | Name | Group | Period | Family | Group Req. | Trajectory | Order |
| :--- | :--- | :--- | :--- | :--- | :---: | :---: | :---: |
| `over_under_full_time` | Over/Under Full Time | Over/Under | Full Time | `total` | Yes | Yes | 30 |
| `sets_over_under_full_time` | Sets Over/Under Full Time | Total Sets | Full Time | `total` | Yes | No | 31 |
| `over_under_full_time_including_overtime` | Over/Under Full Time Including Overtime | Over/Under | Full Time Including Overtime | `total` | Yes | Yes | 31 |
| `over_under_1st_half` | Over/Under 1st Half | Over/Under | 1st Half | `total` | Yes | Yes | 32 |
| `over_under_1st_quarter` | Over/Under 1st Quarter | Over/Under | 1st Quarter | `total` | Yes | Yes | 33 |
| `over_under_1st_period` | Over/Under 1st Period | Over/Under | 1st Period | `total` | Yes | No | 34 |
| `total_cards_full_time` | Total Cards Full Time | Total Cards | Full Time | `total` | Yes | No | 34 |
| `total_corners_full_time` | Total Corners Full Time | Total Corners | Full Time | `total` | Yes | No | 35 |
| `total_sets_games_extra_time` | Total Sets/Games Extra Time | Total Sets/Games | Extra Time | `total` | Yes | No | 36 |

> **Note on key renaming:** `total_corners_full_time` replaces the legacy key `corners_2_way_full_time`.

---

## 3. Team Totals

Team totals require `choice_group` for the line threshold (e.g. `1.5`, `105.5`).

| Key | Name | Group | Period | Family | Group Req. | Trajectory | Order |
| :--- | :--- | :--- | :--- | :--- | :---: | :---: | :---: |
| `team_total_home_full_time` | Team Total Home Full Time | Over/Under Team 1 | Full Time | `team_total` | Yes | Yes | 37 |
| `team_total_away_full_time` | Team Total Away Full Time | Over/Under Team 2 | Full Time | `team_total` | Yes | Yes | 38 |
| `team_total_home_full_time_including_overtime` | Team Total Home Full Time Including Overtime | Over/Under Team 1 | Full Time Including Overtime | `team_total` | Yes | No | 39 |
| `team_total_away_full_time_including_overtime` | Team Total Away Full Time Including Overtime | Over/Under Team 2 | Full Time Including Overtime | `team_total` | Yes | No | 40 |

---

## 4. Handicaps / Spreads

Handicap and spread markets require `choice_group` to specify the spread or handicap value (e.g. `-1.5`, `+3.5`).

| Key | Name | Group | Period | Family | Group Req. | Trajectory | Order |
| :--- | :--- | :--- | :--- | :--- | :---: | :---: | :---: |
| `asian_handicap_full_time` | Asian Handicap Full Time | Asian Handicap | Full Time | `spread_2way` | Yes | Yes | 50 |
| `asian_handicap_1st_half` | Asian Handicap 1st Half | Asian Handicap | 1st Half | `spread_2way` | Yes | Yes | 51 |
| `asian_handicap_full_time_including_overtime` | Asian Handicap Full Time Including Overtime | Asian Handicap | Full Time Including Overtime | `spread_2way` | Yes | Yes | 52 |
| `handicap_full_time_including_overtime` | Handicap Full Time Including Overtime | Handicap | Full Time Including Overtime | `spread_2way` | Yes | Yes | 53 |
| `handicap_first_to_fifth_inning` | Handicap First To Fifth Inning | Handicap | 1st to 5th Inning | `spread_2way` | Yes | Yes | 54 |
| `european_handicap_full_time` | European Handicap Full Time | European Handicap | Full Time | `side_3way` | Yes | No | 53 |

---

## 5. Specials, Combinations & Props

| Key | Name | Group | Period | Family | Group Req. | Trajectory | Order |
| :--- | :--- | :--- | :--- | :--- | :---: | :---: | :---: |
| `double_chance_full_time` | Double Chance Full Time | Double Chance | Full Time | `side_combo` | No | No | 61 |
| `both_teams_to_score_full_time` | Both Teams To Score Full Time | Both Teams To Score | Full Time | `decision` | No | No | 62 |
| `both_teams_to_score_full_time_including_overtime` | Both Teams To Score Full Time (Incl. Overtime) | Both Teams To Score | Full Time Including Overtime | `decision` | No | No | 63 |
| `first_goal_full_time` | First Goal Full Time | First Goal | Full Time | `goal_team` | No | No | 70 |
| `last_goal_full_time` | Last Goal Full Time | Last Goal | Full Time | `goal_team` | No | No | 71 |
| `first_team_to_score_full_time` | First Team To Score Full Time | First Team To Score | Full Time | `goal_team` | No | No | 72 |
| `next_goal_full_time` | Next Goal Full Time | Next Goal | Full Time | `goal_team` | No | No | 73 |
| `tie_break_in_match_extra_time` | Tie Break In Match Extra Time | Tie Break In Match | Extra Time | `decision` | No | No | 80 |

---

## Canonical Market Families & Choice Normalization

The ingestion normalizer ([`modules/odds_ingestion/choice_normalization.py`](file:///c:/Users/gadie/Documents/projects/sofascore/modules/odds_ingestion/choice_normalization.py)) standardizes choices according to the `market_family` attribute:

| Market Family | Canonical Choice Roles | Parameterized By `choice_group` | Notes |
| :--- | :--- | :---: | :--- |
| `side_3way` | `1`, `x`, `2` | No (except European Handicap) | 3-way regulation results (Home, Draw, Away). |
| `side_2way` | `1`, `2` | No | 2-way moneyline, match winner, draw no bet, or set winner. |
| `total` | `over`, `under` | Yes | Parameterized by total line value (e.g. `2.5`). |
| `team_total` | `over`, `under` | Yes | Parameterized by team line value (e.g. `1.5`). |
| `spread_2way` | `1`, `2` | Yes | Parameterized by spread value (e.g. `+1.5`, `-2.5`). |
| `side_combo` | `1x`, `12`, `x2` | No | Double chance selections. |
| `decision` | `yes`, `no` | No | Binary outcome propositions (e.g. BTTS, Tie-break). |
| `goal_team` | `1`, `2`, `no_goal` | No | Goal scoring team proposition or scoreless match. |

---

## Key Aliases and Renames

Defined in `CANONICAL_MARKET_KEY_RENAMES` ([`infrastructure/persistence/catalogs/canonical_market_types.py`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/catalogs/canonical_market_types.py)):

```python
CANONICAL_MARKET_KEY_RENAMES = {
    "corners_2_way_full_time": "total_corners_full_time",
}
```

When ingesting or querying historical records, `corners_2_way_full_time` is automatically mapped to `total_corners_full_time`.
