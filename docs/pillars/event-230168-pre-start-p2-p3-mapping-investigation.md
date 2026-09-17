# Investigación: pre-start Oddspapi, trayectoria y P2/P3 (evento 230168)

Fecha: 2026-09-02  
Evento: `events.id = 230168` — Boston Red Sox vs Seattle Mariners (MLB, `competition_id=129`)  
Fixture Oddspapi: `id1300010963302093`  
Respuesta `/odds` T-5: `debug/oddspapi_odds_responses/230168_Boston_Red_Sox_Seattle_Mariners/230168_id1300010963302093_t_5_odds_pinnacle_bet365_betfair-ex.json`

Este documento resume lo encontrado en la sesión: dónde se recorta Oddspapi, cómo entra la trayectoria a pillars, cómo están partidos P2/P3, y por qué P2 quedó `INSUFFICIENT_DATA` en este partido de baseball.

---

## 1. Gate temporal Oddspapi (T-5)

Oddspapi **no** decide key moments. El plan compartido marca `should_extract_odds` cuando `minutes_until_start` está en `PRE_START_ODDS_MOMENTS` (default `120, 30, 5, 1, 0, -5`). El job regular excluye el minuto de cierre (`PRE_START_CLOSING_ODDS_MINUTE`, default `1`) vía `regular_pre_start_moments()`.

El recorte a **solo T-5** es un gate **local de Oddspapi**, no del orquestador:

- `Config.ODDSPAPI_PRE_START_ALLOWED_MOMENTS` default `[5]`
- `restrict_candidates_to_allowed_moments()` en `modules/jobs/pre_start_check_job/providers/oddspapi/event_selector.py`
- Aplicado en `run_oddspapi_pre_start_odds()` justo después del filtro de competiciones trackeadas

`run_pre_start_check_job.py` **no** debe cambiar `key_moments` a `[5]`. Ese valor es compartido por SofaScore, OddsPortal, alerts y pillars. El loop de providers (líneas 195–204) está bien: pasa un parámetro (`tracked_competition_ids`) y no muta el plan.

Efectos del allowlist `[5]`:

- Oddspapi no fetcha T-120, T-30, T-0, T--5 ni T-1 (si ese job pasa por el mismo phase).
- SofaScore / OddsPortal / evaluation siguen en 120/30/5.
- Revertir: `ODDSPAPI_PRE_START_ALLOWED_MOMENTS=[]` o `120,30,5,0,-5`, y quitar el helper.

---

## 2. Cómo llega la trayectoria a pillars

No hay merge en memoria del payload `/odds` hacia `oddsTrajectory`.

Orden en la misma ejecución de `run_pre_start_check_job`:

1. Ingesta (Oddspapi/SofaScore) persiste quotes y snapshots.
2. `build_event_context(..., odds_trajectory=[])` nace vacío.
3. `_load_trajectory_payloads()` llama `OddsTrajectoryRepository.get_pre_start_trajectory_map()` → `odds_trajectory_query.py`.
4. El resultado se asigna a `context.odds_trajectory`.
5. `pillar_pipeline.py` hace `build_odds_trajectory_context(odds_trajectory)` sobre esa lista.

Los T-5 de **esta** corrida entran si los snapshots se persistieron a tiempo y `minutes_before_start` cae dentro de `PRE_START_ODDS_MOMENT_TOLERANCE_MINUTES`. T-120/T-30 salen de corridas anteriores en DB.

En este caso el query confirmó persistencia:

```
Loaded event-scoped pre-start odds trajectory events_requested=1 events_returned=1 targets=5 rows=28
Pillar odds trajectory context ... available=True market_groups=3 present_minutes=[120, 30, 5] missing_minutes=[1, 0]
Canonical structural target selected ... target_minute=5 ... hardcoded_override ... pre_start_signal_profile
```

`HARDCODED_TARGET_MINUTE_BY_FLOW["pre_start_signal_profile"] = 5` en `market_snapshot_extractor.py` fuerza T-5 aunque haya 120 y 30 en la trayectoria.

`market_groups=3` y `rows=28` implican que **sí había datos canónicos** (p. ej. totals / home-away / algún spread). P2 falló después, en el matching de identidades, no en el load.

---

## 3. Responsabilidades P2 y P3

`pillar_pipeline.py` no extrae variables. Orquesta: contexto, minuto, `calculate_pillar_*`, mining.

| Capa | Módulo | Rol |
|---|---|---|
| Índice | `odds_trajectory_context.py` | Anida filas: mercado → período → bookie → choice → minuto |
| Extractor genérico | `market_snapshot_extractor.py` | Match exacto `(group, period, name)` + bookie + choice; lee precio/size/trace |
| Vocabulario | `pillar_2_side_market/periods.py` / `pillar_3_totals_market_context/periods.py` | Nombres de input e identidades |
| Política | `snapshot_policy.py` | Qué pedir, gates de completeness, arma snapshot |
| Snapshot tipado | `models.py` | Contrato de **inputs extraídos** (no señales) |
| Cálculo | `signal_engine.py` + `metrics.py` / `relations.py` | Edges, gaps, direcciones. **No lee trayectoria** |
| Perfil | `signal_models.py` | DTOs de output (`PIN_EDGE`, `BOOK_RELATION`, …) |
| Orquestación | `run_pillar_2.py` / `run_pillar_3.py` | extract → engine → dict mining |
| Persistencia | `mining/adapters/pillar_2.py` / `pillar_3.py` | Tras el cálculo |

`models.py` es el snapshot crudo (`QuotePoint`, AH/OU, diagnósticos de período, `input_values()` / `input_trace()`). `signal_models.py` es el perfil calculado.

P3 FT ya acepta dos identidades (`Over/Under Full Time` y `Over/Under Full Time Including Overtime`). Por eso P3 pudo calcular con `status=PARTIAL` (FT ok, 1H incompleto). P2 no tiene el equivalente para Home/Away ni para el nombre de AH con extras.

---

## 4. Caso 230168: logs y oferta `/odds`

### 4.1 Resultado de pillars

- **P2:** `INSUFFICIENT_DATA`. `missing` incluye **todo** el vocabulario FT y 1H: 1X2 Pinnacle/Bet365, AH Pinnacle/Bet365, Betfair 1X2 y Betfair AH. `invalid=()` `ambiguous=()`.
- **P3:** `PARTIAL`. FT Pinnacle línea 8.0 OVER, Bet365 línea 8.5 UNDER. 1H vacío. Betfair FT OU `COMPLETE` (`CONVERGENCE_UNDER`).
- **P1:** side y totals OK (no dependen de AH/1X2).

Conclusión: P3 demuestra que Pinnacle/Bet365/Betfair **sí** estaban en trayectoria para totals. El hueco de P2 no es “no hubo odds”, sino “P2 no reconoció los mercados side/spread canónicos de baseball”.

### 4.2 Mercados Oddspapi relevantes (sportId 13)

Catálogo `odds_papi/markets_data/markets_20260623_141927.json`:

| marketId | marketName | period | marketType | handicap |
|---|---|---|---|---|
| 1368 | Handicap (incl. extra innings) | result | spreads | -1.5 |
| 1372 | Handicap (incl. extra innings) | result | spreads | -0.5 |
| 1380 | Handicap (incl. extra innings) | result | spreads | 1.5 |
| 13840 | Handicap First Inning | p1 | spreads | 0 |
| 131381 | Handicap First To Fifth Inning | p1+p2+p3+p4+p5 | spreads | 0 |
| 131 | Winner (incl. extra innings) | result | moneyline | 0 |

En `/odds` T-5, mainLine observado:

- `131381` Pinnacle: 1.72 / 1.96, `mainLine: true` (First To Fifth)
- `1368` Bet365: `-1.5/home` 2.48, `-1.5/away` 1.606, `mainLine: true`
- `1372` Betfair-ex: handicap -0.5 / +0.5, `mainLine: true`, con `exchangeMeta` back/lay
- `1380` Pinnacle: un lado `mainLine: true` (1.444), el otro `active: false` / `mainLine: false`
- `1380` Bet365 alt: `1.5/home` y `1.5/away`, **ambos `mainLine: false`**

MLB no ofrece “Asian Handicap” con ese nombre. Ofrece **Handicap / run line** (`spreads`) y **Winner (incl. extra innings)** (`moneyline` 2-way).

La exclusión deliberada de mapear el nombre genérico `"handicap"` a Asian Handicap (para no mezclar soccer AH con handicap europeo/regular) es real, pero **no es la única causa** de este fallo. El run line FT **ya tiene** un alias en el seed. El fallo de P2 es sobre todo **mismatch de identidad canónica vs lo que P2 busca**.

---

## 5. Tres causas independientes

### Causa 1 — P2 no conoce `Home/Away` (bloquea 1X2 entero)

`Winner (incl. extra innings)` resuelve a `home_away_full_time_including_overtime`:

- `canonical_market_group = "Home/Away"`
- `canonical_market_name = "Home/Away Full Time Including Overtime"`
- `canonical_market_period = "Full Time Including Overtime"`
- familia `side_2way` (home/away, sin empate)

P2 `FULL_TIME_SIDE_SCOPE.one_x_two` solo pide:

```text
MarketIdentity("1X2", "Full Time", "1X2 Full Time")
MarketIdentity("1X2", "Full Time Including Overtime", "1X2 Full Time")
```

El extractor hace match **exacto** de `(group, period, name)`. `"Home/Away"` ≠ `"1X2"` y el name tampoco coincide. Resultado: todos los inputs `PIN_*_1X2_*` y `B365_*_1X2_*` salen `missing`, aunque el moneyline esté en trayectoria.

`1x2_full_time` además **excluye** `moneyline` a propósito (comentario: moneylines 3-way). Baseball FT moneyline **nunca** cae en el grupo `1X2`.

### Causa 2 — AH FT sí está mapeado; P2 busca el nombre corto

El seed `asian_handicap_full_time_including_overtime` **ya incluye**:

```text
"handicap (incl. extra innings)"
```

con `period_lock: "Full Time"` (`result` → Full Time vía `FULL_TIME_PERIOD_ALIASES`).

Tras ingestión el mercado persiste como:

- group: `"Asian Handicap"`
- period: `"Full Time Including Overtime"`
- name: `"Asian Handicap Full Time Including Overtime"`

P2 pide solo:

```text
MarketIdentity("Asian Handicap", period, "Asian Handicap Full Time")
```

para `period in ("Full Time", "Full Time Including Overtime")`. El **período** sí se prueba con extras; el **nombre** no. P3 ya hace lo correcto para OU: lista explícitamente `"Over/Under Full Time Including Overtime"`.

Por eso 1368/1372/1380 pueden estar en DB y P2 igual marca `PIN_AH_*` / `B365_AH_*` / `BF_AH_*` como missing.

### Causa 3 — First To Fifth Inning no llega al resolver de nombre

`Handicap First To Fifth Inning` (`marketId 131381`, `period: p1+p2+p3+p4+p5`):

`resolve_oddspapi_key()` llama `resolve_canonical_period()` **antes** de `market_name`. `period_aliases.py` no tiene `p1+p2+p3+p4+p5`. Retorna `unsupported_period`. El import de catálogo no crea mapping.

Aunque se añadiera el nombre al seed `asian_handicap_1st_half`, sin alias de período el item nunca matchea.

`p1` baseball sí resuelve a `1st Inning` (`Handicap First Inning` / 13840). P2 no tiene scope de 1st Inning; 1H opcional busca `"1st Half"`. First inning no es el equivalente de 1H; **first five innings sí** (convención MLB).

---

## 6. Cómo encaja el mapping (sin implementar)

Cadena:

```
catalog JSON
  → import_oddspapi_catalog_mappings.py
      → resolve_oddspapi_key()
          → resolve_canonical_period()   [period_aliases.py]
          → oddspapi_match en CANONICAL_MARKET_TYPE_SEEDS
      → upsert mapping DB
  → adapter de ingestión usa mapping → markets / quotes / snapshots
  → trajectory query
  → OddsTrajectoryContext
  → P2/P3 snapshot_policy + MarketIdentity exacta
```

Dos capas distintas:

1. **Catálogo / resolver:** ¿este `marketId` Oddspapi se persiste y con qué key canónica?
2. **P2 identities:** ¿el extractor encuentra ese `(group, period, name)` en `periods.py`?

Causa 3 es capa 1. Causas 1 y 2 son capa 2 (el moneyline y el run line FT ya tienen seeds; P2 no los pide con esos nombres).

No conviene mapear el string genérico `"handicap"` a `asian_handicap_full_time`: chocaría con soccer AH vs handicap europeo/regular. El alias ya existe y es **específico**: `"handicap (incl. extra innings)"` → seed OT/extras. El hueco de 1H es `"handicap first to fifth inning"` + período compuesto.

---

## 7. Diseño de arreglo acordado (aún no implementado)

No generalizar el extractor compartido. La equivalencia 1X2 ↔ Home/Away y Full Time ↔ Including Overtime es dominio de **side market**. El extractor sigue match exacto.

Orden:

1. **P2 Home/Away:** en `pillar_2_side_market/periods.py`, helper local que genere identidades `1X2` y `Home/Away` (nombres `1X2 …` y `Home/Away …`) para FT (ambos períodos) y 1H. `_read_quote` ya usa choices `"1"`/`"2"`; no exige empate.
2. **P2 AH nombre OT:** segunda identidad `"Asian Handicap Full Time Including Overtime"` en `FULL_TIME_SIDE_SCOPE.asian_handicap`, mismo patrón que P3 OU.
3. **First five innings:**  
   - `period_aliases.py`: `p1+p2+p3+p4+p5` + `sport_id == "13"` → `"1st Half"`.  
   - Seed existente `asian_handicap_1st_half`: añadir `"handicap first to fifth inning"`.  
   - No crear canonical key nueva. `FIRST_HALF_SIDE_SCOPE.asian_handicap` ya apunta a `"Asian Handicap 1st Half"`.  
   - Reimportar catálogo Oddspapi para persistir el mapping.

Efecto esperado en 230168 tras 1+2 (y re-ingesta si hace falta): P2 debería ver moneyline Home/Away y run line AH FT. First five innings requiere 3 + import. 1H 1X2 solo si existe moneyline first-five mapeado a `home_away_1st_half` (el seed moneyline 1H no restringe `market_name`).

Fuera de alcance inmediato: `Handicap First Inning` (1st Inning, no es 1H); líneas no-mainLine; P4/P5.

---

## 8. Archivos clave

| Área | Path |
|---|---|
| Gate T-5 Oddspapi | `modules/jobs/pre_start_check_job/providers/oddspapi/odds_phase.py` |
| Allowlist | `infrastructure/settings/config.py` (`ODDSPAPI_PRE_START_ALLOWED_MOMENTS`) |
| Query trayectoria | `infrastructure/persistence/repositories/odds_trajectory_query.py` |
| Load + attach | `modules/jobs/pre_start_check_job/key_moment_evaluation.py` |
| Orquestación pillars | `modules/jobs/pre_start_check_job/pillar_pipeline.py` |
| Extractor | `modules/pillars/market_snapshot_extractor.py` |
| Identidades P2 | `modules/pillars/pillar_2_side_market/periods.py` |
| Identidades P3 | `modules/pillars/pillar_3_totals_market_context/periods.py` |
| Seeds | `infrastructure/persistence/catalogs/canonical_market_types.py` |
| Resolver | `modules/odds_ingestion/canonical_market_resolver.py` |
| Períodos Oddspapi | `modules/oddspapi/period_aliases.py` |
| Import mappings | `scripts/maintenance/import_oddspapi_catalog_mappings.py` |
