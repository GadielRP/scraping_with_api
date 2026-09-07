# Cobertura por bookie en P2 y P3

## Comportamiento

La unidad de cobertura es **bookie + periodo + minuto seleccionado**. La falta de
cobertura, un precio inválido o un contrato ambiguo de una casa no descartan las
lecturas válidas de las demás. No se completan contratos combinando casas, líneas,
proveedores ni minutos diferentes.

| Pilar | Bookie completo en Full Time | Papel de Betfair |
| --- | --- | --- |
| P2 Side | Pinnacle o bet365: 1X2/Home-Away completo y AH **o** Handicap completo de esa misma casa | BACK y LAY completos del mercado de lado; puede habilitar el perfil por sí solo |
| P3 Totals | Pinnacle o bet365: línea, Over y Under completos | O/U BACK y LAY completos, con la misma línea y periodo; puede habilitar un perfil parcial por sí solo |

Full Time sigue siendo necesario: al menos un bookie debe estar completo allí.
First Half no sustituye datos faltantes de Full Time. Los bookies disponibles
pueden ser distintos en Full Time y First Half.

En P2, cada casa puede usar su propia alternativa de spread: Pinnacle AH y bet365
Handicap cuentan como dos casas completas. Las familias conservan su identidad.
Para una comparación entre casas se prefiere una familia común completa: AH,
después Handicap. Se conservan las lecturas granulares de 1H existentes.

## Estados y contrato de salida

- `COMPLETE`: cobertura completa de los bookies requeridos por el periodo.
- `PARTIAL`: existe al menos un bookie completo, pero falta cobertura requerida.
- `AMBIGUOUS`, `INVALID`, `INCOMPLETE`: ningún bookie completo; se conserva el motivo.
- Estado del pilar: `ACTIVE` si FT y 1H cumplen su cobertura requerida; `PARTIAL`
  si FT es utilizable y falta cobertura; `INSUFFICIENT_DATA` sin FT utilizable.

Betfair O/U sigue siendo opcional para que P3 sea `ACTIVE`. Su ausencia no degrada
dos casas de apuestas completas. Sus errores se reportan en `exchange_ou` y
`exchange_ou_1h`, y en su diagnóstico local de bookie.

Ambos pilares usan el mismo contrato en `PERIODS` y `raw.periods`:

```json
{
  "full_time": {
    "status": "PARTIAL",
    "missing_inputs": ["PIN_FT_UNDER_ODDS"],
    "invalid_inputs": [],
    "ambiguous_inputs": [],
    "available_bookies": ["bet365"],
    "bookies": {
      "pinnacle": {
        "status": "INCOMPLETE",
        "missing_inputs": ["PIN_FT_UNDER_ODDS"],
        "invalid_inputs": [],
        "ambiguous_inputs": []
      },
      "bet365": {
        "status": "COMPLETE",
        "missing_inputs": [],
        "invalid_inputs": [],
        "ambiguous_inputs": []
      }
    }
  }
}
```

El ejemplo omite el bookie opcional Betfair por brevedad. Los campos existentes
`P2_STATUS`/`P3_STATUS`, `P2_SIGNAL_PROFILE`/`P3_SIGNAL_PROFILE`, inputs y nombres de
métricas se mantienen. `EXCHANGE` y `BOOK_EXCHANGE` de P2 pueden ser `null`.
Ambos `raw` incluyen `inputs`, `input_trace`, `periods`, `extraction_diagnostics`,
`target_minutes_expected`, `target_minutes_present` y, cuando aplica, `reason`.

Una lectura disponible no equivale a consenso: los representantes entre casas
mantienen la media de dos casas comparables. Si falta una contraparte, su relación,
gap o representante queda `null`; no se reemplaza por cero ni se inventa una media.
Los nuevos resultados usan `p2-signal-profile-v2` y `p3-signal-profile-v2` para
distinguirlos de los resultados históricos de la política anterior.

## Responsabilidades

El flujo existente se conserva:

1. `modules/jobs/pre_start_check_job/pillar_pipeline.py` construye/reutiliza
   `OddsTrajectoryContext` y selecciona un minuto común.
2. `modules/pillars/market_snapshot_extractor.py` lee el contexto tipado, valida
   escalares y conserva el linaje de las cotizaciones.
3. `market_candidate_selection.py` selecciona un contrato único. Prefiere el único
   candidato completo; no combina candidatos parciales. La ambigüedad queda local
   a la solicitud del bookie.
4. Cada `snapshot_policy.py` declara solicitudes y aplica requisitos propios del
   mercado. P2 comparte la misma política por bookie para FT y 1H.
5. `market_coverage.py` contiene `PeriodDiagnostics`, agregación por bookie y estados
   comunes. Los módulos anteriores reexportan sus nombres para compatibilidad.
6. `models.py`, `signal_engine.py`, `metrics.py` y `relations.py` conservan los
   contratos y cálculos específicos de Side y Totals.
7. `market_audit.py` serializa inputs y metadatos; `extraction_logging.py` comparte
   los logs de cobertura y linaje. Los orquestadores ensamblan la respuesta.
8. `mining/adapters/pillar_2.py` y `pillar_3.py` incluyen tanto periodos `COMPLETE`
   como `PARTIAL` en las dimensiones persistidas.

Las configuraciones de periodos y los DTO de cada dominio permanecen explícitos.
No se introduce una clase base universal para los motores ni se trasladan reglas
de Side al código de Totals.

## Memoria, CPU y PostgreSQL

El selector compartido conserva referencias a un candidato completo y uno parcial
y sus contadores; evita listas temporales adicionales. `QuotePoint`, `QuoteTrace`
y `MarketCandidate` usan `slots`. Las identidades de una solicitud se normalizan
una sola vez antes de recorrer las líneas. También se eliminan copias redundantes
de diccionarios en la composición del snapshot de P2.

Los pilares no añaden consultas, no reconstruyen el contexto desde PostgreSQL y
no introducen cachés globales de eventos. Se conserva la persistencia existente
y su upsert por evento/pilar/ámbito/slot/versión. No se requieren migraciones ni
índices nuevos. La validación incluye persistencia con SQLite y compilación del
upsert PostgreSQL; no constituye un benchmark contra un PostgreSQL real.

## Pruebas

`tests/pillars/test_market_bookie_coverage.py` cubre cada bookie por separado,
persistencia de perfiles parciales, cobertura distinta en FT/1H, alternativas
AH/Handicap por casa, ambigüedad local, valores inválidos, candidatos parciales
alternativos, ausencia de empate y contratos de periodos diferentes.

Las suites existentes de P2/P3 mantienen los cálculos de cobertura completa y
actualizan las expectativas que antes exigían abortar por la falta de una casa.
También se verifican el extractor, el contexto de trayectoria, los adaptadores,
el repositorio de mining, la carga del pipeline y sus límites de ejecución.
