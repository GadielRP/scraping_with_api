# Inconsistencia de contrato: cierre pre-start entre P4 y P5

## Estado

Documentado, no corregido como parte de la optimización de carga de trayectoria.

## Problema

- La configuración define `PRE_START_CLOSING_ODDS_MINUTE`, cuyo valor actual es
  `1`, y sustituye cualquier momento configurado como `0` por ese minuto de
  cierre.
- P4 obtiene el cierre desde esa configuración y, por tanto, espera el punto
  T-1.
- P5 mantiene `CURRENT_TARGET_MINUTE = 0` y busca el precio actual únicamente en
  T0.

El contrato de datos entregado a P5 no contiene T0 bajo la configuración actual.
En consecuencia, aunque exista una trayectoria válida en T-1, P5 puede producir
`INSUFFICIENT_DATA` con razón `missing_current_target_minute`.

## Decisión pendiente

Antes de modificar código debe definirse una única semántica de negocio:

1. **T-1 es el cierre oficial:** P5 debe consumir
   `PRE_START_CLOSING_ODDS_MINUTE`.
2. **T0 es obligatorio:** T0 debe conservarse como momento independiente y el
   scheduler debe capturarlo, sin sustituirlo por T-1.

No se recomienda cambiar solamente la constante de P5 sin decidir cuál de esos
dos contratos representa el precio de cierre del producto.

## Archivos relacionados

- `infrastructure/settings/config.py`
- `modules/jobs/pre_start_check_job/moment_policy.py`
- `modules/pillars/pillar_4/drift_engine/drift_engine.py`
- `modules/pillars/pillar_5/exact_price_memory_engine/exact_price_memory_engine.py`

