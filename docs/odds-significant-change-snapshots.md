# Snapshots de cambios significativos de OddsPapi

## Activación y contrato

`ENABLE_ODDSPAPI_SIGNIFICANT_CHANGE_SNAPSHOTS=false` conserva la estrategia
anterior. Al activarlo se seleccionan cambios adaptativos en la adquisición
histórica live; `ENABLE_ODDSPAPI_HISTORICAL_AS_OF_PERSIST` sigue controlando la
escritura y `ENABLE_ODDSPAPI_HISTORICAL_AS_OF_SHADOW` la comparación.
El cambio no modifica el `.env` local ni los datos existentes.

Los umbrales versionados están en `OddspapiPreStartSettings`: magnitud 20%,
historial mínimo 24 horas, reversión 3 minutos y precio mínimo exclusivo 1.01.
Se aplican individualmente a cada bookmaker/mercado/outcome/jugador.

El reader ordena como antes y sanitiza una vez por serie. Apertura, última
cuota, detector y fallback comparten los ticks limpios. Se excluyen precios
no finitos, cuotas inactivas, precios centinela y ticks posteriores al kickoff,
incluso cuando los controles clásicos de actividad/cutoff estén desactivados.
Si falta kickoff, se registra el motivo y se utiliza el comportamiento clásico.

El detector recibe la conversión horaria como dependencia explícita y no
importa configuración, HTTP ni SQL. El DTO se comparte desde
`historical_odds_quote.py`, manteniendo su importación pública anterior desde
`historical_odds_as_of.py`.

## Selección temporal

- No se emite el ancla inicial. Cada cambio confirmado actualiza el ancla.
- La reversión debe volver a una diferencia estrictamente menor al umbral
  antes de completar la ventana. Revertir exactamente al final no invalida
  el candidato. Los microticks en la nueva zona no requieren precio idéntico.
- Un tick se considera vigente hasta el siguiente válido; no se exige una
  frecuencia de actualizaciones para confirmar permanencia.
- En el tramo posterior a `kickoff - ventana`, solo se evalúa el último tick
  válido hasta kickoff. Se emite sin confirmación si cambia al menos el umbral
  respecto al ancla vigente. No se registra un cierre estable automáticamente.
- Con menos de 24 horas se reconstruyen los momentos configurados sobre la
  serie limpia. Este fallback mantiene los tiempos teóricos tradicionales.
- Los cambios dinámicos conservan microsegundos en `collected_at`, fecha del
  proveedor en `createdAt` y minutos fraccionarios en `minutesUntilStart`.
  La consulta analítica sigue devolviendo minutos enteros por compatibilidad.

Los snapshots ordinarios de apertura y cuota actual continúan existiendo;
esta estrategia solo sustituye la reconstrucción de `momentQuotes`.

## Persistencia y coste

La deduplicación conserva todos los timestamps de proveedor por identidad de
cuota y segundo, incluido el comodín de timestamp ausente del contrato previo.
Una consulta proyectada, filtrada por evento, proveedor e intervalo entrante,
sustituye las dos consultas anteriores. Sus filas se leen en bloques y las
claves se actualizan en memoria mientras se escribe el lote.

No hay cambios DDL. La idempotencia cubre reejecuciones secuenciales de
`momentQuotes`; no añade exclusión entre transacciones concurrentes ni elimina
duplicados históricos. Los snapshots ordinarios conservan su política previa.

La sanitización conserva referencias a los ticks originales; los reductores
de apertura/cierre y momentos ya no copian listas completas. El detector
inspecciona ventanas por índice, sin crear slices. Su memoria adicional es
O(n + cambios) contando la lista limpia. El tiempo incluye las inspecciones
de ventanas: no se garantiza complejidad lineal para cualquier serie adversa
con muchos candidatos confirmados y ventanas superpuestas.

## Verificación local

Se ejecutaron 137 pruebas de dominio, reader, adquisición, exchanges,
adapter, persistencia y controles de batch. Se usaron SQLite temporal y
clientes simulados. No se verificó un servidor PostgreSQL real. Queda una
advertencia preexistente de SQLAlchemy sobre `declarative_base`.

Se actualizaron dobles antiguos de adquisición/exchanges al contrato actual
de mappings, caché y scheduler. Los ocho fallos iniciales de esas suites se
reprodujeron también cargando el código de HEAD anterior al cambio.

Medición en Python 3.12 con `tracemalloc`, contando sanitización y detector
después de construir la entrada; serie con spikes alternos, un cambio
sostenido y microticks posteriores:

| Ticks | Tiempo | Pico adicional | Cambios |
| --- | --- | --- | --- |
| 10.001 | 0,042 s | 86.704 bytes | 1 |
| 100.001 | 0,362 s | 802.360 bytes | 1 |

Estas cifras no incluyen descarga JSON ni su ordenamiento inicial.

El archivo local `historical_odds_id1300010963301857_bet365_pinnacle.json`
se reprodujo con kickoff controlado `2026-09-03T18:00:00Z`, sin afirmar que
sea la hora real del evento. Contiene 573 series: 365 quedan vacías tras la
sanitización y 208 tienen entre 13,0043 y 18,0236 horas válidas. Con mínimo
24 horas genera 1.040 observaciones de fallback; con mínimo 12 horas ninguna
serie necesita fallback y no se detectan cambios del 20%.

Comando de las suites verificadas, usando un directorio temporal nuevo:

```text
python -m pytest -q -p no:cacheprovider --basetemp <directorio-temporal-nuevo> tests/test_historical_odds_change_detector.py tests/test_historical_odds_reader_change_fallback.py tests/test_historical_odds_as_of.py tests/test_odds_acquisition_service.py tests/test_exchange_historical_fetch_executor.py tests/oddspapi/test_market_adapter.py tests/test_save_canonical_bookmaker_batches_quotes.py tests/test_market_choice_snapshot_writer.py tests/test_odds_batch_processor_exchange_fanout_wiring.py
```

Para aislar la ejecución del `.env`, establecer `DATABASE_URL=sqlite:///:memory:`
en el proceso de pruebas. El JSON de reproducción es un dato local, no un
requisito de las nuevas pruebas sintéticas.
