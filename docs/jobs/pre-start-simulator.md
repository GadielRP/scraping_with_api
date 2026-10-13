# Simulación del flujo pre-start

`scripts/development/simulate_pre_start_check.py` usa selección de candidatos,
proveedores y evaluación productivos con minutos forzados. Acepta uno o varios
IDs canónicos ya existentes en la base de datos.

## Ejecutar

Desde la raíz del proyecto, con el entorno virtual y la migración Alembic
`20261012_01` aplicada a la base de pruebas:

```powershell
# P1–P5 con las cuotas ya persistidas, en T5, sin adquirir cuotas nuevas.
.venv\Scripts\python.exe -m scripts.development.simulate_pre_start_check 14083613 --minutes 5 --no-odds-ingestion --no-alerts --pillars --custom-pillar-filter --pillar-1 --pillar-2 --pillar-3 --pillar-4 --pillar-5

# Varios eventos: adquirir cuotas SofaScore/Oddspapi y evaluar P5 solamente.
.venv\Scripts\python.exe -m scripts.development.simulate_pre_start_check 14083613,14083614 -m 5 --odds-ingestion --sofascore --oddspapi --no-oddsportal --no-alerts --pillars --custom-pillar-filter --no-pillar-1 --no-pillar-2 --no-pillar-3 --no-pillar-4 --pillar-5 --market-report

# Consultar todas las opciones y sus valores por defecto.
.venv\Scripts\python.exe -m scripts.development.simulate_pre_start_check --help
```

También acepta `<id> <minutos>` o `<id1> <id2> <minutos>`. Los minutos pueden ser
cero o negativos si están contemplados por la configuración productiva.

## Toggles

Cada opción tiene su inversa `--no-…`. Una opción omitida conserva el valor de
la constante al principio del script. Los overrides duran una sola ejecución;
se restaura la configuración incluso cuando falla un proveedor o la persistencia.

| Constante | Opción CLI | Efecto |
|---|---|---|
| `ENABLE_ODDS_INGESTION_SIMULATION` | `--odds-ingestion` | Habilita adquisición de cuotas y validación de horarios productiva. Desactivado, utiliza cuotas almacenadas. |
| `ENABLE_SOFASCORE_ODDS_SIMULATION` | `--sofascore` | Ejecuta el procesador de cuotas SofaScore si la ingestión está habilitada. |
| `ENABLE_ODDSPAPI_ODDS_SIMULATION` | `--oddspapi` | Ejecuta Oddspapi si la ingestión está habilitada, aun cuando su gate global esté apagado. |
| `ENABLE_ODDSPORTAL_ODDS_SIMULATION` | `--oddsportal` | Ejecuta OddsPortal si la ingestión está habilitada y el evento coincide con ruta y minuto de apertura. |
| `ENABLE_ALERT_PIPELINE` | `--alerts` | Ejecuta el pipeline de alertas, que puede enviar notificaciones reales. |
| `ENABLE_PILLAR_PIPELINE` | `--pillars` | Ejecuta el pipeline instrumentado de pilares, con persistencia obligatoria. |
| `ENABLE_CUSTOM_PILLAR_FILTER` | `--custom-pillar-filter` | Usa los cinco toggles locales. Desactivado, usa `Config.PILLAR_PIPELINE_ENABLED_PILLARS`. Los dos toggles de pipeline siguen siendo independientes. |
| `ENABLE_PILLAR_1` … `ENABLE_PILLAR_5` | `--pillar-1` … `--pillar-5` | Selecciona cada pilar cuando el pipeline y el filtro personalizado están habilitados. P1 incluye Side y Totals. |
| `SHOW_MARKET_PERSISTENCE_REPORT` | `--market-report` | Muestra mercados, contratos, cuotas y timestamps almacenados de todos los proveedores por evento; marca snapshots nuevos de esta ejecución. Funciona también sin ingestión. |

Los toggles de proveedor controlan adquisición de cuotas. La preparación de
contexto y P1 pueden consultar SofaScore para metadatos o información deportiva,
independientemente del toggle de cuotas SofaScore.

## Minutos, filtros y resultados

- Se mantienen `PRE_START_ODDS_MOMENTS`, `PILLAR_PIPELINE_EXECUTION_MOMENTS`,
  `ODDSPAPI_PRE_START_ALLOWED_MOMENTS` y `ODDSPORTAL_OPENING_CAPTURE_MINUTES`.
  Habilitar un toggle no elimina estos filtros ni los de competición/deporte.
  El log muestra los toggles efectivos y advierte si el minuto no permite evaluar.
- Los proveedores se ejecutan antes de los pilares. OddsPortal termina antes
  de leer trayectorias y mostrar el reporte; también se espera su cierre si falla
  la fase síncrona de ingestión.
- Es una ejecución real con horario de selección forzado: escribe cuotas,
  metadatos e instrumentación en la base configurada. Con ingestión habilitada,
  la corrección productiva de horarios también puede actualizar el evento.
- `evaluation_as_of` es el instante UTC real de captura de inputs. No se cambia
  por el minuto simulado ni por un reloj histórico. La frontera histórica de
  consulta Oddspapi es kickoff menos minutos simulados y solo limita esa fuente.
- Cada evaluación persistida genera su `evaluation_id`; conserva los seis ámbitos,
  incluidos los `SKIPPED`. P5 utiliza la vista y congela la muestra original.
  Consultar [persistencia minera](../pillars/mining-persistence.md) para lectores.
- Debug utiliza las rutas habituales de `debug/pillar_pipeline_objects/` para
  inspeccionar los objetos y la trayectoria exportada. El reporte de mercados es
  opcional y puede ser extenso.
- Un evento inexistente produce retorno `False` y salida CLI `1`, aunque los
  eventos encontrados sí se procesan. Un fallo de persistencia se propaga.
  Una exclusión por filtros productivos queda explicada en logs y no es un error
  de ejecución. Código CLI `0` indica flujo completado, no garantiza señales
  calculadas: revisar los estados y motivos persistidos.


## Relojes y reconstrucción histórica

La ingestión puede probarse sola, sin configuración minera ni frontera de IDs:

```powershell
python -m scripts.development.simulate_pre_start_check 303597 -m 5 --odds-ingestion --oddspapi --no-sofascore --no-oddsportal --no-alerts --no-pillars --market-report
```

Después puede evaluarse lo ya guardado:

```powershell
python -m scripts.development.simulate_pre_start_check 303597 -m 5 --no-odds-ingestion --no-alerts --pillars --custom-pillar-filter --pillar-2 --pillar-3 --pillar-4 --pillar-5 --no-pillar-1
```

`collected_at` conserva la hora real del lote; `evaluation_as_of`, la hora real de
captura de inputs. El minuto simulado define el objetivo histórico y el límite de
fuente de la descarga Oddspapi, sin mover esos relojes reales.
`source_collected_at` conserva la hora del proveedor o NULL, también en SofaScore.
El reporte muestra ambos timestamps y marca los snapshots nuevos.

Los checkpoints se reconstruyen con el último tick de fuente anterior o igual al
objetivo, aunque se descargue hoy. Un tick puede representar varios momentos sin
duplicarse en la base. Sin fuente solo se utilizan observaciones cercanas a cada
checkpoint: consultar hoy SofaScore no recupera sus precios de ayer. El histórico
con fuente válida se reutiliza sin reescribirlo.

P4 mantiene el cambio original y sitúa el checkpoint reconstruido en el objetivo.
La vista adaptativa añade un cierre lógico en memoria cuando corresponde. No se
persiste como observación ficticia. Es una evaluación nueva con inputs capturados
ahora; para recuperar qué sabía el sistema en una ejecución pasada hay que leer
su expediente original. Los filtros productivos siguen aplicándose.
El esquema minero se necesita al ejecutar pilares, no para ingestión sola.
