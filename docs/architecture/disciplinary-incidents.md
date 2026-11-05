# Incidencias disciplinarias

Implementación sobre `0efc8499b324c09db6985e8ee26a137333a1cb77`; head previo de
Alembic `20261029_01`; incidencias en `20261104_01` y totales en `20261104_02`.
No se escribe disciplina en observaciones.

## Componentes y vocabulario

`modules/discipline/contracts.py` define los términos canónicos: `yellow_card`,
`red_card`, `ejection`, `foul_out`; origen `reported`/`derived`; lado `home`/`away`.
Los hechos y DTO son inmutables. `policies.py` procesa hechos sin HTTP ni ORM;
el adaptador SofaScore interpreta únicamente su formato. El servicio recibe
proveedor, almacenamiento y exclusión. `ApplicationRuntime` compone esas piezas.
El job mantiene el presupuesto, las páginas y los contadores operativos.

Una fila de `disciplinary_incidents` es una incidencia individual vinculada a
`EventSourceMapping`. El ID del jugador es texto opaco de ese proveedor, sin FK
a `participants`. Los equipos se resuelven exclusivamente mediante los IDs de
participantes del mapping y el lado; competición y temporada vienen de `Event`.
`get_team_season_totals(source, participant_id, competition_id, season_id)` exige
esos cuatro filtros. `participant_id` es la PK local de la fila de participante
del proveedor, no su identificador externo bruto. No se mezclan proveedores.

No se persisten IDs de comentarios, textos de jugadas, reloj, periodo, evidencias
de cálculo, JSON original, cobertura ni checkpoints. Cero filas no demuestra
cobertura ni ausencia de tarjetas. Las PK y timestamps pueden cambiar al
reemplazar un snapshot; no representan identidad ni historia de cada tarjeta.

## Interpretación de comentarios

Fútbol acepta solo `yellowCard` y `redCard`, conservando multiplicidad. No infiere
rojas desde amarillas. Básquetbol guarda `ejection` explícitos. Solo el perfil
de competición canónica NBA (`176`) configura `DisciplineRules(6)`; otras ligas
no heredan ese umbral.

La muestra NBA no tiene un subtipo estructurado de falta. El adaptador reconoce
las frases inglesas `personal foul`, `shooting foul` y `offensive foul` después
del nombre del jugador. Reconoce aparte `technical foul`, incluidas las técnicas
de equipo. `possessionChange` por falta ofensiva no vuelve a contar la falta.
Categorías desconocidas, jugadores/lados incompletos en faltas o indicios de una
excepción de elegibilidad impiden reemplazar `foul_out`; `ejection` sigue siendo
administrable. Más de seis faltas también suprime ese cálculo. El orden del feed
no cambia el resultado. Una identidad externa repetida e idéntica se consume una
vez; comentarios distintos del mismo jugador conservan multiplicidad.

El umbral representa la conclusión deportiva derivada bajo la regla ordinaria,
sin demostrar que el jugador abandonó físicamente la cancha. La NBA contempla
excepciones por falta de jugadores elegibles: [regla 3](https://official.nba.com/rule-no-3-players-substitutes-and-coaches/)
y [regla 12](https://official.nba.com/rule-no-12-fouls-and-penalties/).
No se intenta reconstruir alineaciones para resolver esas excepciones.

Las muestras proporcionadas contienen:

- Getafe–Sevilla: 124 comentarios, 2 amarillas locales, 3 amarillas visitantes y
  1 roja visitante; marcadores `matchStarted` y `matchEnded`.
- Warriors–Suns: 541 comentarios, seis faltas personales de Booker (`817000`),
  una pérdida ofensiva asociada y expulsión explícita de Green (`817122`);
  inicio del primer cuarto y final del cuarto cuarto.

No contienen metadatos de paginación. Estos archivos muestran la forma observada,
pero no constituyen documentación de exhaustividad del endpoint para todos los
partidos. El adaptador exige inicio y final de partido (en básquetbol, final de
4Q o prórroga sin un periodo iniciado posterior), documento agotado correctamente,
colección no vacía y ausencia de indicadores conocidos de páginas pendientes.
HTTP fallido, 404, JSON truncado, colección ausente/inválida, límites excedidos
o lados inválidos nunca producen una escritura parcial. Una lista vacía conserva
lo previo. Un feed válido sin incidencias tampoco elimina el detalle anterior;
para fútbol se intenta recuperar totales desde statistics.

**Control de validación del proveedor:** cuando `DisciplineSettings.full_feed_verified`
es `False`, la CLI permite dry-run, pero la composición rechaza
`--commit` hasta verificar el contrato de partido completo y establecer ese valor
en `True`. Si se descubre paginación, implementar su recorrido completo antes de
activar escrituras. La automatización requiere además `after_midnight_results_enabled=True` y se entrega
desactivada. Las pruebas usan snapshots conocidos para validar el writer de forma
independiente de esa decisión operativa.

## Adquisición, memoria y escritura

`open_event_comments(sofascore_event_id)` reutiliza `download_json()` y
`open_json_document()`. `document_entries(..., 'comments')` recorre el documento;
solo permanecen incidencias, contadores de jugadores e identidades relevantes.
No se importa el writer de debug. Se procesa un partido a la vez y la sesión de
lectura está cerrada durante HTTP. La respuesta temporal se elimina al cerrar,
también ante errores normales; un cierre abrupto puede requerir limpieza de
temporales. No se introduce almacenamiento raw ni una política de retención nueva.

Configuración en `infrastructure/settings/discipline.py`. Límites de `DisciplineSettings`: 1024 incidencias, 512 jugadores, 8192 identidades
relevantes, 200 partidos y 900 segundos por ejecución. Los límites de transporte
y de tamaño/profundidad de cada elemento JSON provienen de `JobExecutionSettings`.
Se reutiliza su tamaño de página de candidatos (100). No se acumulan cuerpos ni
resultados de partidos históricos. La memoria depende del elemento individual y
de estos límites; no se promete consumo absoluto constante.

La exclusión usa `exclusive_slot(736106, event_id)`, compartida por CLI y scheduler.
Reserva una conexión en autocommit durante HTTP; la escritura ocupa temporalmente
otra conexión. No ampliar la concurrencia sin medir el pool (actualmente seis).

El writer toma los locks de identidad, después `Event` y mappings en ese orden;
revalida identidad, deporte, competición, temporada, participantes y resultado
completo. Compara multiconjuntos por tipo, origen, jugador, nombre y lado. Igualdad
no ejecuta DML. Diferencias borran solo los tipos administrados del mapping y
ejecutan una inserción bulk en la misma transacción. Un fallo de inserción revierte
el borrado. No recrea padres eliminados o reasignados. La FK tiene `ON DELETE CASCADE`;
el writer de borrado también elimina explícitamente incidencias antes de mappings.

El índice inicial es `(event_source_mapping_id, type)`. No se añaden índices a
eventos ni materializaciones sin un plan de PostgreSQL con volumen representativo.
La selección usa keyset de `Event.id`, high-water fijo, resultado completo y una
sola fila por evento; mappings ausentes o ambiguos se contabilizan como conflicto.
Las consultas por mapping usan los índices existentes, sin agrupar todo el histórico
en cada página. No se filtra por ausencia de incidencias, para permitir correcciones.

## Totales y fallback de statistics

`disciplinary_incidents_totals` conserva un agregado vigente por
`(event_source_mapping_id, type, side)`, con `count >= 0`, endpoint (`comments`
o `statistics`) y timestamps UTC. Esa combinación es su PK; no se duplica
`event_id` ni se añade un índice redundante. El mapping identifica al proveedor,
partido externo y participantes. No hay destinatarios ficticios en esta tabla.

La adquisición combina detalle y agregados antes de escribir. Comments utilizable
con incidencias genera totales para sus tipos administrados y ambos lados,
incluidos ceros. Comments sin incidencias no autoriza un reemplazo destructivo.
En fútbol, 404, colección vacía, ausencia de incidencias, documento inválido,
feed incompleto o fallo ordinario de adquisición disparan el fallback.
Cancelación, deadline y agotamiento de recursos no inician otra solicitud.

`statistics_for_all_events=False` configura ese comportamiento. Con `True` se
consulta statistics para todos los partidos de fútbol seleccionados, aunque
comments tenga incidencias. Las demás rutas y filtros de selección se conservan.
Básquetbol no consulta statistics: sus faltas de equipo no identifican expulsados.

`open_event_statistics` usa descarga temporal y lectura incremental con los mismos
límites de transporte. El parser separado acepta solo `period="ALL"`, claves
`yellowCards` y `redCards`, y pares `homeValue`/`awayValue` enteros no negativos
(sin booleanos). No suma periodos parciales ni convierte una clave ausente en cero.
Valores contradictorios repetidos o documentos truncados invalidan la adquisición.
Una métrica inválida se omite; las demás métricas válidas pueden actualizarse.

En una ejecución, los agregados utilizables de statistics prevalecen sobre los
de comments para el mismo tipo y lado. Otros alcances permanecen intactos salvo
que comments aporte un agregado válido. Statistics nunca reemplaza incidencias
individuales. Por eso el detalle conservado puede diferir del último total.
Si ambas fuentes son inutilizables se conserva todo lo anterior. Un cero explícito
de statistics sí puede actualizar el total a cero sin borrar destinatarios previos.

`replace_acquisition(context, acquisition, dry_run=False)` mantiene locks y
revalidación del contexto y publica detalle y totales en una sola transacción.
Solo actualiza agregados cuya cantidad o endpoint cambió, conservando timestamps
en repeticiones idénticas. `get_team_season_totals` usa el agregado si existe y,
si falta, cuenta las incidencias de ese alcance en SQL. Nunca suma ambos.
`get_event_totals(source, event_id)` devuelve los agregados adquiridos y su endpoint;
la ausencia de un agregado no demuestra un cero real ni cobertura histórica.

La CLI incluye agregados y diagnósticos en el dry-run de un evento. El job registra
los totales con su endpoint, `statistics_reason`, y contadores `statistics_attempted`
y `statistics_used`. Todos los puntos de entrada comparten esta adquisición.

## Migración y operación

Aplicar el cambio de esquema mediante Alembic; el arranque solo verifica esquema:

```bash
python -m alembic upgrade head
python main.py discipline --event-id 12345 --dry-run
python main.py discipline --event-id 12345 --commit
python main.py discipline --date 2026-11-03 --commit
python main.py discipline --from-date 2026-11-01 --to-date 2026-11-04 --dry-run
python main.py discipline --competition-id 176 --season-id ID --limit 200 --after-event-id 0 --commit
```

`--event-id`, competición y temporada son IDs internos; el mapping resuelve la
identidad HTTP. Elegir un evento, una fecha/rango o competición+temporada. El
extremo `--to-date` es exclusivo y las fechas usan `Config.TIMEZONE`. Sin `--commit`
siempre es dry-run; `updated` significa que cambiaría el snapshot. La salida de un
solo evento muestra incidencias normalizadas; el resumen incluye `dry_run`,
`scanned`, `updated`, `unchanged`, `unavailable`, `invalid`, `deferred`,
`mapping_conflict`, `failed`, `last_processed_event_id` y `limit_reached`.

La revisión `20261104_01` crea incidencias y su índice. La `20261104_02` añade
únicamente la tabla de totales, sin backfill. El downgrade de esta última elimina
los agregados y conserva las incidencias; no ejecutar downgrades en datos reales
para probarlos.

Al habilitar `after_midnight_results_enabled` y `full_feed_verified`, midnight
inyecta un callback en `run_results_collection`. Después de cada lote confirmado,
`collect_batch` entrega referencias inmutables `CommittedResultEvent` (ID canónico,
deporte y competición), solo para los IDs confirmados por el writer de resultados.
`updated` cuenta upserts confirmados; no necesariamente inserciones nuevas.

El callback filtra en memoria con `is_tracked_competition` y los deportes soportados
(Football/Basketball), usando los metadatos ya devueltos por `batch_upsert_events`.
El runner recibe `Selection(event_ids=...)`; hace una consulta por página para
resolver mappings y comprobar el resultado completo, sin consulta de high-water
adicional ni selección por fechas. Las lecturas bajo bloqueo del writer siguen
siendo necesarias para coordinar cambios y borrados concurrentes.

No existe horario independiente, ventana automática ni registro en el scheduler.
Los comandos manuales conservan sus filtros. En midnight el límite se ajusta al
lote recibido: no corta el total nocturno a 200. El presupuesto de disciplina se
aplica por lote, respetando cualquier deadline padre.

Los resultados se confirman antes de HTTP de comentarios. Se liberan los buffers
de resultados/metadatos antes del callback y se procesa un partido a la vez; no se
acumulan los 711 eventos de una noche. La duración global de results collection
incluye disciplina; los logs de persistencia por lote excluyen comentarios.

Un fallo ordinario de disciplina se registra sin revertir resultados ni impedir
los siguientes lotes. Cancelación y `WorkDeferred` se propagan conservando los
contadores de resultados confirmados. Si resultados se difiere antes del callback,
ese lote no ejecuta disciplina. Sin cola durable ni ventana de reintento, las
incidencias omitidas o fallidas requieren recuperación manual por fechas o IDs.
No hay garantía de exactly-once ni cobertura histórica completa.

Intraday utiliza el mismo callback con `origin="intraday"`, activable mediante
`after_intraday_results_enabled`. Después del commit de resultados toma solo los
IDs confirmados por el writer y reutiliza deporte/competición de los candidatos
que ya tiene en memoria. Entrega referencias por páginas y libera los payloads
de resultados antes de adquirir comentarios. No consulta de nuevo las competiciones.
Hereda la prioridad, cancelación y deadline del pre-start que lo invoca.

`tracked_competitions_only=True` es el filtro compartido de disciplina para midnight
e intraday; al desactivarlo acepta también competiciones no seguidas, manteniendo
los deportes soportados. El filtro se aplica a los eventos recibidos. Pre-start
filtra por separado los candidatos de intraday mediante
`Config.INTRADAY_RESULT_FRESHNESS_TRACKED_COMPETITIONS_ONLY` (actualmente True).
Para recibir competiciones no seguidas desde esa ruta también debe desactivarse
ese filtro de origen. Los comandos manuales mantienen su selección explícita.

El script de temporadas admite `--with-discipline` en sus modos de temporada
individual, `--season_name` y `--auto`:

```bash
python -m scripts.sport_seasons_processing 100772 -t 132 --skip-odds --with-discipline
```

El flag activa la recolección del script independientemente de los toggles de
midnight/intraday, manteniendo `full_feed_verified` y `tracked_competitions_only`.
Se aplica tanto a eventos del endpoint de temporada como a los resultados de su
reconciliación. Los resultados se confirman en lotes de `event_write_batch_size`
y se entregan solo referencias confirmadas al callback compartido. Los comentarios
se procesan incrementalmente y no se guardan como respuestas raw. Sin el flag,
el script no solicita comentarios. El script conserva su modo habitual de escritura;
este flag no es un dry-run. Las incidencias fallidas se registran para recuperación
manual, igual que en las otras rutas.

## Validación reproducible

`tests/discipline/fixtures/` contiene versiones mínimas de los dos ejemplos,
sin perfiles ni traducciones. Las pruebas cubren su resultado, orden inverso,
multiplicidad, scopes, snapshot repetido, dry-run, rollback, cascada, identidad,
selección CLI, calendario y exclusión. Para PostgreSQL usar exclusivamente una DB
desechable mediante `DISCIPLINE_TEST_DATABASE_URL`: el fixture recrea el esquema
anterior y ejecuta el upgrade Alembic real desde `20261029_01`.

```bash
python -m pytest tests/discipline tests/test_schema_version.py tests/test_sofascore_event_comments.py -q
python -m scripts.benchmarks.discipline_comments --comments 1000
python -m scripts.benchmarks.discipline_comments --comments 100000
python -m scripts.benchmarks.discipline_comments --comments 100000 --events 20
python -m scripts.benchmarks.discipline_comments --comments 100000 --mode materialized
```

Ejecutar cada benchmark en un proceso nuevo. Reporta tiempo, filas y pico de
asignaciones Python con `tracemalloc`, que **no equivale a RSS**. La línea base
materializada solo decodifica y cuenta tarjetas; no ejecuta políticas completas,
por lo que sus tiempos no son una comparación equivalente de procesamiento.

Medición local del 4 de noviembre de 2026, un proceso limpio por fila:

| Modo | Comentarios por partido | Partidos | Bytes de respuesta | Pico Python | Tiempo |
|---|---:|---:|---:|---:|---:|
| Incremental | 1.000 | 1 | 256.137 | 319.141 B | 0,229 s |
| Incremental | 100.000 | 1 | 25.600.137 | 319.141 B | 17,447 s |
| Incremental | 1.000 | 20 | 256.137 | 320.579 B | 3,950 s |
| Materializado | 100.000 | 1 | 25.600.137 | 76.101.321 B | 0,553 s |

Cada partido produjo una tarjeta. La instrumentación agrega coste de CPU; estos
tiempos no representan latencia HTTP ni constituyen un SLA de producción.

Antes de incorporar los agregados, en PostgreSQL 17 desechable se ejecutó
`EXPLAIN (ANALYZE, BUFFERS)` de los
totales con datos sintéticos: 10.000 eventos, 60.000 incidencias y 100 equipos.
La consulta devolvió las 600 tarjetas esperadas para el equipo seleccionado en
17,681 ms, con 934 buffers compartidos encontrados en caché. Usó `BitmapOr` sobre
los índices existentes de participantes y hash joins con lecturas secuenciales
de eventos/incidencias. No se justifica un índice adicional de competición/temporada
con esta distribución (todos los eventos pertenecían a la misma competición y
temporada); medir nuevamente con la distribución real antes de optimizar ese plan.
