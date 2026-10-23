# Mantenimiento de disponibilidad e identidad de eventos

Los comandos se ejecutan desde la raíz del repositorio y usan `DATABASE_URL` de
la configuración existente. Por defecto solo muestran la selección. `--apply`
habilita las escrituras. No consultan APIs de proveedores.

Ambos scripts paginan por ID, conservan únicamente el lote actual en memoria y
abren transacciones independientes. No usan OFFSET ni cargan toda la población
como objetos ORM. Fijan el ID máximo al comenzar para que el recorrido sea finito.
No se ejecutan COUNT ni agregaciones globales antes de procesar. Cada página
selecciona primero un máximo de `--batch-size` IDs por clave primaria; solo después
se evalúan los filtros sobre esos IDs. El límite superior se obtiene mediante
`ORDER BY id DESC LIMIT 1`. `--limit` limita los candidatos que cumplen los filtros,
no los IDs examinados para encontrarlos. Si se interrumpe una ejecución, los lotes anteriores quedan confirmados
y el lote en curso se revierte si no llegó al commit.

## Corregir positivos históricos Oddspapi

`scripts/maintenance/reset_oddspapi_has_odds.py` cambia exclusivamente
`has_odds=true` a `NULL`, actualizando también `updated_at`, cuando:

- El mapping pertenece a `oddspapi`.
- Su `created_at` es anterior a `--created-before`.
- El evento no tiene ningún mercado, de ninguna fuente.

No depende del estado de SofaScore. Conserva los negativos, los desconocidos,
los mappings recientes y los eventos con mercados. No elimina mappings ni
eventos. El cambio significa disponibilidad desconocida; no significa ausencia
de cuotas y no ejecuta una comprobación contra el proveedor.

El corte por defecto es `2026-09-24T00:00:00Z`, correspondiente a la fecha de la
revisión que eliminó el default positivo. La base no guarda cuándo se desplegó
esa revisión: si el corte operativo fue otro, pásalo explícitamente. El filtro
usa la creación del mapping, no el inicio del evento ni su última modificación.
Los positivos de ese período sin mercados se resetean aunque se hayan actualizado
después; no hay una marca separada que identifique qué positivos se comprobaron.

```powershell
# Previsualizar todos los candidatos, en lotes de 500 mappings.
.venv/Scripts/python.exe scripts/maintenance/reset_oddspapi_has_odds.py --batch-size 500

# Aplicar la corrección por lotes.
.venv/Scripts/python.exe scripts/maintenance/reset_oddspapi_has_odds.py --batch-size 500 --apply

# Otra fecha de corte y una ejecución limitada.
.venv/Scripts/python.exe scripts/maintenance/reset_oddspapi_has_odds.py --created-before 2026-09-24T18:00:00Z --batch-size 100 --limit 1000 --apply
```

`selected` y `updated` cuentan mappings, no eventos distintos. `scanned` cuenta
IDs examinados, incluidos los que no cumplen los filtros. En la
previsualización del 22/10/2026 hubo **17.066 mappings** candidatos, correspondientes
a los positivos históricos sin mercados de la auditoría. Un evento puede tener
más de un mapping Oddspapi.

La salida de cada lote incluye `last_mapping_id`. Para continuar desde un lote
confirmado, usa `--after-id <last_mapping_id>` con los mismos filtros. También se
puede repetir desde el principio: los mappings que ya están en NULL dejan de ser
candidatos. El cursor se imprime después del commit; si se perdió la última
salida, repetir el lote no vuelve a modificar los NULL.

## Eliminar eventos mediante filtros configurables

`scripts/maintenance/prune_events.py` no tiene una población predeterminada.
Ejecutarlo sin argumentos produce un error de selección y no recorre eventos.
Por defecto no hay filtros por estado de fuente ni por presencia de mercados,
y las listas de discovery están desactivadas. Debes indicar competiciones,
activar uno de los selectores opt-in de discovery o seleccionar explícitamente
`--all-competitions`.

Los filtros disponibles son:

- `--source-state SOURCE=true|false|unknown|missing`, repetible. Sin este argumento,
  no se filtra por disponibilidad ni por existencia de mappings.
- `--markets absent|present|any`: eventos sin mercados, con mercados o ambos.
  El valor por defecto es `any`, que no impone ninguna restricción.
- `--competition-id` / `--competition-ids`: IDs canónicos de competiciones.
- `--source-unique-tournament-id` / `--source-unique-tournament-ids`: IDs externos
  SofaScore, mediante la relación normalizada con `competitions` y
  `source='sofascore'`.
- `--discovery-exclusions`: agrega los valores actuales de
  `SOFASCORE.filters.excluded_sofascore_unique_tournament_ids` y
  `ODDSPAPI.filters.excluded_sofascore_unique_tournament_ids` de `discovery.py`.
  Se leen al arrancar y no dependen del switch `exclude_competitions`.
- `--discovery-excluded-categories`: agrega los pares actuales de deporte y
  categoría de `SOFASCORE.filters.excluded_categories` y
  `ODDSPAPI.filters.excluded_categories`. El deporte se compara con el nombre
  canónico de `events.sport`; la categoría debe coincidir exactamente con
  `competitions.category_name`, dentro de competiciones `source='sofascore'`.
  Se leen al arrancar, aunque `exclude_categories` esté desactivado.
- `--category SPORT=CATEGORY`: agrega un par explícito; puede repetirse. Por
  ejemplo, `--category "football=Romania Amateur"`.
- `--all-competitions`: todas las competiciones dentro de los filtros explícitos.
  Sin filtros de fuente o mercado, esto selecciona todos los eventos.

Los IDs indican qué competiciones eliminar, no cuáles conservar. Los estados
por fuente y el filtro de mercados se combinan con AND. Los selectores de
competición y categoría se combinan con OR, incluyendo las listas de discovery
solo si se solicitan. Categorías y torneos excluidos son selectores separados;
`--discovery-exclusions` no activa las categorías. Ninguno se activa por defecto.

```powershell
# Población auditada: sin mercados, SofaScore false y sin mapping Oddspapi.
# Todas las competiciones; solo previsualizar.
.venv/Scripts/python.exe scripts/maintenance/prune_events.py --markets absent --source-state sofascore=false --source-state oddspapi=missing --all-competitions

# La misma población, limitada a las competiciones excluidas en discovery.py.
.venv/Scripts/python.exe scripts/maintenance/prune_events.py --markets absent --source-state sofascore=false --source-state oddspapi=missing --discovery-exclusions

# Todos los eventos sin mercados de las competiciones excluidas, con cualquier estado de fuente.
.venv/Scripts/python.exe scripts/maintenance/prune_events.py --markets absent --discovery-exclusions

# Todos los eventos sin mercados de las categorías excluidas configuradas.
# Preview: omite --apply hasta revisar los contadores y los IDs seleccionados.
.venv/Scripts/python.exe scripts/maintenance/prune_events.py --markets absent --discovery-excluded-categories

# Una categoría exacta escrita en CLI, independiente de discovery.py.
.venv/Scripts/python.exe scripts/maintenance/prune_events.py --markets absent --category "football=Romania Amateur"

# Sumar competiciones concretas a las listas de discovery.
.venv/Scripts/python.exe scripts/maintenance/prune_events.py --markets absent --source-state sofascore=false --source-state oddspapi=missing --discovery-exclusions --competition-ids 123 456 --source-unique-tournament-ids 789 1011

# Otro conjunto: disponibilidad Oddspapi desconocida, sin mercados, competición 123.
.venv/Scripts/python.exe scripts/maintenance/prune_events.py --markets absent --source-state oddspapi=unknown --competition-id 123

# Seleccionar la competición 123 sin restricciones de fuentes o mercados.
.venv/Scripts/python.exe scripts/maintenance/prune_events.py --competition-id 123

# Aplicar una selección explícita en lotes de 100, con un máximo de 500 eventos.
.venv/Scripts/python.exe scripts/maintenance/prune_events.py --markets absent --source-state sofascore=false --source-state oddspapi=missing --discovery-exclusions --batch-size 100 --limit 500 --apply

# Aplicar categorías excluidas configuradas en lotes de 100 después de revisar el preview.
.venv/Scripts/python.exe scripts/maintenance/prune_events.py --markets absent --discovery-excluded-categories --batch-size 100 --apply
```

`unknown` requiere al menos un NULL y ningún positivo; `false` requiere al menos
un mapping y que todos sean negativos. `missing` significa ausencia de mapping,
no `has_odds=false`. Los positivos tienen precedencia cuando hay varios mappings.
No se admite repetir una misma fuente con estados diferentes.

La salida muestra los filtros efectivos y los contadores acumulados `scanned`,
`selected` y `deleted`. El total de candidatos se conoce al terminar una
previsualización completa, sin `--limit`; no se carga ni cuenta previamente toda
la población. La población auditada se reproduce pasando los argumentos
explícitos del primer ejemplo. Como referencia histórica, la auditoría del
22/10/2026 encontró 133.365 eventos en esa población y 6.904 dentro de las listas
de discovery; los conteos pueden cambiar.

Sin `--apply` se ejecuta una previsualización. Los defaults operativos son lotes
de 100 eventos, sin límite total y cursor inicial 0. Cada lote incluye
`event_ids` y `last_event_id`. `--after-id` recibe un ID canónico de evento y
permite continuar desde un lote confirmado con los mismos filtros.

El borrado usa el escritor existente y sus dependencias: no elimina los catálogos
de competiciones o participantes; puede eliminar temporadas sin eventos e
invalida las vistas de reporting. Revalida los filtros después de bloquear la
identidad, el evento y sus mappings. Si el evento dejó de cumplirlos, se omite.

Las claves foráneas y los registros de auditoría inmutables mantienen sus
protecciones: si impiden un borrado, falla el lote, sin deshabilitar restricciones.
No se genera memoria de descarte de un estado deportivo. Un evento puede volver
a descubrirse si otro flujo permite admitirlo.

`--all-competitions` ignora las listas de discovery y no se puede combinar con
IDs o categorías explícitos.

## Memoria y recuperación de espacio en un servidor de 1 GB

El tamaño predeterminado en ambos scripts es 100. El tamaño del lote acota los
IDs examinados, los candidatos y las escrituras de entidades principales. Las
consultas de dependencias se restringen a esos IDs; el número de mappings,
quotes, snapshots u otras filas hijas puede superar el número de eventos del
lote. No se carga la colección completa de esas dependencias al borrar: el
escritor existente usa deletes SQL y las cascadas de claves foráneas.

Un LIMIT acota las filas devueltas, no garantiza un límite de bytes de RAM ni de
páginas leídas por PostgreSQL. El volumen de dependencias, los planes de consulta,
`work_mem`, los índices y las otras tareas del servidor también cuentan. No se ha
certificado el consumo combinado de aplicación, script y PostgreSQL en un host
real de 1 GB. El archivo `compose.prod.yaml` ya define, por defecto, 384 MB por
contenedor de aplicación y PostgreSQL, `shared_buffers=64MB`, `work_mem=4MB`,
`maintenance_work_mem=32MB` y consultas sin workers paralelos. Son valores de ese
archivo, no una verificación de la configuración efectiva del servidor.

Los scripts no ejecutan VACUUM ni VACUUM FULL, tampoco entre lotes. UPDATE y
DELETE generan versiones muertas; autovacuum o VACUUM normal dejan ese espacio
reutilizable dentro de PostgreSQL. Generalmente no reducen el tamaño de los
archivos en disco. VACUUM FULL reescribe la tabla, necesita espacio adicional y
bloquea exclusivamente la tabla; requiere mantenimiento separado.

Si se decide hacer mantenimiento manual tras terminar y confirmar los lotes,
se puede usar una sesión independiente, fuera de una transacción:

```sql
SET maintenance_work_mem = '16MB';
VACUUM (ANALYZE, PARALLEL 0) public.event_source_mappings;
VACUUM (ANALYZE, PARALLEL 0) public.events;
```

Incluye también las tablas dependientes que realmente hayan sufrido borrados
(por ejemplo `results`; si se eliminaron eventos con mercados, también las tablas
de cuotas afectadas). No ejecutar después de cada lote. Estos comandos son una
opción operativa; no se ejecutaron como parte de la implementación.

Referencias: [VACUUM en PostgreSQL 15](https://www.postgresql.org/docs/15/sql-vacuum.html)
y [consumo de recursos](https://www.postgresql.org/docs/15/runtime-config-resource.html).
