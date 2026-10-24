# Corrección de líneas OddspAPI

La ingesta con selección cacheada admite exclusivamente los outcomes de esa
selección para mercados con línea. `mainLine=true` en el payload `/odds` no puede
reintroducir una alternativa descartada. Esta regla también se aplica cuando
`persist_main_line_only=false` o se permiten quotes inactivos. Los mercados sin
línea conservan su política anterior.

## Auditoría y evidencia

Ejecutar desde la raíz del repositorio, con la base elegida en `DATABASE_URL`:

```powershell
.venv/Scripts/python.exe -m scripts.maintenance.backfill_oddspapi_lines audit --output exports/oddspapi_line_repair/before
.venv/Scripts/python.exe -m scripts.maintenance.backfill_oddspapi_lines plan --output exports/oddspapi_line_repair/reviewed/manifest.json
```

`audit` solo lee la base y escribe `audit.md`, `audit.csv` y `audit.json`.
Un grupo es evento/casa/tipo canónico/live; el tipo canónico incluye el período.
Detecta más de una línea distinta con al menos un quote OddspAPI principal,
sin multiplicar líneas por choices ni por niveles back/lay. Los contadores de
quotes y snapshots abarcan toda la familia, incluidas líneas antiguas no principales.

`plan` solo lee la base y los JSON originales. Ejecuta `select_current_lines()`
por casa sobre cada respuesta, con los mappings y criterios actuales de producción.
La selección no depende de `oddspapi_mainline_outcome_cache` ni de casas donantes:
funciona aunque la caché no exista o contenga una selección antigua diferente.
No hace HTTP ni reingesta precios. El parámetro `--[no-]require-active-quotes`
controla el filtro de mercados inactivos; por defecto usa la configuración local.
Como en producción, los choices individuales deben estar activos para seleccionar
una línea, incluso con `--no-require-active-quotes`.

Antes de buscar una selección, las familias cuyo tipo canónico tiene
`enabled_for_ingestion=false` en la base se planifican con modo
`disabled_ingestion` y motivo `disabled_for_ingestion`. Ese es el mismo flag
del catálogo que usa `MarketMappingRepository.build_index()` para excluirlas.
Se propone eliminar todas sus cuotas OddspAPI y snapshots, y sus choices y
mercados si quedan vacíos, aunque no haya debug. También se incluyen mercados
deshabilitados sin línea. Los datos de otros proveedores se conservan.
No se confunde un mercado habilitado sin selección válida con uno deshabilitado.

El directorio predeterminado es `debug/oddspapi_odds_responses`; se puede cambiar
con `--debug-root`. Se reconocen nombres antiguos sin momento/endpoint y se excluyen
los nombres históricos. Cada JSON debe tener `bookmakerOdds` y su fixture debe
pertenecer al evento indicado por el nombre del archivo.

Si hay varias respuestas aplicables, se usa la que tenga el momento T más cercano
al inicio (por ejemplo, T−5). La línea elegida por el selector queda como principal;
se proponen eliminar las cuotas OddspAPI de las otras líneas persistidas de esa
familia, junto con sus snapshots. Si otro proveedor comparte el mercado, sus cuotas
se conservan. La tabla de caché no se modifica.

Sin una selección actual válida en una respuesta utilizable, o si la línea
seleccionada no existe en la base, la familia se omite sin proponer eliminaciones.
`plan_report.json` resume las correcciones y omisiones y contiene `selections`,
con ruta, hash, momento y diagnósticos del selector por captura y casa, incluidos
los candidatos descartados y sus motivos. El manifiesto incluye los diagnósticos
de la familia elegida, IDs, evidencia y estado esperado antes/después.
El historial de las cuotas conservadas permanece intacto; se propone eliminar
todo el historial de las cuotas descartadas, sin reconstruir precios desde el JSON.
La estrategia es `oddspapi_debug_line_repair_v1`; los manifiestos antiguos de
selección cacheada no se aceptan con esta versión: generar un nuevo plan.
La exportación no sobrescribe un manifiesto existente: usar otro nombre para una
nueva ejecución. `plan` recorre todas las familias con línea y todas las
deshabilitadas de eventos que tengan cuotas OddspAPI, no solo las ambigüedades
detectadas por `audit`. El estado esperado incluye `enabled_for_ingestion`:
si el flag cambia antes de aplicar una acción, el evento produce `CONFLICT`.

## Aplicar primero en la copia local

Revisar `manifest.json` y `plan_report.json`. El destino local de esta investigación
es `127.0.0.1:5435/sofascore_odds`:

```powershell
.venv/Scripts/python.exe -m scripts.maintenance.backfill_oddspapi_lines apply --manifest exports/oddspapi_line_repair/reviewed/manifest.json --checkpoint exports/oddspapi_line_repair/local/checkpoint.json --results exports/oddspapi_line_repair/local/results.jsonl --expected-db-host 127.0.0.1 --expected-db-port 5435 --expected-db-name sofascore_odds --confirm-write
.venv/Scripts/python.exe -m scripts.maintenance.backfill_oddspapi_lines audit --output exports/oddspapi_line_repair/after
```

`apply` no accede a los JSON debug ni recalcula la selección. Ejecuta las acciones
del manifiesto mediante `BackfillRunner`, con checkpoints y resultados. Cada evento
se aplica en una transacción y se valida contra el estado que leyó `plan`; si los
registros de una familia cambiaron desde entonces, produce `CONFLICT`. Los errores
operativos producen `FAILED` y se puede reanudar.

Solo se eliminan los quotes OddspAPI declarados y sus snapshots. Los choices y
mercados se eliminan si quedan vacíos; otros proveedores conservan sus datos.
Los flags no cambian el reloj de actualización del precio. La caché seleccionada
permanece igual. Las vistas se invalidan con el mecanismo existente.

Repetir el mismo comando reanuda el checkpoint. Para comprobar idempotencia de
la base, repetir con otro par de archivos `--checkpoint` y `--results`: debe
producir `ALREADY_CORRECT`. `--limit N` permite ejecutar un subconjunto de eventos.
La auditoría posterior puede seguir mostrando familias sin una selección actual
válida en las respuestas debug disponibles.

## Llevar el manifiesto al servidor

Desplegar primero la corrección de ingesta y este script. Subir únicamente
`reviewed/manifest.json`; los hashes de debug son evidencia informativa, no archivos
requeridos para aplicar. **No subir el checkpoint local**, porque marcaría eventos
como ya procesados. Usar rutas de checkpoint/resultados exclusivas del servidor.

Con un Python configurado para la base del servidor y desde la raíz del proyecto:

```bash
python -m scripts.maintenance.backfill_oddspapi_lines apply \
  --manifest /ruta/subida/manifest.json \
  --checkpoint /ruta/resultados/server/checkpoint.json \
  --results /ruta/resultados/server/results.jsonl \
  --expected-db-host postgres --expected-db-port 5432 \
  --expected-db-name sofascore_odds --confirm-write
python -m scripts.maintenance.backfill_oddspapi_lines audit \
  --output /ruta/resultados/server/after
```

Dentro de Docker, ejecutar esos comandos con `docker compose exec app python ...`
y rutas accesibles al contenedor. Los tres argumentos de destino deben coincidir
con la URL real de esa ejecución. No se trasladan credenciales en el manifiesto.
Los estados auditados deben coincidir entre copia y servidor; una ingesta posterior
puede generar conflictos que requieren revisar y exportar una nueva corrección.
