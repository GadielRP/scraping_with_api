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

`plan` solo lee la base y los JSON. Reutiliza los mappings, `select_current_lines`,
el extractor de caché y su resolución de donantes. No hace HTTP ni reingesta precios.
El parámetro `--[no-]require-active-quotes` permite reproducir explícitamente la
configuración de selección; por defecto usa la configuración local y la registra
en el manifiesto. La validez individual de los players sigue la política actual.

El directorio predeterminado es `debug/oddspapi_odds_responses`; se puede cambiar
con `--debug-root`. Se reconocen nombres antiguos sin momento/endpoint y se excluyen
los nombres históricos. Cada JSON debe tener `bookmakerOdds` y su fixture debe
pertenecer al evento indicado por el nombre del archivo.

Los momentos T−120, T−30 y T−5 establecen el orden de captura. Un archivo sin
momento es utilizable si es la única captura aplicable; varios momentos desconocidos
o duplicados se reportan como ambiguos. `updatedAt` del fixture y las fechas del
filesystem no se usan como reloj de observación.

La línea principal se obtiene de la última captura válida aplicable a la familia.
Las líneas seleccionadas en capturas anteriores conservan su historial y pasan a
`main_line=false`. Para borrar una alternativa debe estar presente y descartada,
no haber sido seleccionada en otra captura disponible y sus instrumentos deben
coincidir con ticks reales del JSON. Además, todas sus observaciones deben pertenecer
a un único lote verificable compartido con la línea conservada, y sus quotes no
pueden haber sido creados antes de ese lote. Si hay evidencia histórica anterior
o insuficiente, se conserva el historial y se corrigen únicamente los flags.

Sin debug utilizable, una caché inequívoca (propia o donante según la política
existente) solo permite corregir flags. Sin caché válida, sin mercado conservable,
sin outcomes completos o con una captura anterior al último lote persistido, se
omite la familia. No se inventan precios ni observaciones. Los archivos inválidos
también se reportan; un conjunto incompleto no autoriza eliminaciones.

El manifiesto identifica mercados/quotes, línea conservada, selecciones anteriores,
acciones, hashes de evidencia y estados esperados antes y después. `plan_report.json`
contiene el resumen y cada omisión. La exportación no sobrescribe un manifiesto
existente: usar otro nombre para una nueva auditoría.

## Aplicar primero en la copia local

Revisar `manifest.json` y `plan_report.json`. El destino local de esta investigación
es `127.0.0.1:5435/sofascore_odds`:

```powershell
.venv/Scripts/python.exe -m scripts.maintenance.backfill_oddspapi_lines apply --manifest exports/oddspapi_line_repair/reviewed/manifest.json --checkpoint exports/oddspapi_line_repair/local/checkpoint.json --results exports/oddspapi_line_repair/local/results.jsonl --expected-db-host 127.0.0.1 --expected-db-port 5435 --expected-db-name sofascore_odds --confirm-write
.venv/Scripts/python.exe -m scripts.maintenance.backfill_oddspapi_lines audit --output exports/oddspapi_line_repair/after
```

`apply` no accede a los JSON debug ni recalcula la selección. Reutiliza
`BackfillRunner` para bloqueo, checkpoints, resultados y reanudación. Cada evento
es una transacción; primero se comprueban todas sus familias. Un cambio en
identidad, precios, historial o caché produce `CONFLICT`, sin cambios parciales.
Errores operativos producen `FAILED` y detienen el avance; se puede reanudar.

Solo se eliminan los quotes OddspAPI declarados y sus snapshots. Los choices y
mercados se eliminan si quedan vacíos; otros proveedores conservan sus datos.
Los flags no cambian el reloj de actualización del precio. Las correcciones de
caché afectan únicamente a la casa y familia declaradas. Las vistas se invalidan
con el mecanismo existente, sin forzar una reconstrucción en esta herramienta.

Repetir el mismo comando reanuda el checkpoint. Para comprobar idempotencia de
la base, repetir con otro par de archivos `--checkpoint` y `--results`: debe
producir `ALREADY_CORRECT`. `--limit N` permite ejecutar un subconjunto de eventos.
La auditoría posterior puede seguir mostrando familias sin evidencia suficiente;
no se considera correcto resolverlas mediante una elección arbitraria.

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
  --expected-db-host sofascore-pg --expected-db-port 5432 \
  --expected-db-name sofascore_odds --confirm-write
python -m scripts.maintenance.backfill_oddspapi_lines audit \
  --output /ruta/resultados/server/after
```

Dentro de Docker, ejecutar esos comandos con `docker compose exec app python ...`
y rutas accesibles al contenedor. Los tres argumentos de destino deben coincidir
con la URL real de esa ejecución. No se trasladan credenciales en el manifiesto.
Los estados auditados deben coincidir entre copia y servidor; una ingesta posterior
puede generar conflictos que requieren revisar y exportar una nueva corrección.
