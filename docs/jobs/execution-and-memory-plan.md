# Plan de implementación: ejecución de jobs y memoria acotada

Estado: diseño implementado y validado con proveedores sintéticos y PostgreSQL real. Los resultados y límites de capacidad están en `execution-validation.md`; la validación del servidor/proveedores reales permanece operativa, no se presume por pasar tests.
Fecha: 2026-10-03. Alcance: scheduler, pre-start, discovery, resultados, reporting y recursos compartidos.

## 1. Objetivo y decisiones

Separar el despacho de la ejecución, preservar la prioridad temporal de pre-start y evitar que el volumen diario determine cuántos payloads permanecen en RAM. La separación por hilos ya existe parcialmente; no se partirá de cero ni se añadirá un worker por deporte.

Decisiones:

- Un proceso Python, un scheduler ligero y tres ejecutores de un hilo: pre-start, cierre T−1 y mantenimiento. PostgreSQL sigue separado. Los pools internos también cuentan para el presupuesto de concurrencia.
- Mantenimiento ejecuta una sola tarea pesada a la vez. Discovery, results y refresh comparten ese ejecutor; sus dependencias se coordinan explícitamente.
- El scheduler conoce horarios y descripciones de trabajo. No conoce repositorios, SQL, proveedores, estados de eventos ni recuperación específica de Oddspapi.
- CLI y scheduler invocan los mismos casos de uso. La CLI no construye un scheduler para ejecutar un job manual.
- Reutilizar `shared/batching.py`, los escritores de eventos/resultados y las protecciones de identidad/descarte. No crear un segundo mecanismo de batch upsert.
- El pipeline debe limitar datos desde HTTP hasta la persistencia; limitar únicamente el INSERT es insuficiente.
- No mantener alias, rutas dobles o wrappers para preservar APIs internas antiguas. Migrar todos sus consumidores en la misma fase.
- Los tests existentes no condicionarán la arquitectura. Se reemplazarán los que verifiquen contratos retirados; sí se verificará la integridad de datos y los nuevos contratos.
- No cambiar en esta iniciativa qué competiciones se ingieren ni qué categorías de resultados se eliminan. Separar esas políticas de la ejecución.

## 2. Hallazgos que determinan el diseño

| Ubicación actual | Problema observado | Tratamiento |
|---|---|---|
| `infrastructure/scheduler/job_scheduler.py` | Registra horarios, ejecuta pre-start, gestiona estado de eventos, recupera Oddspapi, ejecuta SQL y ofrece wrappers CLI | Reducir a lifecycle y despacho; extraer responsabilidades concretas |
| `infrastructure/scheduler/maintenance_executor.py` | Ya limita mantenimiento a un worker y ocho claves, pero solo conoce acciones sin identidad temporal | Evolucionar su implementación hacia un ejecutor serial acotado, reutilizado por los tres canales |
| `modules/jobs/pre_start_check_job/*` | Recibe `scheduler` para acceder a repositorios, eventos reprogramados y thread de OddsPortal | Dependencias explícitas y estado propio de pre-start/proveedor |
| `modules/jobs/daily_discovery/extractor.py` | Mezcla fetch, filtrado, acumulación por deporte, odds, escritura y estados | Separar fuente de eventos y coordinador; retirar `all_events` |
| `modules/jobs/daily_discovery/persistence.py` | Escritura acotada útil, pero ubicación específica aunque hay consumidores de otros discoveries | Mover el comportamiento común a un paquete de ingestión de discovery |
| `modules/jobs/discovery_persistence_summary.py` | Guarda un diccionario con todos los IDs del run para deduplicar por fecha | Mantener exactitud sin retener todos los IDs/payloads en RAM |
| `ResultRepository.pending_batches` | Buena lectura keyset; mezcla política temporal de deportes con SQL y usa tamaño de escritura como tamaño de lectura | Mantener keyset, recibir selección y tamaño explícitos |
| `run_results_collection_job.py` | Fetch/clasificación/escritura/observaciones/contadores en una función; `failed` incluye eventos no acabados | Separar resolución y coordinación del lote; resultados tipados |
| `shared/runtime_observability.py` | Una sola `active_operation` global y sampler por operación | Registro concurrente de operaciones con un sampler de proceso |
| `shared/shutdown.py` | Mezcla cancelación con prioridad HTTP mediante `background_work` | Contexto de ejecución explícito; shutdown solo coordina cierre |
| `views/view_manager.py` | Refresh de dos vistas en una llamada; debug añade conteos de otras vistas | Ejecución e instrumentación por vista; diagnóstico costoso separado |
| `compose.prod.yaml` / `database.py` | App hasta 768 MiB, PostgreSQL sin límite de contenedor, pool sin presupuesto explícito | Presupuesto conjunto medido y configuración coherente |

Evidencia de producción: resultados del 3 de octubre completaron 2.501 candidatos en 766,717 s; persistencia consumió 8,267 s. Pre-start se ejecutó durante midnight y daily. El OOM global fue posterior, durante un refresh sin finalización registrada. No existe evidencia suficiente para atribuir toda la memoria al refresh.

## 3. Arquitectura y límites de responsabilidad

```text
app/runtime.py                  composición y lifecycle de dependencias
  ├─ app/cli.py                 invoca casos de uso manuales
  └─ infrastructure/scheduler  calendario → JobRequest → ejecutor asignado
        ├─ pre_start           una ejecución + como máximo un tick pendiente
        ├─ closing             T−1 con identidad de slot y vencimiento
        └─ maintenance         cola acotada; una tarea activa

modules/jobs/<caso de uso>      reglas, secuencia y resultados del trabajo
modules/sofascore              transporte y adaptación del proveedor
infrastructure/persistence     selección SQL, transacciones y almacenamiento
shared                         primitivas sin conocimiento de jobs/proveedores
```

Aplicación de SOLID: cada módulo tiene un motivo concreto de cambio; los casos de uso reciben dependencias estrechas; los repositorios no deciden horarios ni prioridad; el ejecutor no interpreta resultados deportivos. Usar dataclasses, funciones y Protocols pequeños solo en fronteras que necesiten sustitución. No introducir un framework de plugins, un contenedor DI, una clase base para todos los jobs ni una interfaz por cada función.

### Módulos a crear o transformar

| Ubicación destino | Responsabilidad y procedencia |
|---|---|
| `app/runtime.py` | Construye clientes, servicios, repositorios, ejecutores y scheduler; reemplaza singleton con efectos al importar |
| `infrastructure/scheduler/contracts.py` | `JobRequest`, canal, identidad, `scheduled_at`, vencimiento, resultado de admisión |
| `infrastructure/scheduler/serial_executor.py` | Evolución de MaintenanceExecutor: admisión, deduplicación, ejecución, coalescencia y shutdown |
| `infrastructure/scheduler/schedules.py` | Registro de horarios en una instancia privada de `schedule.Scheduler`; sin ejecutar casos de uso |
| `infrastructure/scheduler/job_scheduler.py` | Loop de despacho y lifecycle; recibe registro/ejecutores construidos por app |
| `shared/execution_context.py` | Identidad de run, cancelación, deadline y clase de prioridad; propagación a threads hijos |
| `modules/jobs/pre_start_check_job/runtime.py` | Dependencias explícitas y registro de reprogramaciones con expiración y acceso sincronizado |
| `modules/jobs/pre_start_check_job/budget.py` | Presupuesto temporal, prioridad por vencimiento y comprobación antes de iniciar otra unidad |
| `modules/jobs/oddspapi/fixture_discovery/recovery.py` | Calendario perdido y recuperación durable que hoy viven en JobScheduler |
| `modules/jobs/daily_discovery/persistence.py` | Normalización y contadores de un lote diario ya filtrado, antes de abrir la transacción de eventos |
| `modules/jobs/discovery/fetching.py`, `persistence.py`, `summary.py` | Consulta concurrente acotada, políticas explícitas de admisión/persistencia y presentación de la auditoría |
| `modules/jobs/daily_discovery/event_source.py` | Iteración de torneos/eventos y filtros del proveedor, sin escrituras |
| `modules/jobs/daily_discovery/run_daily_discovery.py` | Coordinación heartbeat/slot/deporte, limpieza y progreso; absorbe el coordinador del extractor |
| `modules/sofascore/streaming.py` | Adaptación incremental de respuestas voluminosas, reutilizando transporte autenticado/reintentos existentes |
| `infrastructure/persistence/transient/discovery_run_store.py` | Almacenamiento temporal en disco de IDs confirmados y calendario final por run; sin ORM ni payloads completos |
| `modules/jobs/results_collection_job/contracts.py` | Selección temporal y outcomes del procesamiento, sin SQL |
| `modules/jobs/results_collection_job/batch_processor.py` | Resolución de un lote y aplicación de escrituras existentes |
| `modules/jobs/reporting_refresh/run_reporting_refresh.py` | Caso de uso de refresh y recuperación; independiente de daily |
| `infrastructure/persistence/repositories/reporting_refresh_repository.py` | Estado durable mínimo por vista: requested/completed generation, intentos y próximo intento |
| `infrastructure/settings/job_execution.py` | Configuración tipada de límites/ejecución; defaults únicos, lectura centralizada del entorno |

La tabla es el diseño objetivo, no una obligación de crear archivos vacíos. Si una responsabilidad queda en pocas líneas cohesionadas con otra, mantenerla en el módulo correspondiente; no crear forwarding wrappers.

## 4. Despacho y concurrencia

### Contrato de admisión

- `JobRequest`: job key, argumentos pequeños, `scheduled_at` UTC, canal, deadline opcional. Nunca contiene listas de eventos ni sesiones DB.
- Estados de admisión explícitos: accepted, coalesced, expired, queue_full, shutting_down. Un rechazo no se registra como éxito del job.
- Pre-start: una ejecución activa; múltiples ticks durante ella se consolidan en el más reciente. Al terminar, se recalculan candidatos para el momento actual. No reproducir una lista de eventos capturada cinco minutos antes.
- T−1: conservar el slot original, descartar trabajo vencido y registrar qué ventana se perdió. No convertir un slot viejo en el actual. No acumular una cola histórica.
- Mantenimiento: conservar inicialmente capacidad de ocho identidades, con una sola tarea activa. Identidad incluye fecha/slot cuando es parte de la semántica; no solo nombre de función.
- Refresh puede consolidar solicitudes por vista/generación. Una fecha de resultados pendiente no se reemplaza por otra fecha distinta.
- Al llenarse la cola, los trabajos con estado durable continúan pendientes para un próximo despacho. Para discoveries sin estado durable se registra el rechazo y se reintenta con una única descripción acotada, no con payloads.
- Horarios y recuperaciones producen requests; ninguna operación HTTP, SQL o recuperación de slots corre dentro del loop del scheduler.

### Seguridad de concurrencia

- Una sesión SQLAlchemy por unidad de trabajo, nunca una sesión compartida entre hilos. No pasar objetos ORM con lazy loads fuera de la sesión: devolver DTOs/proyecciones con campos requeridos.
- Trasladar `_active_op_thread` al componente que administra el worker de OddsPortal. Evitar que pre-start cree scrapers que sobrevivan sin control a la siguiente ejecución.
- Auditar mutaciones del cliente compartido: flags de diagnóstico, rotación de sesión y evidencia deben ser locales a request/run cuando corresponda. `set_challenge_evidence_enabled` global no debe cambiar el comportamiento del otro worker.
- Revisar el advisory lock actual de T−1: bloquea ejecuciones simultáneas, pero no acredita por sí mismo ejecución única histórica tras liberar el lock. Separar exclusión y registro de slot consumido si se requiere deduplicación entre contenedores.
- Un lock de sesión de PostgreSQL requiere conexión dedicada y liberación garantizada; no retener una transacción abierta durante HTTP. Contabilizar esa conexión dentro del pool.
- No aumentar los pools de proveedores; inventariar y limitar futures pendientes. Propagar el contexto de prioridad/cancelación existente a sus tareas.
- Shutdown: detener admisión, cancelar requests aún no iniciados, señalar activos, cerrar respuestas HTTP y esperar una frontera de commit. Usar timeouts de red/SQL; no intentar matar threads arbitrariamente.

## 5. Daily discovery: memoria desde la fuente hasta el commit

### Flujo propuesto

1. Ejecutar cleanup de discard memory al entrar al heartbeat, antes de cache/slot, como hoy; conservar toggle y retención de tres días.
2. Resolver fecha/slot una vez. Inicializar estados mediante un upsert de conjunto; no SELECT previo por deporte.
3. Cargar el scope existente una vez por run. Distinguir scope vacío deliberado de error de acceso: un error no equivale a “no hay pendientes”.
4. Iterar páginas de torneos y eventos. Filtrar y llenar un lote máximo de 100 eventos, sin `all_events`.
5. Normalizar y persistir el lote mediante el escritor actual. Registrar exclusivamente eventos confirmados en el almacén temporal del run. Liberar referencias al lote.
6. Al terminar los eventos de un deporte, recorrer su respuesta de odds de manera incremental. Normalizar solo una tanda; resolver sus IDs en conjunto contra los eventos confirmados del run y el estado actual en DB.
7. Persistir odds únicamente para los eventos elegibles que siguen cumpliendo el umbral temporal. Reutilizar el servicio de ingestión de odds, sin añadir una petición por evento como sustituto del endpoint masivo.
8. Marcar el deporte completado solo cuando sus operaciones requeridas terminaron. Una respuesta incompleta, cancelación o error deja trabajo reintentable y conserva lo ya confirmado.
9. Emitir resumen exacto por fecha/deporte y solicitar reporting después de las escrituras. Cerrar el almacén temporal.

### Respuestas HTTP grandes

Un generador sobre `response.json()` sigue cargando el documento completo. Implementar lectura incremental específica para los endpoints masivos usando el transporte existente; revisar antes su soporte real de streaming, compresión y cierre. No duplicar headers, proxies, circuit breaker o reintentos en streaming.py.

El parser debe validar la estructura y distinguir JSON válido vacío de respuesta truncada. Ante un corte después de emitir eventos, se permite repetir la página mediante upserts idempotentes; no marcarla completa. Cerrar siempre el response. Aplicar límite de bytes y tamaño de un elemento; un elemento excesivo se registra como error reintentable/diagnosticable, nunca como colección vacía exitosa.

Si el transporte actual obliga temporalmente a materializar una respuesta, medir y documentar esa excepción; no declarar memoria acotada end-to-end hasta sustituirla. No añadir un segundo cliente HTTP para eludir este trabajo.

### Exactitud de conteos sin diccionarios ilimitados

El resumen actual necesita recordar la última fecha/deporte por ID, y el join con odds necesita saber qué se confirmó en este run. Usar un almacén efímero SQLite en un directorio temporal de disco, con caché acotada, no `:memory:` ni un tmpfs.

- Clave canónica y clave del proveedor; fecha/deporte final; solo escalares necesarios. Actualización por lote tras commit de eventos.
- Consulta de pertenencia por lote y agregación final mediante SQL. Esto preserva `persisted_unique` incluso si el proveedor repite un evento o cambia su fecha en el mismo run.
- No es una nueva fuente de verdad ni un checkpoint durable de negocio. Si falla, registrar auditoría incompleta y dejar el run reintentable; no inventar conteos ni deshacer eventos ya confirmados.
- Lifecycle por run con cierre y borrado normal; al iniciar, limpiar artefactos huérfanos de runs terminados/interrumpidos mediante un directorio dedicado y un lock de propietario. No borrar archivos de un proceso activo.
- Medir coste de disco. Esta pieza evita que una necesidad de observabilidad obligue a retener O(N) IDs en RAM.

Objetivo de memoria de daily: O(lote + buffer HTTP + elemento máximo + cachés acotadas), no O(total de eventos del deporte). El catálogo de competiciones/torneos también se pagina o acota; no ignorarlo en la medición.

### Reanudación

Primera implementación: conservar progreso durable por deporte del repositorio existente y repetición idempotente del deporte incompleto. No prometer reanudación exacta por torneo. Los commits son por lote; la unidad de retry inicial es el deporte.

El runner de `infrastructure/persistence/backfill/` existe, pero está acoplado a manifiestos inmutables y estrategias de backfill. No reutilizarlo como scheduler de calendarios mutables. Reutilizar primitivas únicamente cuando tengan el mismo contrato.

## 6. Result collection y repositorios

- Conservar el keyset por ID, el límite superior fijo y las proyecciones ligeras de `pending_batches`. Recibir límites UTC y tamaño de lectura explícitos; la política “finished según deporte” pertenece al caso de uso.
- Separar tamaño de lectura HTTP y escritura SQL. Valor inicial de ambos: 100; no obligar a que permanezcan iguales.
- Extraer el cuerpo de `_collect_batch` a `batch_processor.py`. Outcomes: persisted, deleted, deferred_not_finished, missing_mapping, ambiguous_mapping, provider_error, persistence_conflict. Contabilización exhaustiva por candidato.
- No cambiar `results_parser` ni ampliar los descartes memorizados. El outcome explica el parser, no lo reemplaza.
- Mantener validación de identidad de respuesta, expected canonical IDs, locks y evidencia de borrado. Una respuesta tardía no puede recrear un evento eliminado.
- Revisar el criterio “existe cualquier Result”: definir según el esquema y parser qué representa un resultado completo. Si hay filas parciales, deben ser candidatas cuando corresponda; no asumir que `home_score IS NULL` es una política universal para todos los deportes.
- Reutilizar `batch_upsert_results`, pero devolver IDs realmente confirmados. Generar observaciones solo para esos IDs. Un conteo no permite identificar cuál dejó de existir durante una carrera.
- Las escrituras de metadata, resultados y borrados siguen siendo transacciones separadas y acotadas; documentar recuperación idempotente de estados intermedios. No afirmar atomicidad del lote completo.
- Decidir explícitamente la garantía de observaciones: si deben recuperarse tras crash posterior al commit de resultados, persistir una marca de pendiente en la misma transacción y consumirla en lotes. No declarar completitud con el best-effort actual. Este subpaso exige revisar consumidores antes de cerrar el contrato.
- No introducir un cursor durable global que omita fallos anteriores. Reiniciar la misma fecha vuelve a seleccionar pendientes y excluye resultados completos ya confirmados.
- No sumar requests de resultados en paralelo en esta iniciativa. El limitador HTTP y la prioridad temporal explican gran parte de la duración.

### SQL e índices

Capturar `EXPLAIN (ANALYZE, BUFFERS)` sobre copia representativa para selección de pendientes, mappings, upcoming y vistas. No ejecutar pruebas pesadas libremente en producción.

Revisar índices existentes antes de añadir otros: `events(starts_at)`, `events(sport, starts_at)`, PK/índice de `results(event_id)` y mappings. Un índice de fecha/ID no mejora automáticamente un keyset ordenado solo por ID. Comparar el plan con una alternativa ordenada por `(starts_at,id)` y adoptar solo la que conserve corrección y mejore lecturas medidas. No duplicar índices por intuición.

Eliminar N+1 dentro de cada lote en los caminos tocados; no convertir un refactor de scheduler en reescritura general de todos los repositorios. Separar módulos de lectura/escritura solo donde ya existan responsabilidades sustanciales, no para reemplazar una función con tres delegaciones.

## 7. Reporting: recuperación, aislamiento y bloqueos

Retirar `_reporting_refresh_pending` del scheduler. El coordinador de reporting tendrá estado durable por vista: generación solicitada, generación completada, intento, próximo intento y último error.

- Registrar invalidación con las escrituras relevantes, en su transacción cuando deba garantizarse recuperación tras crash. No depender exclusivamente de un callback después del commit.
- Capturar generación objetivo al iniciar. Al terminar, completar únicamente esa generación: nuevas escrituras durante el refresh permanecen pendientes. Consolidar múltiples invalidaciones.
- Scheduler consulta el estado a través del caso de uso en mantenimiento, con backoff; no espera al siguiente daily de ocho horas. No hacer consultas DB dentro del loop de despacho.
- Un solo refresh activo, vistas secuenciales, registro antes/después de cada una. Retirar los conteos de vistas ordinarias del debug normal; conservarlos solo en comando de diagnóstico explícito.
- Mantener las funciones privilegiadas con allowlist y grants mínimos. Crear una migración nueva; no editar migraciones históricas ni dar ownership de vistas al rol app.
- Revisar dependencias de consumidores antes de dividir el commit actual: dos commits por vista permiten recuperación independiente, pero exponen generaciones distintas entre vistas. Si un consumidor necesita consistencia conjunta, conservar la transacción conjunta y aun así medir cada sentencia.
- El refresh normal bloquea lectores de la vista. Por ello separar threads no basta: medir espera de pre-start por locks. Evaluar `CONCURRENTLY` con índices únicos, vistas pobladas y permisos válidos; adoptar tras ensayo de RAM/tiempo/disco. No es un ahorro de memoria automático.
- Usar `SET LOCAL` de límites/timeout por operación, no elevar `work_mem` global para acelerar el refresh. Aplicar inicialmente ejecución SQL sin workers paralelos al refresh de la prueba de 1 GB y comparar.
- El requisito de aceptación es que reporting no provoque incumplimientos de pre-start por locks o recursos. Si refresh concurrente no cabe, no desplegar el refresh bloqueante como solución “aislada”: ajustar la consulta, ventana o capacidad y volver a validar.

## 8. Pre-start y prioridad de recursos

Además de mover su ejecución, retirar su dependencia del scheduler. `runtime.py` aporta servicios concretos y estado de reprogramaciones; el worker de OddsPortal administra su propio lifecycle.

- Ordenar trabajo por vencimiento real. Separar tareas de mantenimiento intradía de captura crítica sin retrasar correcciones necesarias para decidir el kickoff actual.
- Medir por fase selección, correcciones, proveedores y evaluación. Introducir deadline cooperativo antes de iniciar otra unidad; no cortar una transacción ni descartar silenciosamente trabajo.
- No compartir flags mutables de diagnóstico entre T−1 y pre-start.
- No ejecutar actualización de cuotas de cuenta Oddspapi antes de cada trabajo crítico; recuperarla fuera de ese camino, manteniendo el estado necesario para selección de claves.
- Revisar política de endpoints ausentes: no implementar TTL global arbitrario que oculte odds que aparezcan cerca del inicio. Aplicar cooldown por proveedor/evento y reservar un intento en momentos críticos, con métricas de supresión y recuperación.
- El presupuesto de HTTP distingue cierre, pre-start y mantenimiento. Prioridad no significa capacidad infinita; mantener límite total y medir espera. Evitar inanición silenciosa de mantenimiento: pendiente antiguo se expone y alerta, no se oculta como éxito.
- No pausar por RSS de Python de forma indefinida: el allocator puede retener memoria reutilizable. Para presión sostenida, terminar una unidad tras commit, liberar recursos y reprogramar; medir memoria disponible del host/cgroup además del RSS.

## 9. Configuración y presupuesto del servidor

`infrastructure/settings/job_execution.py` define defaults, tipos y validación una sola vez. `.env` contiene overrides operativos; los casos de uso reciben configuración ya resuelta. Eliminar fallbacks encadenados de `getattr` y aliases de variables retiradas en el alcance migrado.

Parámetros a definir: capacidad de mantenimiento, tamaños de lectura/escritura, deadline por ciclo, límite de respuesta/elemento, cache temporal, retry/backoff de reporting, pool DB y timeouts. Número de workers de cada canal: uno por diseño inicial, sin un toggle que permita concurrencia insegura accidentalmente.

Presupuesto de prueba, no recomendación ya validada: app 384 MiB, PostgreSQL 384 MiB y aproximadamente 189 MiB restantes sobre los 957 MiB observados. Comprobar además consumo del sistema, page cache y picos; si no cabe, reducir huellas o aumentar capacidad antes de desplegar. No inferir que estos límites garantizan estabilidad ni provocar OOM de PostgreSQL en producción para probarlos.

- Pool DB explícitamente acotado, `max_overflow=0`, timeout finito. Calcular conexiones según ejecutores, scrapers y locks; verificar que mantenimiento no acapare las necesarias para cierre.
- `shm_size` es un máximo de espacio compartido, no una reserva de RAM. Corregir el comentario actual que da por suficiente 512 MiB en un host de 1 GB.
- Perfil de prueba limita el conjunto app + DB dentro de una máquina/cgroup padre de 1 GB. `compose.memory-test.yaml` actual solo limita app y no reproduce el OOM global.
- Swap, si se habilita operativamente, es amortiguación; no cuenta como RAM disponible para cumplir deadlines. No modificar el droplet como parte de escribir este plan.

## 10. Observabilidad y criterios de aceptación

Reemplazar `active_operation` única por un registro con `run_id`, job, canal, thread, fase y comienzo. Un sampler recoge memoria de proceso/cgroup/host y asocia las operaciones activas; no atribuir RSS del proceso a un job como consumo exclusivo.

Cada ejecución registra: scheduled_at, admitted_at, started_at, dispatch_lag, queue_wait, duración, causa de diferimiento/error, lotes y conteos confirmados. No volcar payloads completos ni listas ilimitadas de IDs.

Validación obligatoria de los nuevos contratos:

1. Con daily/midnight bloqueados artificialmente, el scheduler sigue despachando pre-start/T−1. Con pre-start largo, mantenimiento puede admitirse. Latencia de despacho objetivo en prueba controlada: menos de un segundo, separada de latencia real del proveedor.
2. Nunca dos ejecuciones activas del mismo canal; cola acotada, slots vencidos explícitos y ninguna acumulación de payloads en requests.
3. Fixtures de 1k, 10k y 50k eventos equivalentes: medir pico y pendiente de memoria. El tamaño de buffers vivos no crece con N; el RSS puede tener un escalón de allocator. Publicar medidas antes/después, no solo un “pasó”.
4. Daily: duplicados, cambio de fecha, respuesta truncada, error de odds, error DB y reinicio. Conteos únicos correctos y deporte incompleto reintentable.
5. Resultados: eventos eliminados/remapeados concurrentemente, resultados parciales, partido en curso y crash entre transacciones. No recreación accidental, no observación de resultado no confirmado y reintento correcto.
6. Refresh: invalidación durante ejecución, fallo entre vistas, reinicio y comparación de locks/modos. No pérdida de solicitudes ni espera hasta el siguiente daily.
7. Prueba integrada con PostgreSQL real y dataset representativo, limitada a 1 GB conjunto: pre-start + daily/resultados + refresh. Sin alertas externas ni eliminación de resultados de producción.
8. Repetir varios ciclos para detectar retención, pools/hilos huérfanos y recuperación. El arranque de pre-start a tiempo no sustituye verificar finalización antes del momento del evento.
9. Contratos de shutdown y fallo: no mantener transacciones durante HTTP, no declarar éxito tras excepción y no bloquear indefinidamente el cierre.

## 11. Secuencia de implementación

| Fase | Entregable | Condición para avanzar |
|---|---|---|
| 0 | Inventario de consumidores, consultas, pools y baseline reproducible; documentación de contratos | Dependencias y métricas verificadas; sin cambios productivos |
| 1 | Contexto de ejecución y observabilidad concurrente | Operaciones simultáneas visibles; cancelación propagada |
| 2 | Composición app, scheduler ligero y ejecutores; extracción de recuperación Oddspapi/estado pre-start | Pruebas de despacho, coalescencia y shutdown |
| 3 | Pipeline daily incremental, ingestión común y almacén temporal | Pruebas de exactitud y memoria 1k/10k/50k |
| 4 | Resultados con outcomes/IDs confirmados y consultas explícitas | Integridad/reanudación en PostgreSQL; planes medidos |
| 5 | Reporting durable, por vista, privilegios/migración y recuperación | Locks y consumo compatibles con pre-start |
| 6 | Presupuestos de pre-start/proveedores, pools y perfil de 1 GB | Prueba integrada de carga y varios ciclos |
| 7 | Retirada inmediata de código reemplazado, actualización de CLI/docs y despliegue revisable | Búsqueda de referencias huérfanas, contratos verificados y Graphify actualizado |

Las fases se pueden revisar en commits separados, pero no desplegar solo más concurrencia antes de limitar memoria. La capacidad operativa final depende de la prueba integrada. Si 1 GB no satisface el objetivo con margen, documentar el límite medido y cambiar capacidad o alcance; no declarar solucionado por haber pasado tests unitarios.

## 12. Eliminaciones y único cleanup posterior

Eliminar durante esta implementación, una vez migrados consumidores:

- `maintenance_executor.py` reemplazado por `serial_executor.py`; no conservar ambos.
- `DailyDiscoveryExtractor` y su archivo si ya no conserva responsabilidad propia.
- Wrapper singular `persist_event_and_optional_odds` y wrappers de `discovery_optimization.py` que solo delegan, tras trasladar sus consumidores al servicio común. Conservar funciones con política real.
- `run_daily_discovery_retry_job`, `job_daily_discovery_retry`, wrappers `run_job_*_now` y wrappers vacíos de resultados: registros/CLI apuntan a casos de uso con parámetros explícitos.
- `_reporting_refresh_pending`, estado de eventos y lógica de recuperación de proveedores en JobScheduler.
- Singleton de scheduler creado al importar, registro global de `schedule` y ejecuciones largas dentro de callbacks.
- Uso de “background” como sustituto de prioridad y cancelación; todos los consumidores se migran al contexto explícito.
- Retornos `[]`, `False` o `{}` que convierten fallos de repositorio en ausencia de trabajo, dentro de los repositorios intervenidos.
- Conteos pesados automáticos por `global_debug_mode` y logs de payloads completos en el camino de ingestión.

Crear al implementar `docs/maintenance/execution-legacy-cleanup.md`, una sola lista de seguimiento con ID, símbolo/ruta, consumidor restante, motivo de permanencia y condición verificable de retirada. En código propio pendiente usar `LEGACY(EXEC-xxx)` enlazado a esa lista. No marcar como legacy todo módulo que no se haya refactorizado.

Candidatos a esa única tarea posterior, sujetos a confirmar referencias y datos:

| ID | Candidato | Condición de retirada |
|---|---|---|
| EXEC-001 | `_build_event_data_with_legacy_fallback` y export de compatibilidad `NBA_SEASONS` | Backfill de datos normalizados completo y todos los consumidores migrados; no perder datos por retirar el fallback antes |
| EXEC-002 | Etiquetas históricas AM/PM de DailyDiscoveryLog | Migración explícita de filas, configuración y referencias; nunca reinterpretar slots existentes silenciosamente |
| EXEC-003 | Funciones SQL antiguas de refresh/diagnóstico | Nuevos callers/grants desplegados y ausencia de scripts consumidores; retirada por nueva migración |
| EXEC-004 | APIs singulares antiguas de repositorio aún usadas fuera del alcance | Migrar consumidores concretos y eliminar en una sola pasada; registrar la lista real, no estimada |

Esta tarea posterior será un único trabajo de código/documentación, no un job recurrente del scheduler. Las migraciones históricas quedan como historia válida, no se borran por contener SQL antiguo. No diferir a cleanup el código que este mismo cambio deja sin consumidores.

## 13. Fuera del alcance inmediato

- Cambiar la lista de competiciones o la política de descarte.
- Workers por deporte, multiprocessing, Redis/Celery o separación en microservicios.
- Reconciliación automática de días anteriores tras discovery: sigue siendo necesaria para cobertura, pero debe diseñarse como caso de uso posterior reutilizando el colector acotado, no añadirse incidentalmente a este refactor.
- Reescribir pilares, predicción o todos los repositorios no implicados.

## Referencias de diseño verificadas

- SQLAlchemy: sesión por hilo/unidad de trabajo, no Session compartida: https://docs.sqlalchemy.org/en/20/orm/session_basics.html
- PostgreSQL: requisitos y comportamiento de refresh concurrente: https://www.postgresql.org/docs/15/sql-refreshmaterializedview.html
- PostgreSQL: consumo por operación/worker y límites de memoria: https://www.postgresql.org/docs/15/runtime-config-resource.html
- Evidencia local de producción: `scratch/production-log-review-20261003/evaluacion.md` y `server-memory-evidence.txt` (artefactos de investigación, no fuente versionada requerida).
