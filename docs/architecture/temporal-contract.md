# Contrato temporal del sistema

## Estado y alcance

Decisión aceptada para los instantes de inicio de evento. Este documento no
convierte automáticamente todos los timestamps de auditoría del sistema; esos
campos se clasificarán en iniciativas posteriores.

## Vocabulario

- **Instante:** un punto inequívoco de la línea de tiempo. En Python siempre es
  un `datetime` timezone-aware y, dentro del dominio, se normaliza a UTC.
- **Fecha civil (`LocalDate`):** una fecha de calendario, por ejemplo
  `2026-09-16`, cuyo significado depende de una zona de negocio.
- **Hora civil (`LocalTime`):** una hora recurrente, por ejemplo `03:00`, que
  no identifica por sí sola un instante.
- **Fecha zonificada (`ZonedDateTime`):** la representación de un instante en
  una zona IANA, usada en calendarios y presentación.

## Invariantes

1. `Event.start_time_utc` representa un instante, no una hora civil.
2. El modelo de aplicación acepta y devuelve ese valor como UTC aware.
3. PostgreSQL lo persiste como `TIMESTAMP WITH TIME ZONE` (`timestamptz`).
4. Un `datetime` naive no puede cruzar un límite de dominio o persistencia.
5. Unix timestamps se interpretan directamente como UTC.
6. `Config.TIMEZONE` controla calendarios locales, horarios recurrentes y
   presentación. Cambiarla no cambia ningún instante persistido.
7. Las consultas por “día local” calculan primero el intervalo semiabierto
   `[inicio_local, siguiente_inicio_local)` y luego lo convierten a UTC.

El sufijo histórico `_utc` se conserva para evitar una migración transversal;
ahora coincide con el contrato real del valor.

## Límites de conversión

- Proveedor -> dominio: parsear una vez, exigir offset o usar el contrato
  documentado del proveedor, y normalizar con `shared.temporal.as_utc`.
- Dominio -> base: `UTCDateTime` rechaza valores naive.
- Base -> dominio: `UTCDateTime` devuelve valores UTC aware.
- Dominio -> interfaz: convertir a `Config.TIMEZONE` únicamente para mostrar o
  aplicar una regla de calendario.
- Legacy -> dominio: `interpret_local_naive` solo se permite cuando el código
  declara explícitamente la zona que dio significado al valor antiguo.

## Migración de datos existente

La columna `events.start_time_utc` contiene, antes de esta migración, horas
civiles naive de `America/Mexico_City`. Se convierte in-place con la semántica:

```sql
start_time_utc AT TIME ZONE 'America/Mexico_City'
```

No se usa un offset fijo. PostgreSQL debe aplicar las reglas históricas de la
zona IANA para cada fecha. La migración calcula previamente el epoch esperado,
cambia el tipo y exige que el epoch resultante coincida para cada fila dentro
de la misma transacción.

`event_source_resolution_queue.source_start_time_utc` tiene una procedencia
distinta: sus valores legacy fueron producidos desde Unix timestamps como UTC
naive, por lo que esa columna se interpreta explícitamente con `UTC`.

La vista legacy `basketball_results_season_year` no forma parte de los read
models actuales, no tiene consumidores en el repositorio ni dependencias en
PostgreSQL y se retira explícitamente durante esta migración. Se elimina sin
`CASCADE` para que cualquier dependencia desconocida en otro entorno detenga
la operación en vez de ser borrada silenciosamente.

## Despliegue

La conversión in-place requiere una ventana de mantenimiento porque PostgreSQL
puede bloquear la tabla durante el cambio de tipo. El orden seguro es:

1. detener writers y schedulers con el contrato antiguo;
2. tomar respaldo y registrar conteo, mínimo y máximo;
3. desplegar este código e iniciar una sola instancia para ejecutar la
   migración;
4. comprobar que la migración y la recreación de vistas terminaron;
5. ejecutar consultas de humo de eventos próximos y del job pre-start;
6. reanudar el resto de instancias y schedulers.

El runner es idempotente: si la columna ya es timezone-aware no vuelve a
convertirla. La aplicación configura cada conexión PostgreSQL en UTC, de modo
que lecturas y SQL de diagnóstico respeten el nombre `start_time_utc`.
