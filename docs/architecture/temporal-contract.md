# Contrato temporal del sistema

## Estado

Contrato aplicado a todos los instantes persistidos y verificado en PostgreSQL
el 2026-09-17. El esquema no contiene columnas `timestamp without time zone`.

## Vocabulario

- **Instante:** un punto inequívoco de la línea de tiempo. En Python es un
  `datetime` timezone-aware y se normaliza a UTC al entrar al dominio.
- **Fecha civil (`LocalDate`):** una fecha de calendario cuyo significado
  depende de una zona de negocio.
- **Hora civil (`LocalTime`):** una hora recurrente que por sí sola no
  identifica un instante.
- **Fecha zonificada (`ZonedDateTime`):** un instante representado en una zona
  IANA; se usa para calendario y presentación, no como formato de persistencia.

## Invariantes

1. Toda columna temporal del esquema representa un instante absoluto.
2. Los modelos aceptan y devuelven `datetime` aware normalizados a UTC.
3. PostgreSQL persiste esos valores como `TIMESTAMP WITH TIME ZONE`
   (`timestamptz`).
4. Un `datetime` naive no puede cruzar un límite de dominio o persistencia.
5. Unix timestamps se interpretan directamente como UTC.
6. `Config.TIMEZONE` controla calendarios locales, horarios recurrentes y
   presentación. Cambiarla no cambia ningún instante persistido.
7. Las consultas por día local construyen el intervalo semiabierto
   `[inicio_local, siguiente_inicio_local)` y lo convierten a UTC.

## Responsabilidades de los módulos

`shared/temporal.py` contiene las primitivas temporales independientes de la
base de datos: validar aware, normalizar a UTC, leer el reloj, interpretar una
hora naive solo en una frontera explícita y calcular límites de un día local.
No conoce SQLAlchemy ni modelos.

`infrastructure/persistence/types.py` contiene `UTCDateTime`, el adaptador de
SQLAlchemy que hace cumplir el contrato al escribir y leer. PostgreSQL usa
`timestamptz`; SQLite guarda UTC sin `tzinfo` internamente porque no tiene un
tipo equivalente, pero el adaptador restaura UTC-aware al devolver el valor.

`infrastructure/persistence/migrations/temporal_schema.py` solo se ocupa de la
transición del esquema histórico. No contiene reglas de negocio ni se usa para
conversiones ordinarias durante la ejecución.

## Límites de conversión

- Proveedor -> dominio: parsear una vez, exigir `Z`/offset o declarar el
  contrato documentado del proveedor, y normalizar con `as_utc`.
- Dominio -> base: `UTCDateTime` rechaza valores naive.
- Base -> dominio: `UTCDateTime` devuelve UTC-aware.
- Dominio -> interfaz: convertir a `Config.TIMEZONE` solo para mostrar o
  aplicar una regla de calendario.
- Legacy -> dominio: `interpret_local_naive` exige declarar la zona que daba
  significado al dato antiguo. No es una comodidad para código nuevo.

## Nombres canónicos

- `events.starts_at`: instante de inicio del evento.
- `event_source_resolution_queue.source_starts_at`: instante de inicio
  informado por el proveedor.

Los nombres describen el concepto y no el formato visual. Un cliente SQL puede
mostrar un `timestamptz` con `+00`, `-06` u otro offset según la zona de la
sesión; todos representan el mismo instante.

## Migración histórica

La precondición confirmada es que todos los valores históricos que estaban en
columnas `timestamp without time zone` eran horas civiles de
`America/Mexico_City`. Cada columna se convirtió in-place mediante la
semántica:

```sql
legacy_value AT TIME ZONE 'America/Mexico_City'
```

No se aplicó un offset fijo. PostgreSQL usó las reglas históricas de la zona
IANA para la fecha de cada fila. Para cada columna, el migrador:

1. calcula y conserva temporalmente el epoch esperado por clave primaria;
2. cambia el tipo a `TIMESTAMP WITH TIME ZONE`;
3. compara el epoch de cada fila con el esperado;
4. aborta y revierte toda la transacción si existe una diferencia;
5. aborta si al final queda cualquier columna `timestamp without time zone`.

La misma transacción renombró `events.start_time_utc` a `events.starts_at` y
`event_source_resolution_queue.source_start_time_utc` a
`event_source_resolution_queue.source_starts_at`. Las vistas dependientes se
recrean después de la migración. La vista obsoleta
`basketball_results_season_year` se retira explícitamente.

## Estado verificado de PostgreSQL

- cero columnas `timestamp without time zone` en el esquema de aplicación;
- `events.starts_at` es `timestamp with time zone`;
- el nombre antiguo `start_time_utc` ya no existe;
- las conexiones de la aplicación ejecutan `SET TIME ZONE 'UTC'`;
- el migrador es idempotente y una segunda ejecución no modifica el esquema.

La conversión in-place exige una ventana de mantenimiento porque PostgreSQL
puede tomar locks exclusivos durante `ALTER COLUMN TYPE`. Antes de repetirla
en otro entorno se deben detener writers antiguos y tomar un respaldo.
