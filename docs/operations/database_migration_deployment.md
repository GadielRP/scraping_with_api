# Despliegue de migraciones y permisos de PostgreSQL

`docker compose up -d --build` ejecuta `alembic upgrade head` en el servicio
`migrate` antes de arrancar `app`. El proceso de la aplicación verifica que
`alembic_version` coincida con los scripts incluidos en la imagen y no crea
tablas ni vistas en PostgreSQL. Si la revisión esperada no se puede resolver,
el arranque falla.

La revisión `20260919_02` instala las vistas de reporte, sus índices y la
función `public.refresh_reporting_views()`; el refresco periódico invoca esa
función. La función pertenece al rol que ejecuta Alembic. La misma revisión
concede DML y `EXECUTE` al rol de aplicación indicado por `APP_DB_ROLE`, revoca
las escrituras sobre `alembic_version` y configura permisos para tablas futuras.
La revisión `20260919_03` restaura la clave foránea de
`event_source_mappings.event_id` con `ON DELETE CASCADE`. Si hay mappings
huérfanos, la migración se detiene y comunica su cantidad para revisión; no
elimina esos datos automáticamente.

## Preparar el rol de aplicación en una instalación existente

Crear el rol **antes** de ejecutar `docker compose up`, como administrador de
PostgreSQL. Sustituir la contraseña según el despliegue:

```sql
CREATE ROLE sofascore_app LOGIN PASSWORD 'use_a_unique_secret';
```

El rol `sofascore_app` no debe ser propietario de tablas, vistas ni del esquema,
ni recibir `CREATE` sobre `public`. Configurar en `.env`:

```dotenv
APP_DB_ROLE=sofascore_app
APP_DATABASE_URL=postgresql+psycopg://sofascore_app:use_a_unique_secret@postgres:5432/sofascore_odds
```

El servicio `migrate` usa `POSTGRES_USER`/`POSTGRES_PASSWORD`; `app` usa
`APP_DATABASE_URL`. Si la revisión `20260919_02` ya se había aplicado antes de
crear el rol de aplicación, ejecutar manualmente los `GRANT` equivalentes de
esa revisión. Si la base todavía no tiene `alembic_version`, verificar
primero que coincide con el esquema histórico y aplicar la revisión base
`alembic stamp 20260918_00` antes de ejecutar `up`. La revisión base no crea
tablas en una base vacía.
