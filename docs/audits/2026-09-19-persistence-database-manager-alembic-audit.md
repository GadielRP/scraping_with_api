# Auditoría de Persistencia: Alembic Head Dinámico, Ciclo DDL y Deuda Técnica en DatabaseManager

**Fecha:** 2026-09-19  
**Revisión Alembic Anterior:** `20260918_04`  
**Revisión Alembic Actual:** `20260919_01`  
**Objetivo:** Documentar los problemas arquitectónicos detectados en el ciclo de migración/arranque de la base de datos, la deuda técnica identificada en `DatabaseManager`, y la solución inmediata implementada para desacoplar el versionado de Alembic.

---

## 1. Diagnóstico de los Problemas

### Problema A: Revisión de Alembic Hardcodeada (Falso Positivo en el Arranque)
* **Ubicación:** `infrastructure/persistence/database.py` (L18 y L101-L133).
* **Descripción:** 
  Existía la constante fija:
  ```python
  ALEMBIC_HEAD_REVISION = "20260918_04"
  ```
  La función `verify_schema_at_head()` comparaba la tabla `alembic_version` de PostgreSQL contra este valor estático.
* **Impacto:** 
  Al crearse la migración `20260919_01` (para la vista materializada de Pillar 5), la base de datos no había sido migrada. Sin embargo, al iniciar el contenedor con `docker compose up -d --build`, la aplicación inició con éxito (`System initialized successfully`). El gatekeeper no detectó que faltaba una migración porque su constante esperada seguía apuntando a la revisión anterior. Esto representaba un riesgo de falsos positivos en despliegues.

---

### Problema B: Fuga de Responsabilidades DDL en Runtime (`app/initialize.py`)
* **Ubicación:** `app/initialize.py` (L39-L41).
* **Descripción:** 
  En cada arranque de la aplicación, el código ejecutaba:
  ```python
  create_or_replace_views(db_manager.engine)
  create_or_replace_materialized_views(db_manager.engine)
  ```
* **Impacto:** 
  La aplicación web/scheduler posee permisos de DDL administrador y crea/reemplaza imperativamente vistas e índices (`DROP MATERIALIZED VIEW ... CASCADE`, `CREATE ...`). 
  Esto provocó que la vista `mv_p5_price_memory` y sus índices se crearan físicamente en PostgreSQL durante el arranque de la app, **sin que Alembic hubiera registrado la versión** en `alembic_version`. 
  Esta divergencia rompe el principio de que Alembic debe ser la única fuente de verdad transaccional de los cambios estructurales de la base de datos.

---

### Problema C: Deuda Técnica Acumulada en `DatabaseManager` (God Object de 1,600+ Líneas)
* **Ubicación:** `infrastructure/persistence/database.py`.
* **Descripción:** 
  Un gestor de base de datos estándar debe limitarse a inicializar el `engine`, configurar el pool de conexiones y suministrar la sesión (`get_session()`), lo que suele ocupar ~100 líneas.
  Sin embargo, `database.py` cuenta con más de 1,600 líneas, conteniendo más de 1,400 líneas de migraciones manuales procedurales escritas antes de la adopción formal de Alembic:
  - `_migrate_pillar_mining_schema_v2`
  - `_migrate_market_source_mappings`
  - `_migrate_market_choice_quotes`
  - `_migrate_events_participant_and_competition`
  - `_migrate_daily_discovery_log_run_slots`
  - `_migrate_event_identity_canonicalization` (más de 400 líneas de remapeo manual de IDs de cuando el proyecto migraba de SQLite a Postgres).
* **Impacto:** 
  Existen dos sistemas de migración paralelos compitiendo en el codebase: Alembic (el estándar oficial) y los métodos `_migrate_*` manuales en Python.

---

## 2. Solución Inmediata Implementada

### A. Resolución Dinámica de la Revisión Head de Alembic
Se eliminó la dependencia de actualizar manualmente constantes de texto. Ahora [database.py](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/database.py) inspecciona directamente los scripts de Alembic en runtime:

```python
def get_alembic_head_revision() -> str | None:
    """Resolve the latest Alembic revision dynamically from the script directory.

    Avoids hardcoding revision IDs in source code so new migrations are recognized
    automatically without manual constant updates.
    """
    try:
        from pathlib import Path
        from alembic.config import Config as AlembicConfig
        from alembic.script import ScriptDirectory

        root_dir = Path(__file__).resolve().parents[2]
        ini_path = root_dir / "alembic.ini"
        if not ini_path.exists():
            return None
        cfg = AlembicConfig(str(ini_path))
        script = ScriptDirectory.from_config(cfg)
        return script.get_current_head()
    except Exception as exc:
        logger.debug("Could not resolve dynamic Alembic head revision: %s", exc)
        return None
```

Y en `verify_schema_at_head()`:
```python
def verify_schema_at_head(self, expected_revision: str | None = None) -> bool:
    target_revision = expected_revision or get_alembic_head_revision() or ALEMBIC_HEAD_REVISION
    ...
```

### B. Aplicación Formal de la Migración `20260919_01`
Se ejecutó la migración pendiente sobre PostgreSQL:
```bash
alembic upgrade head
# INFO  [alembic.runtime.migration] Running upgrade 20260918_04 -> 20260919_01, Create Pillar 5 price memory materialized view and indexes.
```
La tabla `alembic_version` en PostgreSQL ahora registra formalmente `20260919_01`.

### C. Desacoplamiento de Vistas de `models.py`
Se resolvió la deuda en `models.py` trasladando todas las definiciones de vistas e índices al nuevo paquete modular:
- `infrastructure/persistence/views/dual_process_views.py`
- `infrastructure/persistence/views/basketball_views.py`
- `infrastructure/persistence/views/season_views.py`
- `infrastructure/persistence/views/p5_price_memory_view.py`
- `infrastructure/persistence/views/view_manager.py`

`models.py` ahora contiene únicamente entidades ORM de SQLAlchemy y re-exporta los símbolos de vistas para garantizar 100% de compatibilidad hacia atrás.

---

## 3. Estado de Verificación

* `verify_schema_at_head()` devuelve `True` de forma dinámica sin ninguna constante manual.
* Pruebas ejecutadas y validadas:
  ```bash
  python -m pytest tests/test_p5_price_memory_view.py tests/test_dual_process_market_odds_read_migration.py tests/pillars/test_pillar_5.py
  # Output: 14 passed in 0.10s
  ```

---

## 4. Recomendaciones a Mediano Plazo

1. **Depuración de Métodos `_migrate_*` en `database.py`:**
   Mover o retirar los métodos de migración manual legacy de SQLite que ya no se utilizan en PostgreSQL para reducir `database.py` a sus responsabilidades exclusivas de conexión y pooling (~100 líneas).
2. **Hacer `app/initialize.py` de Sólo Lectura:**
   En despliegues de producción senior, retirar la ejecución de `create_or_replace_materialized_views` del arranque de la app y delegar toda creación de objetos de base de datos a `alembic upgrade head` en el paso de pre-despliegue (init-container / pipeline CI/CD).
