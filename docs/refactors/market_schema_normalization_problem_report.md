# Reporte Técnico: Desacople de Catálogo Canónico y Redundancia en el Schema de Cuotas

> **Documento:** Diagnóstico de Deuda Técnica y Plan de Migración de Persistencia de Mercados  
> **Área:** Persistencia, Normalización 3NF e Integridad Referencial  
> **Estado:** Fase 1 implementada (Filtro de ingesta); Fases 2, 3 y 4 pendientes de ejecución.

---

## 1. Definición del Problema Técnico

El sistema de persistencia de cuotas (`markets`, `market_choices`, `market_choice_quotes`, `market_choice_snapshots`) opera actualmente con **desconexión referencial** y **duplicación de estado**, lo que genera sobrecosto de almacenamiento, riesgo de inconsistencia y dependencia de cadenas de texto libre para relacionar entidades.

### 1.1. Desconexión entre `canonical_market_types` y `markets`
* **Archivo afectado:** [`infrastructure/persistence/models.py`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/models.py#L715-L757)
* **Diagnóstico:** Existe una tabla maestra [`CanonicalMarketType`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/models.py#L573) con clave primaria `canonical_market_key` (ej. `total_corners_full_time`). Sin embargo, la tabla [`Market`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/models.py#L715) no posee una Foreign Key hacia dicha tabla; en su lugar, almacena cadenas de texto desnormalizadas:
  * `markets.market_name` (`TEXT`)
  * `markets.market_group` (`TEXT`)
  * `markets.market_period` (`TEXT`)
* **Consecuencia:** 
  1. No existe validación de integridad referencial a nivel de base de datos.
  2. Los cambios de nombre en el catálogo maestro (como `corners_2_way_full_time` → `total_corners_full_time` en [`canonical_market_types.py`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/catalogs/canonical_market_types.py#L59)) dejan huérfanas o desincronizadas las filas históricas en `markets`.
  3. Cada fila almacena strings redundantes de hasta 40 bytes multiplicados por ~770,000 registros.

---

### 1.2. Sobrecarga de Restricción Única e Índices Compuestos sobre Texto
* **Archivo afectado:** [`infrastructure/persistence/models.py`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/models.py#L744-L748)
* **Diagnóstico:** La unicidad de un mercado por casa de apuesta y evento se valida mediante un índice de 6 columnas:
  ```python
  UniqueConstraint('event_id', 'bookie_id', 'market_name', 'market_period', 'choice_group', 'is_live', name='unique_market_per_event_bookie')
  ```
* **Consecuencia:** 
  1. El árbol B-Tree del índice indexa columnas de texto redundantes (`market_name`, `market_period`), inflando el tamaño del índice en disco y memoria RAM.
  2. Cada inserción durante la ingesta rápida de cuotas (`MarketRepository.save_canonical_bookmaker_batches`) incurre en alta penalización de I/O.

---

### 1.3. Duplicación de Estado en Cuotas (`market_choices` vs `market_choice_quotes`)
* **Archivo afectado:** [`infrastructure/persistence/models.py`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/models.py#L759-L815)
* **Diagnóstico:**
  * `market_choices` almacena: `initial_odds`, `current_odds`, `change`.
  * `market_choice_quotes` almacena: `initial_odds`, `current_odds`, `initial_captured_at`, `current_updated_at`.
* **Consecuencia:** Viola la Tercera Forma Normal (3NF). Existen dos fuentes de verdad para la cuota actual de una opción. Si la ingesta de un proveedor falla parcialmente o se actualiza una sin la otra en [`market_choice_quote_writer.py`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/repositories/market/market_choice_quote_writer.py), se produce divergencia de datos.

---

## 2. Archivos Impactados en el Codebase

| Capa | Archivo | Responsabilidad Actual |
| :--- | :--- | :--- |
| **Modelos ORM** | [`infrastructure/persistence/models.py`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/models.py) | Define `Market`, `MarketChoice`, `MarketChoiceQuote`, `CanonicalMarketType`. |
| **Migraciones DB** | [`infrastructure/persistence/database.py`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/database.py) | Crea tablas, constraints y gestiona migraciones automáticas en el arranque. |
| **Repositorio Market** | [`infrastructure/persistence/repositories/market_repository.py`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/repositories/market_repository.py) | Inserción/actualización de lotes de mercados (`save_canonical_bookmaker_batches`). |
| **Escritor de Cuotas** | [`infrastructure/persistence/repositories/market/market_choice_quote_writer.py`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/repositories/market/market_choice_quote_writer.py) | Persistencia de quotes y sincronización de cuotas en choices. |
| **Normalizador Canónico** | [`modules/odds_ingestion/canonical_market_normalizer.py`](file:///c:/Users/gadie/Documents/projects/sofascore/modules/odds_ingestion/canonical_market_normalizer.py) | Mapea payloads crudos a diccionarios con `marketName`, `marketGroup`, etc. |
| **Adaptadores** | [`modules/odds_ingestion/adapters/oddspapi_market_adapter.py`](file:///c:/Users/gadie/Documents/projects/sofascore/modules/odds_ingestion/adapters/oddspapi_market_adapter.py)<br>[`modules/odds_ingestion/adapters/sofascore_market_adapter.py`](file:///c:/Users/gadie/Documents/projects/sofascore/modules/odds_ingestion/adapters/sofascore_market_adapter.py)<br>[`modules/odds_ingestion/adapters/oddsportal_market_adapter.py`](file:///c:/Users/gadie/Documents/projects/sofascore/modules/odds_ingestion/adapters/oddsportal_market_adapter.py) | Transforman datos crudos a DTOs canónicos intermedios. |
| **Consumidores / Trayectoria** | [`infrastructure/persistence/repositories/odds_trajectory_repository.py`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/repositories/odds_trajectory_repository.py)<br>[`infrastructure/persistence/repositories/dual_process_odds_repository.py`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/repositories/dual_process_odds_repository.py) | Consultan cuotas filtrando por `market_name` y `market_period`. |

---

## 3. Plan de Mitigación en Fases (2, 3 y 4)

---

### FASE 2: Normalización Aditiva (Non-Breaking Schema Migration)
**Objetivo:** Agregar la relación formal `canonical_market_key` en `markets` sin alterar ni romper los campos existentes.

#### Pasos técnicos:
1. **Modificar [`models.py`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/models.py):**
   * Añadir `canonical_market_key = Column(Text, ForeignKey("canonical_market_types.canonical_market_key"), nullable=True)` en la clase `Market`.
2. **Crear migración DDL en [`database.py`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/database.py):**
   * Ejecutar:
     ```sql
     ALTER TABLE markets 
     ADD COLUMN IF NOT EXISTS canonical_market_key TEXT 
     REFERENCES canonical_market_types(canonical_market_key);
     ```
3. **Script de Backfill (Poblado de datos históricos):**
   * Mapear registros existentes mediante `canonical_market_name` y `canonical_market_period`:
     ```sql
     UPDATE markets m
     SET canonical_market_key = cmt.canonical_market_key
     FROM canonical_market_types cmt
     WHERE m.market_name = cmt.canonical_market_name
       AND m.market_period = cmt.canonical_market_period
       AND m.canonical_market_key IS NULL;
     ```
4. **Actualizar el Pipeline de Ingesta:**
   * Modificar [`MarketRepository.save_canonical_bookmaker_batches`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/repositories/market_repository.py) y los adaptadores para que el diccionario de inserción incluya explícitamente `canonical_market_key`.

---

### FASE 3: Transición de Lecturas y Consultas a Clave Canónica
**Objetivo:** Migrar la lógica de consulta y joins de los módulos de análisis para que usen `canonical_market_key` en vez de comparar strings.

#### Pasos técnicos:
1. **Actualizar Repositorios de Análisis:**
   * Modificar [`odds_trajectory_repository.py`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/repositories/odds_trajectory_repository.py) y [`dual_process_odds_repository.py`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/repositories/dual_process_odds_repository.py):
     * Cambiar filtros `WHERE m.market_name = 'Over/Under Full Time' AND m.market_period = 'Full Time'` por `WHERE m.canonical_market_key = 'over_under_full_time'`.
2. **Actualizar Vistas Materializadas y Consultas de Alertas:**
   * Si existen vistas SQL o queries en [`modules/alerts/`](file:///c:/Users/gadie/Documents/projects/sofascore/modules/alerts/) que hacen join sobre texto, actualizarlas a `canonical_market_key`.
3. **Hacer `canonical_market_key` obligatorio (`NOT NULL`):**
   * Una vez asegurado que el 100% de las filas tienen clave canónica asignada:
     ```sql
     ALTER TABLE markets ALTER COLUMN canonical_market_key SET NOT NULL;
     ```

---

### FASE 4: Optimización de Índices y Limpieza de Columnas Legacy
**Objetivo:** Reducir tamaño de tablas e índices, y retirar las columnas de texto redundantes.

#### Pasos técnicos:
1. **Reemplazar la Restricción Única Compuesta en [`models.py`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/models.py) y [`database.py`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/database.py):**
   * Reemplazar la restricción vieja de 6 columnas por la nueva optimizada de 5 columnas:
     ```sql
     ALTER TABLE markets DROP CONSTRAINT IF EXISTS unique_market_per_event_bookie;
     ALTER TABLE markets ADD CONSTRAINT unique_market_per_event_bookie 
     UNIQUE (event_id, bookie_id, canonical_market_key, choice_group, is_live);
     ```
2. **Eliminar Columnas Desnormalizadas en `markets`:**
   * Retirar `market_group` y `market_period` de `markets` (pasan a resolverse siempre mediante join con `canonical_market_types`).
   * Mantener `market_name` solo si se requiere compatibilidad legacy temporal, o eliminarlo para dejar un schema 3NF puro.
3. **Desacoplar Cuota Actual en `market_choices`:**
   * Retirar `initial_odds`, `current_odds` y `change` de `market_choices` para que residan exclusivamente en `market_choice_quotes`, eliminando la doble actualización en [`market_choice_quote_writer.py`](file:///c:/Users/gadie/Documents/projects/sofascore/infrastructure/persistence/repositories/market/market_choice_quote_writer.py).
4. **Ejecutar `VACUUM FULL` / `REINDEX`:**
   * Recompactar el espacio en disco de PostgreSQL tras la eliminación de columnas e índices antiguos.
