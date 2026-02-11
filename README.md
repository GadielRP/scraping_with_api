# SofaScore Odds System

**Versión:** v1.5.5  
**Estado:** ✅ **PRODUCCIÓN - AUTOMATED CLEANUP + BACKFILL SYSTEM + GLOBAL DISCOVERY FILTERING**  
**Última Actualización:** 7 de Enero, 2026

## 🎯 **Descripción del Sistema**

Sistema automatizado de monitoreo y predicción de odds de SofaScore que:
- **Descubre eventos deportivos** automáticamente cada 2 horas
- **Notifica por Telegram** sobre juegos que empiezan en los próximos 30 minutos
- **Predice resultados** basado en patrones históricos de odds
- **Extrae odds inteligentemente** solo en momentos clave (30 y 5 minutos antes)
- **Recolecta resultados** de juegos terminados
- **Limpieza Automática**: Elimina eventos inexistentes (404) y cancelados automáticamente
- **Recuperación de Datos**: Sistema de backfill para completar historial de resultados y odds
- **Funciona 24/7** con programación inteligente y optimizada
- **Muestra odds completas** en notificaciones (apertura y finales)

## 🚀 **Características Principales**

### ✅ **Automated Cleanup & Backfill System (v1.5.5) - NUEVO**
- **404 Auto-Cleanup**: El sistema detecta cuando un evento ya no existe en la API (404) y lo elimina de la base de datos para mantener la higiene de datos.
- **Canceled Event Deletion**: Los eventos con estados de Cancelado, Pospuesto o Suspendido se eliminan automáticamente durante la recolección de resultados.
- **Backfill Results**: Nueva herramienta para procesar retroactivamente eventos que no tienen resultados o odds, con capacidad de reanudar progreso.

### ✅ **PROCESS 1 - Sistema de Predicciones Inteligentes (v1.1) - COMPLETADO**
**📋 Definición**: Process 1 es el sistema de análisis de patrones de odds que evalúa eventos históricos para predecir resultados futuros.

#### **🏗️ Arquitectura Process 1:**
- **Variation Tiers (Niveles de Variación)**:
  - **Tier 1 (Exacto)**: Variaciones idénticas de odds (var_one, var_x, var_two)
  - **Tier 2 (Similar)**: Variaciones dentro de ±0.04 tolerancia
- **Result Tiers (Niveles de Resultado)**:
  - **Tier A (Idéntico)**: Todos los candidatos tienen el mismo resultado exacto
  - **Tier B (Similar)**: Todos los candidatos tienen el mismo ganador y diferencia de puntos
  - **Tier C (Mismo Ganador)**: Todos los candidatos tienen el mismo ganador (con promedio ponderado)

#### **🎯 Lógica Process 1:**
- **Selección de Tier**: Si hay candidatos Tier 1, solo se usa Tier 1. Si no, se usa Tier 2.
- **Evaluación de Reglas**: Se evalúan en orden de prioridad A > B > C
- **Confianza Ponderada**: 
  - Tier A: 100% (peso 4)
  - Tier B: 75% (peso 3) 
  - Tier C: 50% (peso 2)
- **Status Logic**:
  - **SUCCESS**: Todos los candidatos del tier seleccionado cumplen al menos una regla
  - **NO MATCH**: Al menos un candidato falla todas las reglas
  - **NO CANDIDATES**: No se encontraron candidatos

#### **📊 Características Process 1:**
- **Análisis de Patrones**: Encuentra eventos históricos con variaciones de odds similares
- **Predicciones Basadas en Datos**: Predice resultados usando patrones históricos
- **Discovery Source Filtering**: Solo busca candidatos históricos con `discovery_source='dropping_odds'` para mayor precisión
- **Sistema de Reportes Completo**: SUCCESS/NO MATCH con datos completos
- **Lógica Deportiva**: Maneja deportes con empate (Fútbol) y sin empate (Tenis)
- **Mensajes Enriquecidos**: Muestra variaciones Δ1, ΔX, Δ2, confianza y timing
- **Variation Differences Display**: Muestra diferencias exactas para Tier 2 candidatos (similar matches) con signos visibles
- **Ground Type Display**: Muestra tipo de cancha para eventos de tennis en notificaciones
- **Competition Display**: Muestra competencia/torneo para cada candidato histórico
- **Sport Classification**: Sistema modular de clasificación deportiva (Tennis Singles/Doubles)
- **AlertMatch Structure**: Dataclass completo con competition field y var_diffs para candidatos históricos
- **Datos Completos**: 161 eventos de tennis con ground type extraído (99.4% success rate)
- **Odds Display**: Muestra odds de apertura y finales en notificaciones

#### **📊 AlertMatch Dataclass Structure:**
```python
@dataclass
class AlertMatch:
    event_id: int                    # ID del evento histórico
    participants: str                # "Team A vs Team B"
    result_text: str                 # "2-1", "6-4, 6-2"
    winner_side: str                 # "1", "X", "2"
    point_diff: int                  # Diferencia de puntos
    var_one: float                   # Variación odds home
    var_x: Optional[float]           # Variación odds draw (si aplica)
    var_two: float                   # Variación odds away
    sport: str = 'Tennis'            # Deporte del evento
    is_symmetrical: bool = True      # Si variaciones son simétricas
    competition: str = 'Unknown'     # Competencia/torneo
    var_diffs: Optional[Dict[str, float]] = None  # Diferencias de variaciones
    one_open: float = 0.0            # Odds de apertura home
    x_open: float = 0.0              # Odds de apertura draw
    two_open: float = 0.0            # Odds de apertura away
    one_final: float = 0.0           # Odds finales home
    x_final: float = 0.0             # Odds finales draw
    two_final: float = 0.0           # Odds finales away
```

### ✅ **PROCESS 2 - Sistema de Reglas Específicas por Deporte - IMPLEMENTADO (v1.3)**
**📋 Definición**: Process 2 es un sistema de reglas específicas por deporte que complementa Process 1 con análisis deportivo especializado usando fórmulas matemáticas específicas.

#### **🏗️ Arquitectura Process 2:**
- **Estructura Modular**: Un archivo por deporte siguiendo @rules.mdc
  - `process2/sports/football.py`: 11 fórmulas específicas de fútbol implementadas
  - `process2/sports/handball.py`: En desarrollo
  - `process2/sports/rugby.py`: En desarrollo
  - `process2/sports/tennis.py`, `basketball.py`: En desarrollo
- **Variables Deportivas**: Cálculo en memoria de variables específicas por deporte
  - **Fútbol**: β, ζ, γ, δ, ε calculadas a partir de var_one, var_x, var_two
- **Return Estandarizado**: `(winner_side, point_diff)` compatible con Process 1
- **Integración Dual**: Orchestrador ejecuta ambos procesos y compara resultados

#### **🎯 Dual Process Integration:**
- **Prediction Engine**: `prediction_engine.py` orquesta ambos procesos
- **Comparison Logic**: Prioridad en `winner_side`, tolerancia en `point_diff`
- **Enhanced Messages**: Reportes separados + veredicto final (AGREE/DISAGREE/PARTIAL/ERROR)
- **Failure Handling**: Ambos reportes enviados cuando hay desacuerdo o fallas
- **Estado**: 🟢 **IMPLEMENTADO Y FUNCIONANDO - SISTEMA DUAL PROCESS COMPLETO**

#### **📊 Fórmulas de Fútbol Implementadas:**
- **Empateγδ**: γ=0 y δ≥0, δ abs ≤ 0.1 → Empate
- **Empateεζ**: ε=0, ζ abs ≤ 0.1 → Empate
- **Gana Localγδ**: γ=δ o diferencia abs≤0.12, ε≤1.15 → Gana Local
- **Gana Localγδ_var_two**: γ=δ o diferencia abs≤0.12, ε≤1.15, var_two=0 → Gana Local
- **Gana Localγδζ**: γ=δ o diferencia abs≤0.1, ε≤1.15, var_two≥0, var_two≤0.05, ζ=0 → Gana Local
- **Gana localεζ**: ε=0, ζ>1, ζ<2 → Gana Local
- **Gana Visitaγδε**: abs(γ+δ)=ε → Gana Visita
- **Gana Visitaγδ_var_two**: γ=δ o diferencia abs≤0.1, var_one=0 → Gana Visita
- **Gana Visitaγδ**: γ=δ con diferencia abs≤0.1, abs(β+γ)=ε → Gana Visita
- **Gana visitaεζ**: ε=0, ζ < 1 → Gana Visita
- **ENA Localγδ**: γ=abs ≥ 0, γ ≤0.1, δ≥0.01, δ≤0.04 → ENA (No Aplica)

### ✅ **Global Discovery Filtering (v1.5.4) - NUEVO**
- **Filtrado Centralizado**: Aplicado en todos los discovery jobs (Daily, Job A, Job B) para omitir eventos que ya comenzaron o están a menos de 10 minutos de iniciar.
- **Shared Utility**: Uso de `filter_upcoming_events` en `optimization.py` para consistencia.
- **Timestamp Comparison**: Compara `startTimestamp` contra la hora local (aware) en segundos Unix.
- **Integración**: Aplicado a Dropping Odds, High Value Streaks, Top H2H, y Winning Odds.

### ✅ **Upcoming Event Filtering (v1.5.3)**
- **Filtrado Temporal**: Omite eventos que ya comenzaron o están a menos de 10 minutos de iniciar durante el descubrimiento diario.
- **Timestamp Comparison**: Compara `startTimestamp` contra la hora local (aware) en segundos Unix.
- **Integración**: Paso adicional en el pipeline de `today_sport_extractor.py`.

### ✅ **Sistema de Notificaciones Inteligentes (v1.0)**
- **Telegram Bot**: Notificaciones automáticas en tiempo real
- **Timing Inteligente**: Solo notifica cuando se extraen odds en momentos clave
- **Formato Rico**: Emojis, información detallada, odds de apertura y finales
- **Configuración Simple**: Solo requiere bot token y chat ID
- **Lógica Optimizada**: Incluye todos los juegos próximos en una sola notificación
- **Odds Display**: Muestra odds completas (apertura y finales) en candidatos históricos

### ✅ **Smart Alert Filtering (v1.5.0) - NUEVO**
- **Filtrado de Bajo Valor**: Salta alertas de odds para eventos con solo 1 mercado ("Full time").
- **Resurrección por Rachas**: Reactiva alertas (`alert_sent=False`) si el evento tiene suficientes datos históricos (mínimo 15 partidos).
- **Umbrales Configurables**: `STREAK_ALERT_MIN_RESULTS` para control fino de calidad.

### ✅ **Dynamic Odds Storage (v1.5.0) - NUEVO**
- **Extracción Multimercado**: Almacena todos los mercados disponibles (Over/Under, Handicap, etc.).
- **Esquema Relacional**: Estructura modular `markets` ↔ `market_choices`.
- **Eficiencia**: Conversión a decimales y eliminación de metadatos API redundantes.
- **Historial Completo**: Mantiene un registro de todos los mercados detectados en pre-start checks.

### ✅ **Event Enrichment & Midnight Odds Sync (v1.5.2) - NUEVO**
- **Enriquecimiento de Metadatos**: Actualiza automáticamente `season_id` y `round` para eventos existentes durante la recolección de resultados.
- **Sincronización de Odds Finales**: Extrae y almacena las odds finales para todos los eventos del día anterior a las 04:00 AM.
- **Persistencia de Mercados**: Guarda todos los mercados disponibles (Over/Under, Spread, etc.) para eventos finalizados, garantizando un historial completo.
- **Prioridad de Discovery**: Sistema de protección que preserva la fuente original de descubrimiento, permitiendo que solo `dropping_odds` sobrescriba fuentes de menor prioridad.

### ✅ **Multi-Source Discovery System (v1.4)**
#### **Discovery 1 - Dropping Odds (Producción)**
- **Programación**: Cada 2 horas (00:00, 02:00, 04:00, 06:00, 08:00, 10:00, 12:00, 14:00, 16:00, 18:00, 20:00, 22:00)
- **Fuente Principal**: `/odds/1/dropping/all` (todos los deportes)
- **Fuentes Específicas**: `/odds/1/dropping/{sport}` para deportes individuales
  - Deportes procesados: football, basketball, volleyball, american-football, ice-hockey, darts, baseball, rugby
- **Lógica de Procesamiento**: 
  1. Procesa primero `/dropping/all` y registra IDs procesados
  2. Luego procesa deportes individuales, saltando eventos ya procesados
  3. Evita duplicación mediante tracking de IDs en memoria
- **Deportes**: Fútbol, Tenis, Baloncesto, Béisbol y más
- **Cobertura Global**: Eventos de múltiples ligas y competencias

#### **Discovery 2 - Special Events (Producción)**
- **Programación**: Cada 6 horas en hh:02 (configurable via DISCOVERY2_INTERVAL_HOURS)
- **Fuentes Implementadas**:
  - ✅ **High Value Streaks**: Eventos con rachas de alto valor (solo información básica)
  - ✅ **H2H Events**: Eventos con historial head-to-head (solo información básica)
  - ✅ **Winning Odds**: Eventos con mejores odds de victoria (con odds completas)
  - ✅ **Team Streaks**: Eventos de rachas de equipos (con odds completas)
- **Optimización**: High Value Streaks y H2H procesan solo eventos, odds se obtienen en pre-start checks
  
#### **Discovery 3 - Daily Discovery (Producción)**
- **Programación**: Diario a las 05:01
- **Fuente**: `/sport/{sport}/scheduled-events/{date}` y `/sport/{sport}/odds/1/{date}`
- **Deportes**: Basketball, Tennis, Baseball, Hockey, American Football, Football
- **Funcionalidad**: Obtiene todos los eventos programados del día con sus odds iniciales y finales
- **Procesamiento**: Filtra eventos que tienen odds disponibles y los inserta en la base de datos

#### **Event Tracking**
- **Discovery Source Field**: Cada evento incluye `discovery_source` para identificar su origen
- **Valores**: `'dropping_odds'`, `'high_value_streaks'`, `'h2h'`, `'winning_odds'`, `'team_streaks'`, `'daily_discovery'`
- **Dropping Odds Priority**: Eventos de dropping odds siempre sobrescriben `discovery_source` existente (fuente más importante)
- **Uso**: Permite aplicar lógica de alertas específica según la fuente

### ✅ **Verificación Pre-Inicio con Extracción Inteligente**
- **Frecuencia**: Cada 5 minutos en intervalos de reloj
- **Ventana**: 30 minutos antes del inicio del juego
- **Extracción Inteligente**: Solo obtiene odds finales en momentos clave:
  - **30 minutos antes**: Primera extracción de odds finales
  - **5 minutos antes**: Última extracción de odds finales
- **Eficiencia**: Evita extracciones innecesarias cuando odds no cambian significativamente

### ✅ **Sistema de Notificaciones Optimizado**
- **Trigger Inteligente**: Solo envía notificaciones cuando se extraen odds
- **Cobertura Completa**: Incluye todos los juegos próximos en cada notificación
- **Información de Odds**: Muestra tanto odds de apertura como finales
- **Manejo de Edge Cases**: Incluye juegos con diferentes timings en una sola notificación

### ✅ **Recolección de Resultados**
- **Sincronización**: Diaria a las 04:00 (CORREGIDO: era 00:05, causaba eventos faltantes)
- **Lógica Inteligente**: Tiempos de corte específicos por deporte
- **Deduplicación**: Evita resultados duplicados
- **Fix Crítico (10/09/2025)**: Mejorada extracción para manejar todos los códigos de estado terminados
- **Timing Fix (19/09/2025)**: Mover midnight job a 04:00 para dar buffer a eventos tardíos
- **Mejora**: Reducción del 85% en eventos sin resultados (de 8.1% a 1.2% gap)
- **Cobertura Final**: 99.0% (700/707 eventos con resultados)

### ✅ **Sistema de Corrección de Timestamps (v1.2.6 → v1.4.6)**
- **Detección Automática**: Compara timestamps de la API con la base de datos
- **Actualización Inteligente**: Actualiza automáticamente timestamps desactualizados
- **Optimización de API**: Solo verifica timestamps en momentos clave (30 y 5 minutos antes)
- **Late Timestamp Correction (NUEVO v1.4.6)**: Verifica eventos que comenzaron hace 0-5 minutos para detectar correcciones tardías
- **Precisión de Microsegundos**: Manejo robusto de comparaciones de tiempo eliminando problemas de microsegundos
- **Control de Configuración**: Variable `ENABLE_TIMESTAMP_CORRECTION` para activar/desactivar
- **Prevención de Loops**: Sistema anti-bucle para eventos reprogramados
- **Logging Detallado**: Registro completo de correcciones de timestamps
- **Notificaciones Mejoradas**: Mensajes de alerta actualizados para reflejar correcciones tardías

### ✅ **Auto-Migration System (v1.4) - NEW**
- **Model-Driven**: Detecta automáticamente diferencias entre `models.py` y la base de datos
- **Self-Healing**: Añade columnas faltantes sin intervención manual
- **Smart Indexing**: Crea índices automáticamente para columnas comunes (source, sport, status, type, gender)
- **Safe Operations**: Solo añade columnas (no elimina, no modifica tipos por seguridad)
- **Transaction-Based**: Todas las migraciones en transacciones (rollback en error)
- **Zero Downtime**: Migraciones en milisegundos al inicio del sistema
- **Ejemplo**: `discovery_source` column añadida automáticamente en v1.4

### ✅ **H2H Streak Alerts**
- **H2H Analysis**: Analiza head-to-head histórico entre equipos (últimos 2 años)
- **Historical Form**: Incluye últimos juegos con resultados de cada equipo (W-L-D) y standing # con formato de lotes de 5 (dependiendo del evento y su season_id recrea tabla de posiciones por cada fecha/evento y muestra el standing de los participiantes *DB-Based Team Form Retrieval (Optimización)* o solo muestra los resultados pasados sin standings )
- **Ranking Prediction (Tennis)**: Predicción basada en rankings finales y puntos totales históricos
  - Muestra ranking advantage (diferencia entre mejor y peor ranking)
  - Calcula predicción usando diferencia de puntos totales (no puntos por juego)
  - Solo para eventos de Tennis/Tennis Doubles
- **Enhanced Telegram Alerts**: Muestra H2H stats + team form + winning odds + ranking prediction con emojis

### ✅ **DB-Based Team Form Retrieval (Optimización)**
- **Collected Seasons (29 Total)**: Para temporadas completamente recolectadas, el sistema usa consultas a la base de datos local en lugar de llamadas API:
  - **NBA**: 6 seasons (20/21 - 25/26) + 3 NBA Cup IDs (23-25)
  - **NFL**: 6 seasons (2020 - 2025)
  - **La Liga**: 6 seasons (2020 - 2025)
  - **Premier League**: 6 seasons (2020 - 2025)
  - **MLB**: 2 seasons (2024 - 2025)
  - **NHL**: 1 season (2025)
  - **Serie A / Bundesliga**: 2025 seasons
- **Conference/League Standings**: Teams ranked within their conference/league:
  - **NBA**: Eastern / Western Conference (15 teams each)
  - **NFL**: AFC / NFC (16 teams each)
  - **MLB**: American League / National League (15 teams each)
  - **NHL**: Eastern / Western Conference (16 teams each)
  - **Football (Soccer)**: League-wide ranking (3pts win, 1pt draw)
- **Multi-Season Support**: `COLLECTED_SEASON_IDS` soporta `additional_season_id` para temporadas compuestas (ej: NBA regular + NBA Cup).
  - El helper `get_all_season_ids()` garantiza que se consulten todos los IDs relacionados.
  - Pre-computed flat set `_ALL_COLLECTED_IDS` para O(1) lookups.
- **Dual Route Architecture**: `streak_alerts.py → get_team_last_results_by_id()` detecta automáticamente si usar DB o API basado en `is_season_collected()`
  - **Ruta 1 (DB)**: `historical_standings.py → get_team_form_from_db()` - Incluye standings históricos con soporte multi-season y conference splits.
  - **Ruta 2 (API)**: `api_client.get_team_last_results_response()` - Para temporadas no recolectadas.
- **PostgreSQL Optimization**: Las consultas DB usan `= ANY(:season_ids)` para buscar múltiples IDs de temporada de forma eficiente.
- **Standings Simulation**: Calcula standings históricos en cualquier punto del tiempo usando `StandingsSimulator` class.
- **Performance**: Reduce significativamente las llamadas API para temporadas populares y optimiza el uso de recursos locales.

### ✅ **Dropping Odds Discovery Source Priority (v1.4.10)**
- **Priority Overwrite**: Eventos descubiertos por dropping odds (`/odds/1/dropping/all`) siempre sobrescriben `discovery_source` existente
- **Rationale**: Dropping odds es la fuente más importante - si un evento aparece ahí, debe marcarse como `dropping_odds` independientemente de su origen previo
- **Implementation**: Lógica en `repository.py` que detecta `discovery_source='dropping_odds'` y siempre actualiza, incluso para eventos existentes
- **Logging**: Registra cuando se sobrescribe un `discovery_source` diferente a `dropping_odds`

### ✅ **Dropping Odds Filtering for Dual Process (v1.4.12)**
- **Process 1 Candidate Filtering**: Alert engine solo busca candidatos históricos con `discovery_source='dropping_odds'` para mayor precisión
- **Dual Process Event Filtering**: Sistema dual process solo evalúa eventos con `discovery_source='dropping_odds'` para mantener consistencia
- **Rationale**: Dropping odds es la fuente más confiable - solo eventos y candidatos de esta fuente se usan para predicciones
- **Implementation**: Filtros agregados en `alert_engine.py` (SQL query) y `scheduler.py` (pre-start check)

## 🛠 **Instalación y Configuración**

### **Requisitos (local)**
```bash
pip install -r requirements.txt
```

### **Despliegue en la nube (Docker + PostgreSQL)**
- En producción el sistema corre en Docker y usa PostgreSQL 15.
- Archivo `docker-compose.yml` orquesta `app` y `postgres` con volumen persistente `sofascore_pgdata` y timezone `America/Mexico_City`.
- PostgreSQL está ligado a `127.0.0.1:5432` en el servidor y se accede de forma segura mediante túnel SSH desde tu PC.

Pasos rápidos en el servidor (resumen):
```bash
cd /opt/sofascore
docker volume create sofascore_pgdata
docker compose up -d
```
Más detalles: ver `CLOUD_OPERATIONS_GUIDE.md` (túnel SSH, UFW y backups semanales).

### **Configuración de Telegram**
1. **Crear bot** en @BotFather
2. **Agregar bot al grupo** donde quieres recibir notificaciones
3. **Configurar .env**:
   ```bash
   TELEGRAM_BOT_TOKEN=tu_bot_token
   TELEGRAM_CHAT_ID=tu_chat_id_o_grupo_id
   NOTIFICATIONS_ENABLED=true
   ```

### **Configuración de Proxy (Opcional)**
```bash
PROXY_ENABLED=true
PROXY_HOST=pr.oxylabs.io
PROXY_PORT=7777
PROXY_USERNAME=tu_usuario
PROXY_PASSWORD=tu_password
```

### **Configuración de Corrección de Timestamps (Nuevo v1.2.6)**
```bash
# Para PRODUCCIÓN (corrección automática activada)
ENABLE_TIMESTAMP_CORRECTION=true

# Para TESTING (corrección desactivada para timestamps manuales)
ENABLE_TIMESTAMP_CORRECTION=false
```

## 📱 **Uso del Sistema**

### **Comandos Principales**
```bash
# Iniciar sistema completo
python main.py start

# Ejecutar trabajos individuales
python main.py discovery      # Descubrir eventos
python main.py discovery2     # Descubrir eventos especiales (streaks, h2h, winning odds)
python main.py daily-discovery # Descubrir eventos programados del día con odds
python main.py pre-start      # Verificar juegos próximos
python main.py midnight       # Sincronización nocturna
python main.py results        # Recolectar resultados de ayer
python main.py results-all    # Recolectar TODOS los resultados (RECOMENDADO después del despliegue)
python main.py backfill-results # Recuperar resultados y odds faltantes (desde 2026-01-01)

# Sistema de predicciones (v1.1)
python main.py alerts         # Evaluar alertas de patrones
python main.py refresh-alerts # Refrescar vistas materializadas

# Monitoreo y estado
python main.py status         # Estado del sistema
python main.py events         # Ver eventos recientes
```

### **✅ Comando Crítico Post-Despliegue - COMPLETADO**
```bash
# EJECUTADO EXITOSAMENTE después del despliegue para aplicar el fix de resultados
python main.py results-all
```

### **Flujo de Trabajo Automático Optimizado**
1. **05:01**: Descubrimiento diario de eventos programados con odds (con filtrado de eventos inminentes/en juego)
2. **00:00-22:00**: Descubrimiento cada 2 horas (dropping odds)
3. **Cada 5 min**: Verificación de juegos próximos
4. **Momentos Clave**: Extracción de odds a los 30 y 5 minutos
5. **Corrección de Timestamps**: Verificación y actualización automática (si está habilitada)
6. **Análisis de Patrones**: Evaluación de alertas basadas en historial
7. **Notificaciones Agrupadas por Evento**: Para cada evento en momentos clave (30 o 5 min), se envían alertas en orden:
   - **Odds Alert**: Todos los mercados disponibles (si pasa el filtro de bajo valor).
   - **Dual Process Alert**: Predicciones Process 1 + Process 2.
   - **H2H Streak Alert**: Análisis histórico head-to-head (puede resucitar eventos filtrados).
8. **04:00**: Recolección de resultados, sincronización final de odds/mercados y **limpieza automática de eventos inexistentes/cancelados**.

### **Sistema de Predicciones - ¿Qué hace un Candidato?**

Un **candidato** es un evento histórico que el sistema identifica como similar al evento actual basándose en:

#### **🔍 Criterios de Similitud:**
- un candidado se convierte en candidato si cumple una de las siguientes tiers como minimo, despues puede ser descartado o marcado como exitoso.
- **Tier 1 (Exacto)**: Variaciones idénticas en `var_one`, `var_x` (si aplica, hay deportes sin empate), `var_two`
- **Tier 2 (Similar)**: Variaciones dentro de ±0.04 tolerancia

#### **📊 Reglas de Unanimidad:**
- **Resultados Idénticos**: Todos los candidatos Tier 1 tuvieron el mismo resultado
- **Resultados Similares**: Todos los candidatos Tier 2 tuvieron el mismo ganador y diferencia de puntos
- **Datos Completos**: El evento histórico debe tener odds y resultados completos

### **Notas:**
- **Candidatos encontrados = Siempre notificar**: Si se rompe la regla de unanimidad, el sistema envía un mensaje "NO MATCH" con todos los datos para perfeccionar la lógica
- **Datos completos**: Todos los casos con candidatos se reportan con variaciones y resultados detallados
- **Análisis mejorado**: Los datos de "no match" permiten perfeccionar fórmulas y criterios
- **Odds Display**: Las notificaciones muestran odds de apertura y finales para cada candidato histórico

#### **⚽ Ejemplo Práctico:**
Si un evento actual tiene variaciones `Δ1: +0.15, ΔX: -0.08, Δ2: -0.07`, el sistema busca eventos históricos con variaciones similares y verifica si todos tuvieron el mismo resultado (ej: "Home 2-1").

### **Backups y Restauración (producción)**
- Los backups semanales se generan en el servidor con `scripts/backup_server.py` y se descargan a tu PC con `scripts/pull_backup_windows.py`.
- Guía paso a paso (con rutas exactas PC/servidor): sección 14 de `CLOUD_OPERATIONS_GUIDE.md`.

### **Acceso seguro a PostgreSQL**
- PostgreSQL no está expuesto públicamente (bind `127.0.0.1:5432`).
- Conéctate desde tu PC usando un túnel SSH (`-L 5433:localhost:5432`).

## 📊 **Estado Actual**

### ✅ **Completado (100%)**
- Sistema de predicciones basado en patrones históricos
- Sistema de notificaciones Telegram optimizado
- Descubrimiento automático cada 2 horas
- Verificación pre-inicio cada 5 minutos
- Extracción inteligente de odds (solo en momentos clave)
- Sistema de notificaciones inteligente (solo cuando es necesario)
- Recolección automática de resultados **CON FIX CRÍTICO APLICADO**
- Sistema de corrección automática de timestamps **NUEVO v1.2.6**
- Manejo robusto de errores y reintentos
- Sistema de proxy con rotación de IPs
- Base de datos PostgreSQL con SQLAlchemy
- Programación inteligente de trabajos
- **Odds Display en Notificaciones**: Muestra odds de apertura y finales en candidatos históricos
- **Odds Alert System**: Sistema completo de alertas con todos los mercados disponibles **NUEVO v1.4.13**

### 🎯 **En Producción - Optimizado**
- **Predicciones**: Análisis de patrones históricos funcionando
- **Notificaciones**: Funcionando con lógica inteligente y odds completas
- **Descubrimiento**: Programado cada 2 horas
- **Extracción de Odds**: Solo en momentos clave (30 y 5 minutos)
- **Corrección de Timestamps**: Automática y configurable **NUEVO v1.2.6**
- **Resultados**: Recolectándose automáticamente
- **Monitoreo**: Sistema estable y eficiente 24/7

## 🔧 **Arquitectura Técnica**

### **Componentes Principales**
- **`main.py`**: Punto de entrada y CLI
- **`scheduler.py`**: Programación de trabajos con lógica optimizada y dual process integration
- **`alert_engine.py`**: Motor de predicciones basado en patrones históricos (Process 1)
- **`process2/`**: Sistema modular de reglas específicas por deporte (Process 2)
  - **`process2_engine.py`**: Motor principal de Process 2
  - **`sports/football.py`**: 11 fórmulas específicas de fútbol implementadas
  - **`sports/`**: Módulos para otros deportes (en desarrollo)
- **`prediction_engine.py`**: Orchestrador dual process con lógica de comparación
- **`alert_system.py`**: Sistema de notificaciones Telegram con reportes duales y odds display
- **`odds_alert.py`**: Sistema de alertas de odds completas con todos los mercados disponibles (NUEVO v1.4.13)
- **`database.py`**: Gestión de base de datos
- **`repository.py`**: Acceso a datos optimizado
- **`config.py`**: Configuración centralizada
- **`sofascore_api.py`**: API client con manejo inteligente
- **`odds_utils.py`**: Utilidades para procesamiento de odds

### **Tecnologías**
- **Python 3.11+**: Lógica principal
- **Docker & Docker Compose**: Orquestación en producción
- **PostgreSQL 15 (Docker) + SQLAlchemy 2 + psycopg (v3)**: Base de datos en producción
- **SQLite**: Solo para desarrollo local rápido
- **curl-cffi**: HTTP con impersonación/bypass anti-bot
- **schedule**: Programación de trabajos

## 📈 **Métricas del Sistema**

### **Rendimiento**
- **Descubrimiento**: ~2-3 segundos por ejecución
- **Verificación Pre-Inicio**: ~1-2 segundos por ejecución
- **Extracción de Odds**: Solo en momentos clave (30 y 5 minutos)
- **Notificaciones**: ~500ms por mensaje
- **Base de Datos**: Respuesta <100ms

### **Confiabilidad**
- **Manejo de Errores**: Reintentos automáticos con backoff exponencial
- **Proxy**: Rotación automática en caso de fallos
- **Logging**: Registro detallado de todas las operaciones
- **Recuperación**: Reinicio automático en caso de errores críticos

## 🎉 **Sistema Dual Process en Producción**

El sistema está **completamente funcional** y **listo para producción**:
- ✅ **Process 1**: Predicciones basadas en patrones históricos
- ✅ **Process 2**: Sistema de reglas específicas por deporte (fútbol implementado)
- ✅ **Dual Process**: Orchestrador que compara ambos procesos
- ✅ **Notificaciones Telegram**: Reportes duales con veredicto final y odds completas
- ✅ **Descubrimiento automático**: Multi-source discovery system
- ✅ **Extracción de odds**: Solo en momentos clave
- ✅ **Recolección de resultados**: Programada con alta cobertura
- ✅ **Monitoreo 24/7**: Eficiente y estable

## 🔧 **Fixes Recientes**

### **Basketball Results View Enhancement**
- **Overtime Scores**: View `basketball_results` ahora incluye columnas `ot_home` y `ot_away` para puntajes de tiempo extra
- **Start Time**: View muestra `start_time` (desde `start_time_utc`) en lugar de `season_name`
- **Format Support**: Extrae puntajes de overtime desde formato `'23-23-31-24-(16)'` donde `(16)` es el tiempo extra