# SofaScore Odds System

**Versión:** v1.2.4  
**Estado:** ✅ **PRODUCCIÓN - Process 1 COMPLETADO CON COMPETITION FIELD Y SPORT CLASSIFICATION - Process 2 EN PREPARACIÓN**  
**Última Actualización:** 19 de Septiembre, 2025

## 🎯 **Descripción del Sistema**

Sistema automatizado de monitoreo y predicción de odds de SofaScore que:
- **Descubre eventos deportivos** automáticamente cada 2 horas
- **Notifica por Telegram** sobre juegos que empiezan en los próximos 30 minutos
- **Predice resultados** basado en patrones históricos de odds
- **Extrae odds inteligentemente** solo en momentos clave (30 y 5 minutos antes)
- **Recolecta resultados** de juegos terminados
- **Funciona 24/7** con programación inteligente y optimizada

## 🚀 **Características Principales**

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
- **Sistema de Reportes Completo**: SUCCESS/NO MATCH con datos completos
- **Lógica Deportiva**: Maneja deportes con empate (Fútbol) y sin empate (Tenis)
- **Mensajes Enriquecidos**: Muestra variaciones Δ1, ΔX, Δ2, confianza y timing
- **Ground Type Display**: Muestra tipo de cancha para eventos de tennis en notificaciones
- **Competition Display**: Muestra competencia/torneo para cada candidato histórico
- **Sport Classification**: Sistema modular de clasificación deportiva (Tennis Singles/Doubles)
- **AlertMatch Structure**: Dataclass completo con competition field para candidatos históricos
- **Datos Completos**: 161 eventos de tennis con ground type extraído (99.4% success rate)

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
    competition: str = 'Unknown'     # 🆕 Competencia/torneo
```

### 🔮 **PROCESS 2 - Sistema de Reglas Específicas por Deporte - EN DESARROLLO (v1.3)**
**📋 Definición**: Process 2 es un sistema de reglas específicas por deporte que complementa Process 1 con análisis deportivo especializado.

#### **🏗️ Arquitectura Process 2:**
- **Estructura Modular**: Un archivo por deporte siguiendo @rules.mdc
  - `sports/handball.py`: 8+ reglas específicas de handball
  - `sports/rugby.py`: 8+ reglas específicas de rugby
  - `sports/tennis.py`, `football.py`, `basketball.py`: En desarrollo
- **Return Estandarizado**: `[winner_side, point_diff]` compatible con Process 1
- **Integración Dual**: Orchestrador ejecuta ambos procesos y compara resultados

#### **🎯 Dual Process Integration:**
- **Prediction Engine**: `prediction_engine.py` orquesta ambos procesos
- **Comparison Logic**: Prioridad en `winner_side`, tolerancia en `point_diff` por deporte
- **Enhanced Messages**: Reportes separados + veredicto final (AGREE/DISAGREE)
- **Failure Handling**: Ambos reportes enviados cuando hay desacuerdo o fallas
- **Estado**: 🟡 **EN DESARROLLO ACTIVO - ARQUITECTURA DUAL PROCESS**

### ✅ **Sistema de Notificaciones Inteligentes (v1.0)**
- **Telegram Bot**: Notificaciones automáticas en tiempo real
- **Timing Inteligente**: Solo notifica cuando se extraen odds en momentos clave
- **Formato Rico**: Emojis, información detallada, odds de apertura y finales
- **Configuración Simple**: Solo requiere bot token y chat ID
- **Lógica Optimizada**: Incluye todos los juegos próximos en una sola notificación

### ✅ **Descubrimiento Automático Optimizado**
- **Programación**: Cada 2 horas (00:00, 02:00, 04:00, 06:00, 08:00, 10:00, 12:00, 14:00, 16:00, 18:00, 20:00, 22:00)
- **Deportes**: Fútbol, Tenis, Baloncesto, Béisbol y más
- **Cobertura Global**: Eventos de múltiples ligas y competencias
- **Actualización Inteligente**: Actualiza eventos existentes y sus odds

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

## 📱 **Uso del Sistema**

### **Comandos Principales**
```bash
# Iniciar sistema completo
python main.py start

# Ejecutar trabajos individuales
python main.py discovery      # Descubrir eventos
python main.py pre-start      # Verificar juegos próximos
python main.py midnight       # Sincronización nocturna
python main.py results        # Recolectar resultados de ayer
python main.py results-all    # Recolectar TODOS los resultados (RECOMENDADO después del despliegue)

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
1. **00:00-22:00**: Descubrimiento cada 2 horas
2. **Cada 5 min**: Verificación de juegos próximos
3. **Momentos Clave**: Extracción de odds a los 30 y 5 minutos
4. **Análisis de Patrones**: Evaluación de alertas basadas en historial
5. **Notificaciones**: Pre-inicio + Predicciones inteligentes
6. **04:00**: Recolección de resultados (CORREGIDO: era 00:05)

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
- Manejo robusto de errores y reintentos
- Sistema de proxy con rotación de IPs
- Base de datos PostgreSQL con SQLAlchemy
- Programación inteligente de trabajos

### 🎯 **En Producción - Optimizado**
- **Predicciones**: Análisis de patrones históricos funcionando
- **Notificaciones**: Funcionando con lógica inteligente
- **Descubrimiento**: Programado cada 2 horas
- **Extracción de Odds**: Solo en momentos clave (30 y 5 minutos)
- **Resultados**: Recolectándose automáticamente
- **Monitoreo**: Sistema estable y eficiente 24/7

## 🔧 **Arquitectura Técnica**

### **Componentes Principales**
- **`main.py`**: Punto de entrada y CLI
- **`scheduler.py`**: Programación de trabajos con lógica optimizada
- **`process1_engine.py`**: Motor de predicciones basado en patrones históricos (Process 1)
- **`process2_engine.py`**: Motor de reglas específicas por deporte (Process 2)
- **`prediction_engine.py`**: Orchestrador dual process con lógica de comparación
- **`sports/`**: Módulos específicos por deporte (handball, rugby, tennis, etc.)
- **`alert_system.py`**: Sistema de notificaciones Telegram con reportes duales
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

## 🎉 **¡Listo para Producción - Sistema Inteligente!**

El sistema está **completamente funcional**, **optimizado** y **listo para producción**:
- ✅ Predicciones basadas en patrones históricos
- ✅ Notificaciones Telegram con lógica inteligente
- ✅ Descubrimiento automático cada 2 horas
- ✅ Extracción de odds solo en momentos clave
- ✅ Sistema de notificaciones optimizado
- ✅ Recolección de resultados programada
- ✅ Manejo robusto de errores
- ✅ Monitoreo 24/7 eficiente

**¡Tu sistema inteligente de SofaScore está optimizado y funcionando perfectamente!** 🚀⚽🧠

## 🔧 **Fixes Críticos Aplicados y Desplegados**

### **Fix 1: Extracción de Resultados (10/09/2025)**
- **Issue**: 8.1% de eventos sin resultados debido a lógica restrictiva en la extracción de resultados
- **Solución**: Mejorada la lógica para manejar todos los códigos de estado terminados (100, 110, 92, 120, 130, 140)
- **Resultado**: Reducción del 85% en eventos sin resultados (de 27 a 4 eventos)

### **Fix 2: Timing de Midnight Job (19/09/2025)**
- **Issue**: Midnight job a las 00:05 causaba eventos faltantes (eventos que empezaban tarde no terminaban antes de 00:05)
- **Root Cause**: 7 de 17 eventos extractables empezaban a las 23:00 (no terminaban antes de 00:05)
- **Solución**: Mover midnight job de 00:05 a 04:00 para dar 3-4 horas de buffer
- **Resultado**: Cobertura mejorada de 96.6% a 99.0% (683 → 700 eventos con resultados)

### **Despliegue Exitoso**
- ✅ **Sistema v1.2.3 desplegado** en producción (19/09/2025)
- ✅ **Base de datos actualizada** con computed columns y materialized views
- ✅ **Timing fix aplicado**: Midnight job movido a 04:00
- ✅ **Scripts de upsert**: `upsert_debug_results.py` para corregir eventos faltantes
- ✅ **Notificaciones optimizadas**: UPCOMING GAMES ALERT deshabilitado, solo CANDIDATE REPORTS activos

### **Archivos Modificados**
- `sofascore_api.py`: Lógica de extracción de resultados mejorada
- `scheduler.py`: Midnight job movido a 04:00, notificaciones UPCOMING GAMES ALERT deshabilitadas
- `upsert_debug_results.py`: Script para corregir eventos faltantes
- `docker-compose.yml`: Configuración de producción corregida

