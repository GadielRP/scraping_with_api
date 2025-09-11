# SofaScore Odds System

**Versión:** v1.1  
**Estado:** ✅ **PRODUCCIÓN - Sistema Inteligente con Predicciones**  
**Última Actualización:** 10 de Septiembre, 2025

## 🎯 **Descripción del Sistema**

Sistema automatizado de monitoreo y predicción de odds de SofaScore que:
- **Descubre eventos deportivos** automáticamente cada 2 horas
- **Notifica por Telegram** sobre juegos que empiezan en los próximos 30 minutos
- **Predice resultados** basado en patrones históricos de odds
- **Extrae odds inteligentemente** solo en momentos clave (30 y 5 minutos antes)
- **Recolecta resultados** de juegos terminados
- **Funciona 24/7** con programación inteligente y optimizada

## 🚀 **Características Principales**

### ✅ **Sistema de Predicciones Inteligentes (v1.1)**
- **Análisis de Patrones**: Encuentra eventos históricos con variaciones de odds similares
- **Predicciones Basadas en Datos**: Predice resultados usando patrones históricos
- **Dos Niveles de Precisión**: 
  - **Tier 1 (Exacto)**: Variaciones idénticas de odds
  - **Tier 2 (Similar)**: Variaciones dentro de ±0.04 tolerancia
- **Sistema de Reportes Completo**: 
  - **SUCCESS**: Candidatos con unanimidad = predicción exitosa
  - **NO MATCH**: Candidatos sin unanimidad = datos para perfeccionar lógica
  - **SIN MENSAJE**: Sin candidatos = no se envía notificación
- **¿Qué hace un Candidato?**: Un evento histórico se convierte en candidato cuando:
  - Tiene variaciones de odds idénticas o similares al evento actual
  - Después puede ser una alerta exitosa si también cumple reglas de unanimidad
- **Lógica Deportiva**: Maneja deportes con empate (Fútbol) y sin empate (Tenis)
- **Mensajes Enriquecidos**: Muestra variaciones Δ1, ΔX, Δ2, confianza y timing

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
- **Sincronización**: Diaria a las 00:05
- **Lógica Inteligente**: Tiempos de corte específicos por deporte
- **Deduplicación**: Evita resultados duplicados
- **Fix Crítico (10/09/2025)**: Mejorada extracción para manejar todos los códigos de estado terminados
- **Mejora**: Reducción del 85% en eventos sin resultados (de 8.1% a 1.2% gap)

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

### **⚠️ Comando Crítico Post-Despliegue**
```bash
# EJECUTAR INMEDIATAMENTE después del despliegue para aplicar el fix de resultados
python main.py results-all
```

### **Flujo de Trabajo Automático Optimizado**
1. **00:00-22:00**: Descubrimiento cada 2 horas
2. **Cada 5 min**: Verificación de juegos próximos
3. **Momentos Clave**: Extracción de odds a los 30 y 5 minutos
4. **Análisis de Patrones**: Evaluación de alertas basadas en historial
5. **Notificaciones**: Pre-inicio + Predicciones inteligentes
6. **00:05**: Recolección de resultados

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
- **`alert_engine.py`**: Motor de predicciones basado en patrones históricos
- **`alert_system.py`**: Sistema de notificaciones Telegram inteligente
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

## 🔧 **Fix Crítico Aplicado (10/09/2025)**

### **Problema Resuelto**
- **Issue**: 8.1% de eventos sin resultados debido a lógica restrictiva en la extracción de resultados
- **Solución**: Mejorada la lógica para manejar todos los códigos de estado terminados (100, 110, 92, 120, 130, 140)
- **Resultado**: Reducción del 85% en eventos sin resultados (de 27 a 4 eventos)

### **Comando Post-Despliegue**
```bash
# EJECUTAR INMEDIATAMENTE después del despliegue
python main.py results-all
```

### **Archivos Modificados**
- `sofascore_api.py`: Lógica de extracción de resultados mejorada
- Scripts de análisis: `analyze_results_gap.py`, `fix_all_missing_results.py`
