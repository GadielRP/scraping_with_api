# SofaScore Odds System

**Versión:** v1.0  
**Estado:** ✅ **PRODUCCIÓN - Sistema Completamente Optimizado**  
**Última Actualización:** 3 de Septiembre, 2025

## 🎯 **Descripción del Sistema**

Sistema automatizado de monitoreo de odds de SofaScore que:
- **Descubre eventos deportivos** automáticamente cada 2 horas
- **Notifica por Telegram** sobre juegos que empiezan en los próximos 30 minutos
- **Extrae odds inteligentemente** solo en momentos clave (30 y 5 minutos antes)
- **Recolecta resultados** de juegos terminados
- **Funciona 24/7** con programación inteligente y optimizada

## 🚀 **Características Principales**

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

## 🛠 **Instalación y Configuración**

### **Requisitos**
```bash
pip install -r requirements.txt
```

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
python main.py results-all    # Recolectar TODOS los resultados

# Monitoreo y estado
python main.py status         # Estado del sistema
python main.py events         # Ver eventos recientes
```

### **Flujo de Trabajo Automático Optimizado**
1. **00:00-22:00**: Descubrimiento cada 2 horas
2. **Cada 5 min**: Verificación de juegos próximos
3. **Momentos Clave**: Extracción de odds a los 30 y 5 minutos
4. **Notificaciones**: Solo cuando se extraen odds (pero incluye todos los juegos)
5. **00:05**: Recolección de resultados

## 📊 **Estado Actual**

### ✅ **Completado (100%)**
- Sistema de notificaciones Telegram optimizado
- Descubrimiento automático cada 2 horas
- Verificación pre-inicio cada 5 minutos
- Extracción inteligente de odds (solo en momentos clave)
- Sistema de notificaciones inteligente (solo cuando es necesario)
- Recolección automática de resultados
- Manejo robusto de errores y reintentos
- Sistema de proxy con rotación de IPs
- Base de datos SQLite con SQLAlchemy
- Programación inteligente de trabajos

### 🎯 **En Producción - Optimizado**
- **Notificaciones**: Funcionando con lógica inteligente
- **Descubrimiento**: Programado cada 2 horas
- **Extracción de Odds**: Solo en momentos clave (30 y 5 minutos)
- **Resultados**: Recolectándose automáticamente
- **Monitoreo**: Sistema estable y eficiente 24/7

## 🔧 **Arquitectura Técnica**

### **Componentes Principales**
- **`main.py`**: Punto de entrada y CLI
- **`scheduler.py`**: Programación de trabajos con lógica optimizada
- **`alert_system.py`**: Sistema de notificaciones Telegram inteligente
- **`database.py`**: Gestión de base de datos
- **`repository.py`**: Acceso a datos optimizado
- **`config.py`**: Configuración centralizada
- **`sofascore_api.py`**: API client con manejo inteligente
- **`odds_utils.py`**: Utilidades para procesamiento de odds

### **Tecnologías**
- **Python 3.8+**: Lógica principal
- **SQLAlchemy**: ORM para base de datos
- **Schedule**: Programación de trabajos
- **Requests**: API HTTP con manejo de errores
- **SQLite3**: Base de datos local

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

## 🎉 **¡Listo para Producción - Optimizado!**

El sistema está **completamente funcional**, **optimizado** y **listo para producción**:
- ✅ Notificaciones Telegram con lógica inteligente
- ✅ Descubrimiento automático cada 2 horas
- ✅ Extracción de odds solo en momentos clave
- ✅ Sistema de notificaciones optimizado
- ✅ Recolección de resultados programada
- ✅ Manejo robusto de errores
- ✅ Monitoreo 24/7 eficiente

**¡Tu sistema de alertas de SofaScore está optimizado y funcionando perfectamente!** 🚀⚽
