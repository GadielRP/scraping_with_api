# SofaScore Odds Alert System

## 🎯 **Current Status: v0.9 - Comprehensive Results Collection & Sport Intelligence**

A sophisticated sports odds monitoring system that automatically discovers events, tracks odds movements, and collects final results with sport-specific intelligence.

### ✨ **Recent Improvements (v0.9)**

- **🏆 Comprehensive Results Collection**: New system to fetch results for ALL finished events in the database
- **🎯 Sport-Specific Intelligence**: Smart cutoff times for different sports (Football: 2.5h, Tennis: 4h, Baseball: 4h, Basketball: 3h)
- **🔄 Enhanced Error Handling**: Robust retry mechanisms with exponential backoff for HTTP 407, 429, and 5xx errors
- **📊 Flexible Odds Extraction**: Sport-agnostic odds parsing that adapts to different market structures
- **⚡ Configurable Discovery**: Discovery interval configurable via `.env` file (default: every 2 hours)
- **🕐 Clock-Aligned Scheduling**: Pre-start checks run at precise 5-minute intervals (hh:00, hh:05, hh:10, etc.)
- **🌍 Timezone Accuracy**: Fixed timezone handling for accurate event detection and scheduling

### 🚀 **System Architecture**

- **API Client**: `curl-cffi` with browser impersonation to bypass anti-bot measures
- **Proxy System**: Oxylabs residential proxies for IP rotation and anonymity
- **Database**: SQLite3 with SQLAlchemy ORM (`Event`, `OddsSnapshot`, `EventOdds`, `Result`, `AlertLog`)
- **Scheduler**: Python `schedule` library for automated job execution
- **Logging**: Structured logging to console and `sofascore_odds.log`
- **Alert System**: Real-time rules for odds drops, convergence, and extreme values

### 📅 **Scheduled Jobs**

- **Discovery**: Every 2 hours (00:00, 02:00, 04:00, 06:00, 08:00, 10:00, 12:00, 14:00, 16:00, 18:00, 20:00, 22:00)
- **Pre-start Check**: Every 5 minutes (clock-aligned)
- **Results Collection**: Daily at 00:05 (previous day's events)
- **Comprehensive Results**: Manual trigger for ALL finished events

### 🎮 **Usage**

```bash
# Start the system
python main.py start

# Check system status
python main.py status

# Manual results collection (yesterday's events)
python main.py results

# Comprehensive results collection (ALL finished events)
python main.py results-all

# Stop the system
python main.py stop
```

### 🔧 **Configuration**

Set your proxy credentials in `.env`:
```env
PROXY_USERNAME=your_username
PROXY_PASSWORD=your_password
DISCOVERY_INTERVAL_HOURS=2
```

### 📊 **Current Metrics**

- **Events in database**: 60+
- **Results collected**: 31+ (100% success rate for truly finished games)
- **Alert system**: 1200+ alerts generated
- **Sport coverage**: Football, Tennis, Baseball, Basketball, Futsal, and more

### 🎯 **Next Steps**

- **Performance Optimization**: Further reduce scheduler.py line count if needed
- **Enhanced Monitoring**: Add real-time system health metrics
- **API Rate Limiting**: Implement adaptive rate limiting based on response patterns
- **Data Analytics**: Historical odds movement analysis and reporting

---

**Version**: v0.9  
**Last Updated**: 2025-09-02  
**Status**: Production Ready ✅
