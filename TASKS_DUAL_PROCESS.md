# Dual Process Integration Tasks

**Version:** v1.3 - Dual Process Integration  
**Status:** 🟡 EN DESARROLLO ACTIVO  
**Updated:** 11 Sept 2025

## 🎯 Architecture Overview

**Current:** Process 1 only → **Target:** Process 1 + Process 2 + Comparison

### File Structure Plan
```
process1_engine.py      (renamed from alert_engine.py)
process2_engine.py      (new - sport rules orchestrator)  
prediction_engine.py    (new - dual process orchestrator)
sports/
  ├── handball.py       (8+ rules)
  ├── rugby.py          (8+ rules)
  ├── tennis.py         (placeholder)
  └── football.py       (placeholder)
```

## 📋 Phase 1: Process 1 Refactor

- [ ] **1.1** Rename `alert_engine.py` → `process1_engine.py`
- [ ] **1.2** Add structured return: `[winner_side, point_diff]`
- [ ] **1.3** Update imports in scheduler.py, main.py
- [ ] **1.4** Test backward compatibility

## 📋 Phase 2: Process 2 Creation  

- [ ] **2.1** Create `process2_engine.py` orchestrator
- [ ] **2.2** Create `sports/` directory + base classes
- [ ] **2.3** Implement `sports/handball.py` (8+ rules)
- [ ] **2.4** Implement `sports/rugby.py` (8+ rules)
- [ ] **2.5** Create placeholders: tennis.py, football.py, basketball.py

## 📋 Phase 3: Dual Integration

- [ ] **3.1** Create `prediction_engine.py` orchestrator
- [ ] **3.2** Implement `evaluate_with_both_processes()`
- [ ] **3.3** Add comparison logic (winner_side priority)
- [ ] **3.4** Handle agreement/disagreement scenarios

## 📋 Phase 4: Enhanced Messaging

- [ ] **4.1** Update `alert_system.py` for dual reports
- [ ] **4.2** Create dual process message template:
```
📊 PROCESS 1 - PATTERN ANALYSIS
✅ Status + Prediction + Details

📊 PROCESS 2 - SPORT RULES  
✅ Status + Prediction + Details

🎯 FINAL VERDICT
✅ AGREE/❌ DISAGREE + Final Prediction
```
- [ ] **4.3** Handle disagreement cases (show both)
- [ ] **4.4** Preserve failure message sending

## 📋 Phase 5: Scheduler Integration

- [ ] **5.1** Update scheduler.py to use prediction_engine
- [ ] **5.2** Replace alert_engine calls
- [ ] **5.3** Maintain timing logic (30min, 5min)
- [ ] **5.4** Test full integration

## 📋 Phase 6: Testing & Deployment

- [ ] **6.1** Test with working case (Dan Added vs Stan Wawrinka)
- [ ] **6.2** Test agreement scenarios
- [ ] **6.3** Test disagreement scenarios  
- [ ] **6.4** Update documentation
- [ ] **6.5** Deploy to production

## 🎯 Key Requirements

**Return Format:** Both processes return `[winner_side, point_diff]`  
**Agreement Logic:** winner_side exact match (priority), point_diff tolerance per sport  
**Messaging:** Always send reports when candidates found (success or failure)  
**Modularity:** Each sport in separate file (@rules.mdc compliance)

## 📊 Current Status

**Phase:** 1 - Process 1 Refactor  
**Next:** Add structured return format to Process 1  
**Target:** End September 2025
