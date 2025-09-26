# Process 2 - Sport-Specific Rules Engine

**Version:** v1.3.0  
**Status:** ✅ **IMPLEMENTED - Football Formulas Ready**  
**Last Updated:** December 26, 2024

## 🎯 **Process 2 Overview**

Process 2 is the sport-specific rules engine that complements Process 1 by evaluating current events using sport-specific formulas and variables calculated in-memory.

### **🏗️ Architecture**

```
process2/
├── __init__.py              # Main Process 2 exports
├── process2_engine.py       # Main engine and orchestration
├── sports/
│   ├── __init__.py          # Sports module exports
│   └── football.py          # Football-specific formulas
└── README.md               # This file
```

## ⚽ **Football Formulas (Implemented)**

### **Variables Calculated:**
```python
β = var_one + var_two          # Sum of home and away variations
ζ = var_one + var_x + var_two  # Sum of all variations
γ = var_x + var_two            # Sum of draw and away variations  
δ = abs(var_x - β)             # Absolute difference between draw and β
ε = abs(var_one) - abs(var_x)  # Difference of absolute values
```

### **11 Implemented Formulas:**

#### **Draw Predictions (Empate):**
1. **Empateγδ**: γ=0 and δ≥0, δ abs ≤ 0.1 → Draw
2. **Empateεζ**: ε=0, ζ abs ≤ 0.1 → Draw
3. **ENA Localγδ**: γ abs ≥ 0, γ ≤0.1, δ≥0.01, δ≤0.04 → Draw

#### **Home Win Predictions (Gana Local):**
4. **Gana Localγδ**: γ=δ or abs(γ-δ)≤0.12, ε≤1.15 → Home wins
5. **Gana Localγδ_var_two**: Same as #4 + var_two=0 → Home wins
6. **Gana Localγδζ**: γ=δ or abs(γ-δ)≤0.1, ε≤1.15, 0≤var_two≤0.05, ζ=0 → Home wins
7. **Gana localεζ**: ε=0, 1<ζ<2 → Home wins

#### **Away Win Predictions (Gana Visita):**
8. **Gana Visitaγδε**: abs(γ+δ)=ε → Away wins
9. **Gana Visitaγδ_var_two**: γ=δ or abs(γ-δ)≤0.1, var_one=0 → Away wins
10. **Gana Visitaγδ**: abs(γ-δ)≤0.1, abs(β+γ)=ε → Away wins
11. **Gana visitaεζ**: ε=0, ζ<1 → Away wins

## 🔄 **Integration with Process 1**

### **Dual Process Flow:**
1. **Trigger**: Same as Process 1 (events at 5 or 30 minutes before start)
2. **Parallel Execution**: Both processes run independently
3. **Comparison**: `prediction_engine.py` compares results
4. **Final Verdict**: AGREE/DISAGREE/PARTIAL based on winner_side comparison

### **Return Format:**
```python
# Both processes return compatible format
(winner_side, point_diff)
# where:
# winner_side: '1' (Home), 'X' (Draw), '2' (Away)  
# point_diff: Always 1 (future enhancement planned)
```

## 📊 **Usage Examples**

### **Direct Usage:**
```python
from process2 import Process2Engine

# Initialize engine
engine = Process2Engine()

# Evaluate single event
report = engine.evaluate_event(event_obj)

# Check results
if report and report.primary_prediction:
    winner_side, point_diff = report.primary_prediction
    print(f"Process 2 predicts: {winner_side} wins")
```

### **Dual Process Usage:**
```python
from prediction_engine import prediction_engine

# Execute both processes
dual_report = prediction_engine.evaluate_dual_process(event_obj, minutes_until_start)

# Check verdict
if dual_report.verdict == ComparisonVerdict.AGREE:
    print(f"Both processes agree: {dual_report.final_prediction[0]} wins")
elif dual_report.verdict == ComparisonVerdict.DISAGREE:
    print("Processes disagree - manual review needed")
```

## 🚀 **Scheduler Integration**

Process 2 is automatically integrated into the scheduler's `job_pre_start_check()`:

1. **Events detected** at key moments (5 or 30 minutes)
2. **Dual process executed** for all qualifying events
3. **Enhanced notifications** sent via Telegram with:
   - Process 1 results (historical patterns)
   - Process 2 results (sport formulas)
   - Final verdict (AGREE/DISAGREE/PARTIAL)
   - Activated formulas details

## 🔧 **Configuration**

### **Tolerance Settings:**
```python
# In football.py
TOLERANCE = 0.001  # Floating point comparison tolerance
```

### **Supported Sports:**
- ✅ **Football**: 11 formulas implemented
- 🟡 **Handball**: Planned (future)
- 🟡 **Rugby**: Planned (future)
- 🟡 **Tennis**: Planned (future)

## 📈 **Performance Characteristics**

- **Memory Usage**: Minimal (in-memory calculations only)
- **Database Impact**: None (no new tables or queries)
- **Processing Time**: <100ms per event
- **Scalability**: Linear with number of events

## 🛠️ **Development Notes**

### **Adding New Sports:**
1. Create new file in `sports/` directory
2. Implement sport-specific formula class
3. Add sport detection in `process2_engine.py`
4. Update `sports/__init__.py` exports

### **Adding New Football Formulas:**
1. Add method to `FootballFormulas` class
2. Include in `get_all_formulas()` list
3. Follow naming convention: `formula_[description]`
4. Include proper logging and error handling

## 🎯 **Future Enhancements**

- **Point Difference Calculation**: Currently fixed at 1, planned for dynamic calculation
- **Weighted Formula Voting**: Currently simple majority, planned for weighted system
- **Confidence Scoring**: Enhanced confidence calculation based on formula reliability
- **Additional Sports**: Handball, Rugby, Tennis formula implementations

---

**Process 2 is ready for production and integrated with the existing Process 1 system!** 🚀⚽🧠
