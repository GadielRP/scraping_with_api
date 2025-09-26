"""
Process 2 - Sport-Specific Rules Engine

PROCESS 2 BOUNDARIES:
====================
START: This directory contains the complete Process 2 implementation
END: Process 2 ends at the boundaries of this directory

PROCESS 2 DEFINITION:
Process 2 is the sport-specific rules engine that evaluates current events 
using sport-specific formulas and variables to predict outcomes.

PROCESS 2 ARCHITECTURE:
- Sport-specific formula classes (FootballFormulas, HandballFormulas, etc.)
- In-memory calculations for current events only (no database storage)
- Formula-based evaluation with logging and debugging
- Compatible return format with Process 1: (winner_side, point_diff)

CURRENT IMPLEMENTATION (Process 2):
- Football formulas: 11 specific formulas for football events
- Variables: β, ζ, γ, δ, ε calculated in-memory per event
- Formula methods: Each formula as a separate method with logging
- Activation tracking: List of activated formulas with results
"""

from .process2_engine import Process2Engine

__all__ = ['Process2Engine']
