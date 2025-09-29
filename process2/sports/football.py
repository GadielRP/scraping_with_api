#!/usr/bin/env python3
"""
Football Formulas - Process 2 Sport-Specific Rules for Football

FOOTBALL FORMULA BOUNDARIES:
============================
START: This file contains all football-specific formulas for Process 2
END: Football formulas end at the end of this file

FOOTBALL VARIABLES:
β = var_one + var_two
ζ = var_one + var_x + var_two  
γ = var_x + var_two
δ = abs(var_x - β)
ε = abs(var_one) - abs(var_x)

FORMULA STRUCTURE:
- Each formula is a method that returns (winner_side, point_diff) or None
- Logging for debugging and activation tracking
- Floating point tolerance for equality comparisons
"""

import logging
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

# Floating point tolerance for equality comparisons
TOLERANCE = 0.001

class FootballFormulas:
    """Football-specific formulas for Process 2"""
    
    def __init__(self, var_one: float, var_x: float, var_two: float):
        """
        Initialize football formulas with event variations.
        
        Args:
            var_one: Variation for home odds (one_final - one_open)
            var_x: Variation for draw odds (x_final - x_open) 
            var_two: Variation for away odds (two_final - two_open)
        """
        try:
            self.var_one = float(var_one)
            self.var_x = float(var_x)
            self.var_two = float(var_two)
            
            # Calculate all variables ONCE per event
            self.β = self.var_one + self.var_two
            self.ζ = self.var_one + self.var_x + self.var_two
            self.γ = self.var_x + self.var_two
            self.δ = abs(self.var_x - self.β)
            self.ε = abs(self.var_one) - abs(self.var_x)
            
            logger.info(f"🏗️ Football variables calculated: β={self.β:.3f}, ζ={self.ζ:.3f}, γ={self.γ:.3f}, δ={self.δ:.3f}, ε={self.ε:.3f}")
            
        except Exception as e:
            logger.error(f"❌ Error calculating football variables: {e}")
            raise
    
    def _is_equal(self, a: float, b: float) -> bool:
        """Check if two values are equal within tolerance"""
        return abs(a - b) <= TOLERANCE
    
    def get_all_formulas(self):
        """Return list of all formula methods"""
        return [
            self.formula_gana_visita_epsilon_gamma,
            self.formula_empatan_epsilon_gamma,
            self.formula_gana_local_epsilon_gamma
        ]
    
    def _is_equal_with_tolerance(self, a: float, b: float, tolerance: float = 0.04) -> bool:
        """Check if two values are equal within specified tolerance"""
        return abs(a - b) <= tolerance
    
    def formula_gana_visita_epsilon_gamma(self) -> Optional[Tuple[str, int]]:
        """
        Gana Visita: abs(ε=γ) con una tolerancia para la igualdad de 0.04 inclusiva,
        δ=ζ, β=0. Gana Visita.
        """
        try:
            # abs(ε=γ) con tolerancia de 0.04 inclusiva
            condition1 = self._is_equal_with_tolerance(abs(self.ε), abs(self.γ), 0.04)
            # δ=ζ
            condition2 = self._is_equal(self.δ, self.ζ)
            # β=0
            condition3 = self._is_equal(self.β, 0)
            
            if condition1 and condition2 and condition3:
                logger.info(f"✅ Fórmula Gana Visita activada: ε={self.ε:.3f}, γ={self.γ:.3f}, δ={self.δ:.3f}, ζ={self.ζ:.3f}, β={self.β:.3f}")
                return ("2", 1)
            return None
        except Exception as e:
            logger.error(f"❌ Error en formula_gana_visita_epsilon_gamma: {e}")
            return None
    
    def formula_empatan_epsilon_gamma(self) -> Optional[Tuple[str, int]]:
        """
        Empatan: ε=γ, β=0.10, abs(ζ-β)=ε=γ. Empate.
        """
        try:
            # ε=γ
            condition1 = self._is_equal(self.ε, self.γ)
            # β=0.10
            condition2 = self._is_equal(self.β, 0.10)
            # abs(ζ-β)=ε=γ (abs(ζ-β) debe ser igual a ε y γ)
            condition3 = self._is_equal(abs(self.ζ - self.β), self.ε) and self._is_equal(abs(self.ζ - self.β), self.γ)
            
            if condition1 and condition2 and condition3:
                logger.info(f"✅ Fórmula Empatan activada: ε={self.ε:.3f}, γ={self.γ:.3f}, β={self.β:.3f}, ζ={self.ζ:.3f}, abs(ζ-β)={abs(self.ζ - self.β):.3f}")
                return ("X", 1)
            return None
        except Exception as e:
            logger.error(f"❌ Error en formula_empatan_epsilon_gamma: {e}")
            return None
    
    def formula_gana_local_epsilon_gamma(self) -> Optional[Tuple[str, int]]:
        """
        Gana local: ε=γ, β+ζ=δ. Gana Local.
        """
        try:
            # ε=γ
            condition1 = self._is_equal(self.ε, self.γ)
            # β+ζ=δ
            condition2 = self._is_equal(self.β + self.ζ, self.δ)
            
            if condition1 and condition2:
                logger.info(f"✅ Fórmula Gana local activada: ε={self.ε:.3f}, γ={self.γ:.3f}, β={self.β:.3f}, ζ={self.ζ:.3f}, δ={self.δ:.3f}")
                return ("1", 1)
            return None
        except Exception as e:
            logger.error(f"❌ Error en formula_gana_local_epsilon_gamma: {e}")
            return None
