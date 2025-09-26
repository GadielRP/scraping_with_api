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
            self.formula_empate_gamma_delta,
            self.formula_empate_epsilon_zeta,
            self.formula_ena_local_gamma_delta,
            self.formula_gana_local_gamma_delta,
            self.formula_gana_local_gamma_delta_var_two,
            self.formula_gana_local_gamma_delta_zeta,
            self.formula_gana_local_epsilon_zeta,
            self.formula_gana_visita_gamma_delta_epsilon,
            self.formula_gana_visita_gamma_delta_var_two,
            self.formula_gana_visita_gamma_delta,
            self.formula_gana_visita_epsilon_zeta
        ]
    
    def formula_empate_gamma_delta(self) -> Optional[Tuple[str, int]]:
        """
        Empateγδ: γ=0 y δ≥0, δ abs ≤ 0.1; Empate.
        """
        try:
            if self._is_equal(self.γ, 0) and self.δ >= 0 and abs(self.δ) <= 0.1:
                logger.info(f"✅ Fórmula Empateγδ activada: γ={self.γ:.3f}, δ={self.δ:.3f}")
                return ("X", 1)
            return None
        except Exception as e:
            logger.error(f"❌ Error en formula_empate_gamma_delta: {e}")
            return None
    
    def formula_empate_epsilon_zeta(self) -> Optional[Tuple[str, int]]:
        """
        Empateεζ: ε=0, ζ abs ≤ 0.1; Empate.
        """
        try:
            if self._is_equal(self.ε, 0) and abs(self.ζ) <= 0.1:
                logger.info(f"✅ Fórmula Empateεζ activada: ε={self.ε:.3f}, ζ={self.ζ:.3f}")
                return ("X", 1)
            return None
        except Exception as e:
            logger.error(f"❌ Error en formula_empate_epsilon_zeta: {e}")
            return None
    
    def formula_ena_local_gamma_delta(self) -> Optional[Tuple[str, int]]:
        """
        ENA Localγδ: γ=abs ≥ 0, γ ≤0.1, δ≥0.01, δ≤0.04; ENA.
        Nota: ENA (Empate No Aplica) se interpreta como empate por ahora
        """
        try:
            if abs(self.γ) >= 0 and self.γ <= 0.1 and self.δ >= 0.01 and self.δ <= 0.04:
                logger.info(f"✅ Fórmula ENA Localγδ activada: γ={self.γ:.3f}, δ={self.δ:.3f}")
                return ("X", 1)  # Interpretamos ENA como empate por ahora
            return None
        except Exception as e:
            logger.error(f"❌ Error en formula_ena_local_gamma_delta: {e}")
            return None
    
    def formula_gana_local_gamma_delta(self) -> Optional[Tuple[str, int]]:
        """
        Gana Localγδ: γ=δ o la diferencia entre ambos sea abs≤0.12, ε≤1.15; Gana Local
        """
        try:
            condition1 = self._is_equal(self.γ, self.δ) or abs(self.γ - self.δ) <= 0.12
            condition2 = self.ε <= 1.15
            
            if condition1 and condition2:
                logger.info(f"✅ Fórmula Gana Localγδ activada: γ={self.γ:.3f}, δ={self.δ:.3f}, ε={self.ε:.3f}")
                return ("1", 1)
            return None
        except Exception as e:
            logger.error(f"❌ Error en formula_gana_local_gamma_delta: {e}")
            return None
    
    def formula_gana_local_gamma_delta_var_two(self) -> Optional[Tuple[str, int]]:
        """
        Gana Localγδ_var_two: γ=δ o la diferencia entre ambos sea abs≤0.12, ε≤1.15 y var_two=0; Gana Local
        """
        try:
            condition1 = self._is_equal(self.γ, self.δ) or abs(self.γ - self.δ) <= 0.12
            condition2 = self.ε <= 1.15
            condition3 = self._is_equal(self.var_two, 0)
            
            if condition1 and condition2 and condition3:
                logger.info(f"✅ Fórmula Gana Localγδ_var_two activada: γ={self.γ:.3f}, δ={self.δ:.3f}, ε={self.ε:.3f}, var_two={self.var_two:.3f}")
                return ("1", 1)
            return None
        except Exception as e:
            logger.error(f"❌ Error en formula_gana_local_gamma_delta_var_two: {e}")
            return None
    
    def formula_gana_local_gamma_delta_zeta(self) -> Optional[Tuple[str, int]]:
        """
        Gana Localγδζ: γ=δ o la diferencia entre ambos sea abs≤0.1, ε≤1.15, var_two≥0, var_two≤0.05, ζ=0; Gana Local
        """
        try:
            condition1 = self._is_equal(self.γ, self.δ) or abs(self.γ - self.δ) <= 0.1
            condition2 = self.ε <= 1.15
            condition3 = self.var_two >= 0 and self.var_two <= 0.05
            condition4 = self._is_equal(self.ζ, 0)
            
            if condition1 and condition2 and condition3 and condition4:
                logger.info(f"✅ Fórmula Gana Localγδζ activada: γ={self.γ:.3f}, δ={self.δ:.3f}, ε={self.ε:.3f}, var_two={self.var_two:.3f}, ζ={self.ζ:.3f}")
                return ("1", 1)
            return None
        except Exception as e:
            logger.error(f"❌ Error en formula_gana_local_gamma_delta_zeta: {e}")
            return None
    
    def formula_gana_local_epsilon_zeta(self) -> Optional[Tuple[str, int]]:
        """
        Gana localεζ: ε=0, ζ>1, ζ<2; Gana Local.
        """
        try:
            if self._is_equal(self.ε, 0) and self.ζ > 1 and self.ζ < 2:
                logger.info(f"✅ Fórmula Gana localεζ activada: ε={self.ε:.3f}, ζ={self.ζ:.3f}")
                return ("1", 1)
            return None
        except Exception as e:
            logger.error(f"❌ Error en formula_gana_local_epsilon_zeta: {e}")
            return None
    
    def formula_gana_visita_gamma_delta_epsilon(self) -> Optional[Tuple[str, int]]:
        """
        Gana Visitaγδε: abs(γ+δ)=ε; Gana Visita
        """
        try:
            if self._is_equal(abs(self.γ + self.δ), self.ε):
                logger.info(f"✅ Fórmula Gana Visitaγδε activada: γ={self.γ:.3f}, δ={self.δ:.3f}, ε={self.ε:.3f}, abs(γ+δ)={abs(self.γ + self.δ):.3f}")
                return ("2", 1)
            return None
        except Exception as e:
            logger.error(f"❌ Error en formula_gana_visita_gamma_delta_epsilon: {e}")
            return None
    
    def formula_gana_visita_gamma_delta_var_two(self) -> Optional[Tuple[str, int]]:
        """
        Gana Visitaγδ_var_two: γ=δ o la diferencia entre ambos sea abs≤0.1, var_one=0; Gana Visita.
        """
        try:
            condition1 = self._is_equal(self.γ, self.δ) or abs(self.γ - self.δ) <= 0.1
            condition2 = self._is_equal(self.var_one, 0)
            
            if condition1 and condition2:
                logger.info(f"✅ Fórmula Gana Visitaγδ_var_two activada: γ={self.γ:.3f}, δ={self.δ:.3f}, var_one={self.var_one:.3f}")
                return ("2", 1)
            return None
        except Exception as e:
            logger.error(f"❌ Error en formula_gana_visita_gamma_delta_var_two: {e}")
            return None
    
    def formula_gana_visita_gamma_delta(self) -> Optional[Tuple[str, int]]:
        """
        Gana Visitaγδ: γ=δ con diferencia entre ambos abs≤0.1, abs(β+γ)=ε; Gana Visita
        """
        try:
            condition1 = abs(self.γ - self.δ) <= 0.1
            condition2 = self._is_equal(abs(self.β + self.γ), self.ε)
            
            if condition1 and condition2:
                logger.info(f"✅ Fórmula Gana Visitaγδ activada: γ={self.γ:.3f}, δ={self.δ:.3f}, β={self.β:.3f}, ε={self.ε:.3f}, abs(β+γ)={abs(self.β + self.γ):.3f}")
                return ("2", 1)
            return None
        except Exception as e:
            logger.error(f"❌ Error en formula_gana_visita_gamma_delta: {e}")
            return None
    
    def formula_gana_visita_epsilon_zeta(self) -> Optional[Tuple[str, int]]:
        """
        Gana visitaεζ: ε=0, ζ < 1; GanaVisita
        """
        try:
            if self._is_equal(self.ε, 0) and self.ζ < 1:
                logger.info(f"✅ Fórmula Gana visitaεζ activada: ε={self.ε:.3f}, ζ={self.ζ:.3f}")
                return ("2", 1)
            return None
        except Exception as e:
            logger.error(f"❌ Error en formula_gana_visita_epsilon_zeta: {e}")
            return None
