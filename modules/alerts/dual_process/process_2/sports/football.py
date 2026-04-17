"""Football formulas for Process 2."""

import logging
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

TOLERANCE = 0.001


class FootballFormulas:
    """Football-specific formulas for Process 2."""

    def __init__(self, var_one: float, var_x: float, var_two: float):
        self.var_one = float(var_one)
        self.var_x = float(var_x)
        self.var_two = float(var_two)

        self.beta = self.var_one + self.var_two
        self.zeta = self.var_one + self.var_x + self.var_two
        self.gamma = self.var_x + self.var_two
        self.delta = abs(self.var_x - self.beta)
        self.epsilon = abs(self.var_one) - abs(self.var_x)

        logger.info(
            "[PROCESS2] Football vars calculated: beta=%.3f, zeta=%.3f, gamma=%.3f, delta=%.3f, epsilon=%.3f",
            self.beta,
            self.zeta,
            self.gamma,
            self.delta,
            self.epsilon,
        )

    def _is_equal(self, a: float, b: float) -> bool:
        return abs(a - b) <= TOLERANCE

    def _is_equal_with_tolerance(self, a: float, b: float, tolerance: float = 0.04) -> bool:
        return abs(a - b) <= tolerance

    def get_all_formulas(self):
        return [
            self.formula_gana_visita_epsilon_gamma,
            self.formula_empatan_epsilon_gamma,
            self.formula_gana_local_epsilon_gamma,
        ]

    def formula_gana_visita_epsilon_gamma(self) -> Optional[Tuple[str, int]]:
        """Away win formula."""
        try:
            condition1 = self._is_equal_with_tolerance(abs(self.epsilon), abs(self.gamma), 0.04)
            condition2 = self._is_equal(self.delta, self.zeta)
            condition3 = self._is_equal(self.beta, 0)

            if condition1 and condition2 and condition3:
                logger.info(
                    "[PROCESS2] Formula gana visita activated: epsilon=%.3f, gamma=%.3f, delta=%.3f, zeta=%.3f, beta=%.3f",
                    self.epsilon,
                    self.gamma,
                    self.delta,
                    self.zeta,
                    self.beta,
                )
                return ("2", 1)
            return None
        except Exception as e:
            logger.error("Error in formula_gana_visita_epsilon_gamma: %s", e)
            return None

    def formula_empatan_epsilon_gamma(self) -> Optional[Tuple[str, int]]:
        """Draw formula."""
        try:
            condition1 = self._is_equal(self.epsilon, self.gamma)
            condition2 = self._is_equal(self.beta, 0.10)
            abs_diff = abs(self.zeta - self.beta)
            condition3 = self._is_equal(abs_diff, self.epsilon) and self._is_equal(abs_diff, self.gamma)

            if condition1 and condition2 and condition3:
                logger.info(
                    "[PROCESS2] Formula empate activated: epsilon=%.3f, gamma=%.3f, beta=%.3f, zeta=%.3f, abs_diff=%.3f",
                    self.epsilon,
                    self.gamma,
                    self.beta,
                    self.zeta,
                    abs_diff,
                )
                return ("X", 1)
            return None
        except Exception as e:
            logger.error("Error in formula_empatan_epsilon_gamma: %s", e)
            return None

    def formula_gana_local_epsilon_gamma(self) -> Optional[Tuple[str, int]]:
        """Home win formula."""
        try:
            condition1 = self._is_equal(self.epsilon, self.gamma)
            condition2 = self._is_equal(self.beta + self.zeta, self.delta)

            if condition1 and condition2:
                logger.info(
                    "[PROCESS2] Formula gana local activated: epsilon=%.3f, gamma=%.3f, beta=%.3f, zeta=%.3f, delta=%.3f",
                    self.epsilon,
                    self.gamma,
                    self.beta,
                    self.zeta,
                    self.delta,
                )
                return ("1", 1)
            return None
        except Exception as e:
            logger.error("Error in formula_gana_local_epsilon_gamma: %s", e)
            return None


__all__ = ["FootballFormulas"]
