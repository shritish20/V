import numpy as np
from scipy.optimize import minimize
from typing import List, Optional, Dict
from datetime import datetime
import logging
from core.config import settings

logger = logging.getLogger("VolGuard18")


class EnhancedSABRModel:
    def __init__(self):
        self.alpha = 0.2
        self.beta = 0.5
        self.rho = -0.2
        self.nu = 0.3
        self.calibrated = False
        self.last_calibration = None

    def calibrate_to_chain(self, strikes: List[float], market_vols: List[float],
                          forward: float, time_to_expiry: float) -> bool:
        try:
            if len(strikes) != len(market_vols) or len(strikes) < 3:
                return False

            initial_guess = [self.alpha, self.beta, self.rho, self.nu]
            
            # Use safe bounds from config
            bounds = [
                settings.SABR_BOUNDS['alpha'],
                settings.SABR_BOUNDS['beta'],
                settings.SABR_BOUNDS['rho'],
                settings.SABR_BOUNDS['nu']
            ]

            result = minimize(
                fun=self._calibration_error,
                x0=initial_guess,
                args=(strikes, market_vols, forward, time_to_expiry),
                bounds=bounds,
                method='L-BFGS-B',
                options={'maxiter': 1000, 'ftol': 1e-8}
            )

            if result.success:
                self.alpha, self.beta, self.rho, self.nu = result.x
                
                # ============================================
                # 🔧 FIX: Added safety assertion for rho
                # ============================================
                # Ensure rho is strictly within bounds to prevent division by zero
                assert -0.99 < self.rho < 0.99, f"rho={self.rho} must be strictly between -0.99 and 0.99"
                
                self.calibrated = True
                self.last_calibration = datetime.now()
                logger.info(f"SABR calibrated: α={self.alpha:.3f}, β={self.beta:.3f}, ρ={self.rho:.3f}, ν={self.nu:.3f}")
                return True
            else:
                logger.warning(f"SABR calibration failed: {result.message}")
                return False
                
        except Exception as e:
            logger.error(f"SABR calibration error: {e}")
            return False

    def _calibration_error(self, params: List[float], strikes: List[float],
                          market_vols: List[float], forward: float, 
                          time_to_expiry: float) -> float:
        alpha, beta, rho, nu = params
        
        try:
            total_error = 0.0
            for strike, market_vol in zip(strikes, market_vols):
                sabr_vol = self.sabr_volatility(strike, forward, time_to_expiry, 
                                               alpha, beta, rho, nu)
                total_error += (sabr_vol - market_vol) ** 2
            return total_error / len(strikes)
        except:
            return 1e6

    def sabr_volatility(self, strike: float, forward: float, time_to_expiry: float,
                       alpha=None, beta=None, rho=None, nu=None) -> float:
        alpha = alpha or self.alpha
        beta = beta or self.beta
        rho = rho or self.rho
        nu = nu or self.nu

        if strike <= 0 or forward <= 0:
            return 0.0

        try:
            # ============================================
            # 🔧 FIX: Added explicit safety check for rho
            # ============================================
            if abs(rho) >= 0.99:
                logger.warning(f"rho={rho} too close to ±1, clamping to safe range")
                rho = 0.98 if rho > 0 else -0.98
            
            # Handle ATM case where log(F/K) = 0
            if abs(strike - forward) < 1e-5:
                strike = forward * 1.00001

            fk_beta = (forward * strike) ** ((1 - beta) / 2)
            z = (nu / alpha) * fk_beta * np.log(forward / strike)

            # X(z) function
            # Avoid negative sqrt inputs
            inside_sqrt = 1 - 2 * rho * z + z ** 2
            if inside_sqrt < 0:
                inside_sqrt = 0
            
            # Safe division: (1 - rho) is guaranteed non-zero due to bounds
            xz = np.log((np.sqrt(inside_sqrt) + z - rho) / (1 - rho))

            # Expansion terms
            term1 = (alpha / fk_beta) / (1 + ((1 - beta)**2 / 24) * np.log(forward / strike)**2 +
                                        ((1 - beta)**4 / 1920) * np.log(forward / strike)**4)

            term2 = 1 + (
                ((1 - beta)**2 / 24) * (alpha**2 / (forward * strike)**(1 - beta)) +
                (0.25 * rho * beta * nu * alpha / fk_beta) +
                ((2 - 3 * rho**2) / 24) * nu**2
            ) * time_to_expiry

            # Handle small z case
            if abs(xz) < 1e-10:
                vol = term1 * term2
            else:
                vol = term1 * (z / xz) * term2

            return max(0.01, min(2.0, vol))  # Clamp 1% to 200%
            
        except Exception as e:
            logger.debug(f"SABR calculation fallback triggered: {e}")
            return 0.20

    def reset(self):
        """Reset model to default parameters"""
        self.alpha = 0.2
        self.beta = 0.5
        self.rho = -0.2
        self.nu = 0.3
        self.calibrated = False
        self.last_calibration = None
        logger.debug("SABR model reset to defaults")
