import math 
import numpy as np
from scipy import optimize
from datetime import datetime, timedelta
from typing import List
import logging
from core.config import SABR_BOUNDS

logger = logging.getLogger("VolGuard14")

class EnhancedSABRModel:
    """Production-grade SABR model with robust calibration"""
    
    def __init__(self):
        self.alpha = 0.2
        self.beta = 0.5
        self.rho = -0.2
        self.nu = 0.3
        self.calibrated = False
        self.calibration_error = float('inf')
        self.last_calibration = datetime.now() - timedelta(days=1)
        self.fallback_mode = False

    def sabr_volatility(self, F: float, K: float, T: float) -> float:
        """Hagan's SABR formula with comprehensive error handling"""
        if F <= 0 or K <= 0 or T <= 0: return 0.2 
        
        if abs(F - K) < F * 0.001:
            term1 = ((1 - self.beta) ** 2) / 24 * (self.alpha ** 2) / (F ** (2 - 2 * self.beta))
            term2 = 0.25 * self.rho * self.beta * self.nu * self.alpha / (F ** (1 - self.beta))
            term3 = (2 - 3 * self.rho ** 2) / 24 * self.nu ** 2
            expansion = 1 + (term1 + term2 + term3) * T
            result = (self.alpha / (F ** (1 - self.beta))) * expansion
            return float(np.clip(result, 0.05, 1.5))
        
        try:
            z = (self.nu / self.alpha) * (F * K) ** ((1 - self.beta) / 2) * math.log(F / K)
            
            if abs(z) > 100:
                 return self.alpha / (F ** (1 - self.beta))
                 
            x = math.log((math.sqrt(1 - 2 * self.rho * z + z * z) + z - self.rho) / (1 - self.rho))
            
            numerator = self.alpha * (1 + ((1 - self.beta) ** 2 / 24) * (self.alpha ** 2 / (F * K) ** (1 - self.beta)) * T)
            denominator = (F * K) ** ((1 - self.beta) / 2) * \
                        (1 + (1 - self.beta) ** 2 / 24 * math.log(F / K) ** 2 + (1 - self.beta) ** 4 / 1920 * math.log(F / K) ** 4)
                        
            if abs(denominator) < 1e-7:
                return self.alpha / (F ** (1 - self.beta))
                
            if abs(x) < 1e-6:
                result = numerator / denominator
            else:
                result = numerator / denominator * z / x
                
            return float(np.clip(result, 0.05, 1.5))
            
        except (ValueError, ZeroDivisionError):
            return self.alpha / (F ** (1 - self.beta))

    def calibrate_to_chain(self, strikes: List[float], ivs: List[float], F: float, T: float) -> bool:
        """Robust calibration with validation"""
        if len(strikes) < 5 or T <= 1/365: return False
        
        valid_data = [(K, iv) for K, iv in zip(strikes, ivs) if K > 0 and 0.05 < iv < 1.5 and 0.5 * F < K < 2.0 * F]
        if len(valid_data) < 5: return False
        strikes_clean, ivs_clean = zip(*valid_data)
        
        def objective(params):
            alpha, beta, rho, nu = params
            temp_params = (self.alpha, self.beta, self.rho, self.nu)
            self.alpha, self.beta, self.rho, self.nu = alpha, beta, rho, nu
            errors = []
            for K, market_iv in zip(strikes_clean, ivs_clean):
                model_iv = self.sabr_volatility(F, K, T)
                errors.append((model_iv - market_iv) ** 2)
            self.alpha, self.beta, self.rho, self.nu = temp_params
            return math.sqrt(sum(errors) / len(errors)) if errors else 1.0

        try:
            bounds = list(SABR_BOUNDS.values())
            result = optimize.minimize(
                objective, 
                [0.2, 0.5, -0.2, 0.3], 
                bounds=bounds, 
                method="L-BFGS-B", 
                options={'maxiter': 50, 'ftol': 1e-4}
            )
            
            if result.success:
                self.alpha, self.beta, self.rho, self.nu = result.x
                self.calibration_error = result.fun
                self.calibrated = True
                self.fallback_mode = False
                self.last_calibration = datetime.now()
                
                if not (bounds[0][0] <= self.alpha <= bounds[0][1] and bounds[1][0] <= self.beta <= bounds[1][1] and bounds[2][0] <= self.rho <= bounds[2][1] and bounds[3][0] <= self.nu <= bounds[3][1]):
                    self.calibrated = False
                    return False
                    
                logger.info(f"SABR calibrated: α={self.alpha:.3f} β={self.beta:.3f} ρ={self.rho:.3f} ν={self.nu:.3f}")
                return True
                
        except Exception as e:
            logger.error(f"SABR calibration failed: {e}")
            return False
        return False

    def _validate_parameters(self) -> bool:
        """Validate SABR parameters are within reasonable bounds"""
        return (SABR_BOUNDS['alpha'][0] <= self.alpha <= SABR_BOUNDS['alpha'][1] and
                SABR_BOUNDS['beta'][0] <= self.beta <= SABR_BOUNDS['beta'][1] and
                SABR_BOUNDS['rho'][0] <= self.rho <= SABR_BOUNDS['rho'][1] and
                SABR_BOUNDS['nu'][0] <= self.nu <= SABR_BOUNDS['nu'][1])
