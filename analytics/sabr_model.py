import numpy as np
from scipy.optimize import minimize
from typing import List, Tuple, Optional
import logging
from core.config import settings

logger = logging.getLogger("VolGuardSABR")

# ============================================================================
# PROCESS-SAFE WORKER (No class dependencies, no DB, no logs)
# ============================================================================
def _worker_calibrate_sabr(
    initial_guess: List[float],
    bounds: List[Tuple[float, float]],
    strikes: List[float],
    market_vols: List[float],
    forward: float,
    time_to_expiry: float
) -> Optional[Tuple[List[float], float]]:
    """
    Standalone SABR calibration for ProcessPool.
    Returns: (params, error) or None on failure.
    Running in a separate process prevents the GIL from blocking the main loop.
    """
    try:
        def _sabr_vol(k, F, T, alpha, beta, rho, nu):
            """Pure math - no class dependencies"""
            if k <= 0 or F <= 0 or T <= 0:
                return 0.20
            
            try:
                log_fk = np.log(F / k)
                fk_beta = (F * k) ** ((1 - beta) / 2)
                z = (nu / alpha) * fk_beta * log_fk
                
                inside_sqrt = max(0, 1 - 2 * rho * z + z ** 2)
                x_z = np.log((np.sqrt(inside_sqrt) + z - rho) / (1 - rho))
                
                if abs(x_z) < 1e-8:
                    x_z = 1.0  # Avoid division by zero
                
                term1 = alpha / (fk_beta * (1 + ((1 - beta) ** 2 / 24) * log_fk ** 2))
                term2 = 1 + (((1 - beta) ** 2 / 24) * alpha ** 2 / 
                            ((F * k) ** (1 - beta)) + 
                            (rho * beta * nu * alpha) / (4 * fk_beta) +
                            ((2 - 3 * rho ** 2) * nu ** 2) / 24) * T
                
                return term1 * (z / x_z) * term2
            except:
                return 0.20
        
        def _objective(params):
            alpha, beta, rho, nu = params
            error = 0.0
            for k, v_mkt in zip(strikes, market_vols):
                v_model = _sabr_vol(k, forward, time_to_expiry, alpha, beta, rho, nu)
                error += (v_model - v_mkt) ** 2
            return error
        
        result = minimize(
            fun=_objective,
            x0=initial_guess,
            bounds=bounds,
            method='L-BFGS-B',
            options={
                'maxiter': 200,
                'ftol': 1e-6,
                'gtol': 1e-5
            }
        )
        
        if result.success and result.fun < 1.0:  # Sanity check on error
            return (result.x.tolist(), result.fun)
        
        return None
        
    except Exception:
        return None


# ============================================================================
# MAIN SABR CLASS
# ============================================================================
class EnhancedSABRModel:
    def __init__(self):
        # Default params
        self.alpha = 0.2
        self.beta = 0.5
        self.rho = -0.2
        self.nu = 0.3
        self.calibrated = False
        
        # Last known good state
        self._last_valid_params = {
            'alpha': 0.2, 'beta': 0.5, 'rho': -0.2, 'nu': 0.3
        }
        self._last_error = float('inf')
    
    def get_current_params(self) -> List[float]:
        """Returns params for worker process"""
        return [self.alpha, self.beta, self.rho, self.nu]
    
    def update_params(self, params: List[float], error: float = None):
        """Updates params from worker result"""
        if params and len(params) == 4:
            # Sanity bounds check
            alpha, beta, rho, nu = params
            if 0.01 <= alpha <= 2.0 and 0.1 <= beta <= 1.0 and \
               -0.99 <= rho <= 0.99 and 0.01 <= nu <= 5.0:
                
                self.alpha, self.beta, self.rho, self.nu = params
                self.calibrated = True
                
                # Update cache
                self._last_valid_params = {
                    'alpha': self.alpha, 'beta': self.beta,
                    'rho': self.rho, 'nu': self.nu
                }
                if error is not None:
                    self._last_error = error
                
                logger.info(f"✅ SABR: α={self.alpha:.3f} β={self.beta:.3f} "
                           f"ρ={self.rho:.3f} ν={self.nu:.3f} err={error:.4f}")
            else:
                logger.warning("⚠️ SABR params out of bounds - rejected")
    
    def use_cached_params(self):
        """Fallback to last known good params"""
        p = self._last_valid_params
        self.alpha = p['alpha']
        self.beta = p['beta']
        self.rho = p['rho']
        self.nu = p['nu']
        self.calibrated = True
        logger.info("📦 Using cached SABR params")
    
    def sabr_volatility(self, strike: float, forward: float, 
                        time_to_expiry: float) -> float:
        """Fast volatility calculation (main thread safe)"""
        if strike <= 0 or forward <= 0 or time_to_expiry <= 0:
            return 0.0
        
        try:
            log_fk = np.log(forward / strike)
            fk_beta = (forward * strike) ** ((1 - self.beta) / 2)
            z = (self.nu / self.alpha) * fk_beta * log_fk
            
            inside_sqrt = max(0, 1 - 2 * self.rho * z + z ** 2)
            x_z = np.log((np.sqrt(inside_sqrt) + z - self.rho) / (1 - self.rho))
            
            if abs(x_z) < 1e-8:
                x_z = 1.0
            
            term1 = self.alpha / (fk_beta * (1 + ((1 - self.beta) ** 2 / 24) * log_fk ** 2))
            term2 = 1 + (((1 - self.beta) ** 2 / 24) * self.alpha ** 2 / 
                        ((forward * strike) ** (1 - self.beta))) * time_to_expiry
            
            vol = term1 * (z / x_z) * term2
            return max(0.01, min(5.0, vol))
        except:
            return 0.20
