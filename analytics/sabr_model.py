import numpy as np
from scipy.optimize import minimize
from typing import List
import logging
from core.config import settings

logger = logging.getLogger("VolGuardSABR")

class EnhancedSABRModel:
    def __init__(self):
        self.alpha = 0.2
        self.beta = 0.5
        self.rho = -0.2
        self.nu = 0.3
        self.calibrated = False
        self.last_valid_params = {'alpha': 0.2, 'beta': 0.5, 'rho': -0.2, 'nu': 0.3}

    def calibrate_to_chain(self, strikes: List[float], market_vols: List[float], forward: float, time_to_expiry: float) -> bool:
        try:
            if len(strikes) < 3: return False
            initial_guess = [self.alpha, self.beta, self.rho, self.nu]
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
                method='L-BFGS-B'
            )
            if result.success:
                self.alpha, self.beta, self.rho, self.nu = result.x
                self.last_valid_params = {'alpha': self.alpha, 'beta': self.beta, 'rho': self.rho, 'nu': self.nu}
                self.calibrated = True
                logger.info(f"SABR Calibrated: a={self.alpha:.2f} p={self.rho:.2f}")
                return True
            else:
                self._rollback()
                return False
        except Exception as e:
            logger.error(f"SABR Error: {e}")
            self._rollback()
            return False

    def _rollback(self):
        p = self.last_valid_params
        self.alpha, self.beta, self.rho, self.nu = p['alpha'], p['beta'], p['rho'], p['nu']
        self.calibrated = True

    def sabr_volatility(self, strike: float, forward: float, time_to_expiry: float, alpha=None, beta=None, rho=None, nu=None) -> float:
        alpha = alpha or self.alpha
        beta = beta or self.beta
        rho = rho or self.rho
        nu = nu or self.nu
        
        if strike <= 0 or forward <= 0 or time_to_expiry <= 0: return 0.0
        try:
            log_fk = np.log(forward / strike)
            fk_beta = (forward * strike) ** ((1 - beta) / 2)
            z = (nu / alpha) * fk_beta * log_fk
            inside_sqrt = 1 - 2 * rho * z + z ** 2
            if inside_sqrt < 0: inside_sqrt = 0
            x_z = np.log((np.sqrt(inside_sqrt) + z - rho) / (1 - rho))
            if abs(x_z) < 1e-5: x_z = 1e-5
            
            term1 = alpha / (fk_beta * (1 + ((1 - beta) ** 2 / 24) * log_fk ** 2))
            term2 = 1 + (((1 - beta) ** 2 / 24) * alpha ** 2 / (forward * strike) ** (1 - beta)) * time_to_expiry
            vol = term1 * (z / x_z) * term2
            return max(0.01, min(5.0, vol))
        except:
            return 0.20

    def _calibration_error(self, params, strikes, market_vols, F, T):
        err = 0.0
        for k, v_mkt in zip(strikes, market_vols):
            v_model = self.sabr_volatility(k, F, T, *params)
            err += (v_model - v_mkt) ** 2
        return err
