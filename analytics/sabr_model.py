
import numpy as np
from scipy.optimize import minimize
from typing import List, Optional, Dict
from datetime import datetime
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
                logger.warning("Insufficient data for SABR calibration")
                return False

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
                method='L-BFGS-B',
                options={'maxiter': 1000, 'ftol': 1e-8}
            )

            if result.success:
                self.alpha, self.beta, self.rho, self.nu = result.x
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
                           market_vols: List[float], forward: float, time_to_expiry: float) -> float:
        alpha, beta, rho, nu = params
        try:
            total_error = 0.0
            for strike, market_vol in zip(strikes, market_vols):
                sabr_vol = self.sabr_volatility(strike, forward, time_to_expiry, alpha, beta, rho, nu)
                error = (sabr_vol - market_vol) ** 2
                total_error += error
            return total_error / len(strikes)
        except:
            return 1e6

    def sabr_volatility(self, strike: float, forward: float, time_to_expiry: float,
                        alpha: Optional[float] = None, beta: Optional[float] = None,
                        rho: Optional[float] = None, nu: Optional[float] = None) -> float:
        alpha = alpha or self.alpha
        beta = beta or self.beta
        rho = rho or self.rho
        nu = nu or self.nu

        if strike == forward:
            strike = forward * 1.0001
        fk = forward * strike
        fk_beta = fk ** ((1 - beta) / 2)
        z = (nu / alpha) * fk_beta * np.log(forward / strike)
        xz = np.log((np.sqrt(1 - 2 * rho * z + z ** 2) + z - rho) / (1 - rho))

        if abs(z) < 1e-8:
            term1 = 1 + (((1 - beta) ** 2 / 24) * (alpha ** 2) / (fk ** (1 - beta)) +
                         (1 / 4) * (rho * beta * nu * alpha) / (fk ** ((1 - beta) / 2)) +
                         ((2 - 3 * rho ** 2) / 24) * nu ** 2) * time_to_expiry
            vol = (alpha / (forward ** (1 - beta))) * term1
        else:
            term1 = z / xz
            term2 = 1 + (((1 - beta) ** 2 / 24) * (alpha ** 2) / (fk ** (1 - beta)) +
                         (1 / 4) * (rho * beta * nu * alpha) / (fk ** ((1 - beta) / 2)) +
                         ((2 - 3 * rho ** 2) / 24) * nu ** 2) * time_to_expiry
            vol = (alpha / fk_beta) * term1 * term2

        return max(0.05, min(0.80, vol))

    def get_parameters(self) -> Dict[str, float]:
        return {
            'alpha': self.alpha,
            'beta': self.beta,
            'rho': self.rho,
            'nu': self.nu,
            'calibrated': self.calibrated,
            'last_calibration': self.last_calibration.isoformat() if self.last_calibration else None
        }

    def reset(self):
        self.alpha = 0.2
        self.beta = 0.5
        self.rho = -0.2
        self.nu = 0.3
        self.calibrated = False
        self.last_calibration = None
        logger.debug("SABR model reset to defaults")
