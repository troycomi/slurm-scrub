from scipy.optimize import least_squares
import numpy as np


class Estimator():
    def __init__(self, target_eff=0.8):
        '''
        target_eff: Efficiency to target.  Too low and will under estimate
        usage
        '''
        self.target_eff = target_eff

    def cost(self, p, x, y):
        val = np.polyval(p, x)  # estimate
        val = np.where(val < 1, 1, val)  # ensure non-negative, non-zero
        fac = np.ceil(np.log2(y/val))  # number of times to rerun if failed
        return np.where(val < y,  # under estimate, rerun?
                        fac + y/(2**fac*val),
                        y/val) - self.target_eff

    def fit(self, xvars: np.array, yvars: np.array, min_deg=1) -> np.array:
        '''
        Perform linear fit with custom cost function
        Assume input sizes are in GB, times in minutes and memory in MB
        Return polynomial values to evaluate with np.polyval(p, x)
        '''

        best = None
        for deg in range(min_deg, 10):
            init = np.zeros(deg)
            init[-1] = 1
            params = least_squares(self.cost, init, args=(xvars, yvars))
            if best is None or best.cost > params.cost:
                best = params

        ests = np.polyval(best.x, xvars)
        failures = sum(yvars > ests)
        eff = yvars / ests
        eff = np.where(eff > 1,
                       yvars / (3 * ests),  # 3 is underestimate
                       eff)

        return {'params': best.x,
                'samples': len(xvars),
                'range': (min(xvars), max(xvars)),
                'failures': failures,
                'mean_eff': np.mean(eff)*100}
