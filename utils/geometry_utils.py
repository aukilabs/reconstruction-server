import numpy as np
import pyceres


# TODO: Move to C++. Low prio since the global refinement is pretty fast anyway.
class QuaternionNormalizationCostFunction(pyceres.CostFunction):
    def __init__(self, weight=1.0):
        super().__init__()
        self.set_num_residuals(1)
        self.set_parameter_block_sizes([4])
        self.weight = weight

    def Evaluate(self, parameters, residuals, jacobians):
        q = parameters[0]
        n = np.linalg.norm(q)
        # residual = (1 - ||q||)^2
        residuals[0] = (1.0 - n)**2 * self.weight

        if jacobians is not None:
            # avoid division by zero
            if n > 0:
                factor = -2.0 * (1.0 - n) / n * self.weight
                for i in range(4):
                    jacobians[0][i] = factor * q[i]
            else:
                # Jacobian undefined at zero; you could set it to zero or some large value
                for i in range(4):
                    jacobians[0][i] = 0.0

        return True


class TranslationPriorCostFunction(pyceres.CostFunction):
    """Soft prior: residual = weight * (translation - target)."""

    def __init__(self, target=(0.0, 0.0, 0.0), weight=1.0):
        super().__init__()
        self.set_num_residuals(3)
        self.set_parameter_block_sizes([3])
        self.target = np.asarray(target, dtype=float).reshape(3)
        self.weight = float(weight)

    def Evaluate(self, parameters, residuals, jacobians):
        t = np.asarray(parameters[0], dtype=float).reshape(3)
        err = self.weight * (t - self.target)
        residuals[0] = float(err[0])
        residuals[1] = float(err[1])
        residuals[2] = float(err[2])
        if jacobians is not None and jacobians[0] is not None:
            # d(r)/d(t) = weight * I
            jacobians[0][:] = 0.0
            jacobians[0][0] = self.weight
            jacobians[0][4] = self.weight
            jacobians[0][8] = self.weight
        return True
