#!/usr/bin/env python3
"""Ceres solver smoke test for basic functionality."""

from __future__ import annotations

import sys

import numpy as np
import pyceres

class HelloworldCostFunction(pyceres.CostFunction):
    def __init__(self) -> None:
        pyceres.CostFunction.__init__(self)
        self.set_num_residuals(1)
        self.set_parameter_block_sizes([1])

    def Evaluate(self, parameters, residuals, jacobians):
        x = float(parameters[0][0])
        residuals[0] = 10.0 - x
        if jacobians is not None and jacobians[0] is not None:
            jacobians[0][0] = -1.0
        return True


def main() -> int:
    ver = getattr(pyceres, "__version__", "?")
    print("pyceres:", getattr(pyceres, "__file__", "?"))
    print("version:", ver)

    x = np.array([5.0])
    x0 = x.copy()
    prob = pyceres.Problem()
    prob.add_residual_block(HelloworldCostFunction(), None, [x])

    options = pyceres.SolverOptions()
    options.linear_solver_type = pyceres.LinearSolverType.DENSE_QR
    options.minimizer_progress_to_stdout = False
    options.num_threads = 1
    summary = pyceres.SolverSummary()
    pyceres.solve(options, prob, summary)

    print("brief:", summary.BriefReport())
    print(f"x: {x0[0]} -> {x[0]}")
    if not summary.IsSolutionUsable():
        print("FAIL: solution not usable", file=sys.stderr)
        return 1
    if abs(float(x[0]) - 10.0) > 1e-5:
        print("FAIL: expected x ~= 10", file=sys.stderr)
        return 1
    print("OK: helloworld cost solved")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
