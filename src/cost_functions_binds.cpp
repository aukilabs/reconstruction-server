#include <string>

#include "cost_functions.h"
#include <pybind11/eigen.h>
#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

using namespace auki;

PYBIND11_MODULE(cost_functions, m) {
  namespace py = pybind11;
  using namespace py::literals;

  py::module pyceres = py::module::import("pyceres");

  m.def("DistanceMovedCost",
        &DistanceMovedCostFunction::Create,
        "arkit_offset_rotated"_a,
        "arkit_offset_moved"_a,
        "arkit_gravity_direction"_a,
        "offset_weight"_a,
        "gravity_weight"_a);

  m.def("RelativeTransformationSE3CostFunction",
        &RelativeTransformationSE3CostFunction::Create,
        "t_target_reference_quat"_a,
        "t_target_reference_translation"_a,
        "t_reference_target_covariance"_a);

  m.def("RelativeTransformationSE3ViaObservationsCostFunction",
        &RelativeTransformationSE3ViaObservationsCostFunction::Create,
        "t_reference_observation_quat"_a,
        "t_reference_observation_translation"_a,
        "t_target_observation_quat"_a,
        "t_target_observation_translation"_a,
        "t_reference_target_covariance"_a);

  m.def("RelativeTransformationSim3CostFunction",
        &RelativeTransformationSim3CostFunction::Create,
        "t_reference_observation_quat"_a,
        "t_reference_observation_translation"_a,
        "t_target_observation_quat"_a,
        "t_target_observation_translation"_a,
        "t_reference_target_covariance"_a);

  m.def("PoseCenterConstraintCostFunction",
        &PoseCenterConstraintCostFunction::Create,
        "center"_a,
        "weight"_a);

  m.def("FloorAlignmentCostFunction",
        &FloorAlignmentCostFunction::Create,
        "detection_rotation"_a,
        "detection_translation"_a,
        "height_weight"_a = 1.0,
        "direction_weight"_a = 1.0);
}