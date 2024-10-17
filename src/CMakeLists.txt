cmake_minimum_required(VERSION 3.21)
include(ExternalProject)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(COMMAND cmake_policy)
    cmake_policy(SET CMP0003 NEW)
    cmake_policy(SET CMP0057 NEW)
endif(COMMAND cmake_policy)

find_package(Ceres REQUIRED)
find_package (Eigen3 3.3 REQUIRED NO_MODULE)
find_package(colmap REQUIRED)
find_package(pybind11 CONFIG)
include(cmake/modules/sophus.cmake)

pybind11_add_module(
    cost_functions
    cost_functions_binds.cpp)

target_link_libraries(cost_functions PUBLIC Eigen3::Eigen Ceres::ceres sophus)
set_target_properties(cost_functions PROPERTIES LIBRARY_OUTPUT_DIRECTORY
    ${PROJECT_SOURCE_DIR}/src)

pybind11_add_module(
    ply_export
    ply_export.cpp)

target_link_libraries(ply_export PUBLIC colmap::colmap)
set_target_properties(ply_export PROPERTIES LIBRARY_OUTPUT_DIRECTORY
    ${PROJECT_SOURCE_DIR}/src)