cmake_minimum_required(VERSION 3.26)

if (POLICY CMP0048)
  cmake_policy(SET CMP0048 NEW)
endif (POLICY CMP0048)

project(auki_solver VERSION 1.0.0)

include(cmake/modules/libtorch.cmake)
include(cmake/modules/light_glue_cpp.cmake)

add_subdirectory(src)
