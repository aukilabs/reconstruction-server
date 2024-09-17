cmake_minimum_required(VERSION 3.21)

if (POLICY CMP0048)
  cmake_policy(SET CMP0048 NEW)
endif (POLICY CMP0048)

project(auki_solver VERSION 1.0.0)

add_subdirectory(src)