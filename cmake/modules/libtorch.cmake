# LibTorch discovery for MrNeRF / Light_Glue_CPP (S2 wiring).
# Existing pybind modules do not link Torch; this file only prepares find_package(Torch).

if(DEFINED LIBTORCH_ROOT)
    set(LIBTORCH_ROOT "${LIBTORCH_ROOT}" CACHE PATH "LibTorch install prefix")
elseif(DEFINED ENV{LIBTORCH_ROOT})
    set(LIBTORCH_ROOT "$ENV{LIBTORCH_ROOT}" CACHE PATH "LibTorch install prefix")
elseif(EXISTS "/opt/libtorch/share/cmake/Torch/TorchConfig.cmake")
    set(LIBTORCH_ROOT "/opt/libtorch" CACHE PATH "LibTorch install prefix")
endif()

if(LIBTORCH_ROOT)
    list(PREPEND CMAKE_PREFIX_PATH "${LIBTORCH_ROOT}")
    find_package(Torch REQUIRED)
    message(STATUS "LibTorch: ${TORCH_INSTALL_PREFIX} (Torch ${Torch_VERSION})")
    set(AUKI_LIBTORCH_FOUND TRUE)
else()
    message(
        STATUS
        "LIBTORCH_ROOT unset; skipping Torch discovery (MrNeRF pybinds not buildable yet)")
endif()

set(LIGHT_GLUE_CPP_DIR
    "${CMAKE_SOURCE_DIR}/third_party/Light_Glue_CPP"
    CACHE PATH "MrNeRF Light_Glue_CPP submodule root")

if(EXISTS "${LIGHT_GLUE_CPP_DIR}/CMakeLists.txt")
    message(STATUS "Light_Glue_CPP tree: ${LIGHT_GLUE_CPP_DIR}")
else()
    message(
        WARNING
        "Light_Glue_CPP not found at ${LIGHT_GLUE_CPP_DIR} (run git submodule update --init)")
endif()
