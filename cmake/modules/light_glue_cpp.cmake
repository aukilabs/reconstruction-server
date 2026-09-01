# Build MrNeRF Light_Glue_CPP static library for mrnerf_features pybind (S3).
# Requires AUKI_LIBTORCH_FOUND from libtorch.cmake.

if(NOT AUKI_LIBTORCH_FOUND)
    return()
endif()

if(NOT EXISTS "${LIGHT_GLUE_CPP_DIR}/CMakeLists.txt")
    message(WARNING "Light_Glue_CPP missing at ${LIGHT_GLUE_CPP_DIR}; skipping mrnerf_features")
    return()
endif()

if(NOT CMAKE_CUDA_ARCHITECTURES)
    set(CMAKE_CUDA_ARCHITECTURES 80 86 89 90)
endif()

enable_language(CUDA)

find_package(OpenCV REQUIRED)
find_package(CUDAToolkit REQUIRED)

if(CUDAToolkit_VERSION VERSION_LESS "12.1")
    message(FATAL_ERROR "Light_Glue_CPP requires CUDA >= 12.1 (found ${CUDAToolkit_VERSION})")
endif()

set(LIGHTGLUE_MODELS_DIR "${LIGHT_GLUE_CPP_DIR}/models" CACHE PATH "MrNeRF model weights directory")
add_compile_definitions(LIGHTGLUE_MODELS_DIR="${LIGHTGLUE_MODELS_DIR}")

set(_lg_root "${LIGHT_GLUE_CPP_DIR}")

set(_lg_sources
    ${_lg_root}/src/feature/ALIKED.cpp
    ${_lg_root}/src/feature/DKD.cpp
    ${_lg_root}/src/feature/input_padder.cpp
    ${_lg_root}/src/feature/get_patches.cpp
    ${_lg_root}/src/feature/SDDH.cpp
    ${_lg_root}/src/feature/deform_conv2d.cpp
    ${_lg_root}/src/feature/deform_conv2d_kernel.cu
    ${_lg_root}/src/feature/get_patches_cuda.cu
    ${_lg_root}/src/feature/blocks.cpp
    ${_lg_root}/src/matcher/lightglue/attention.cpp
    ${_lg_root}/src/matcher/lightglue/core.cpp
    ${_lg_root}/src/matcher/lightglue/encoding.cpp
    ${_lg_root}/src/matcher/lightglue/matcher.cpp
    ${_lg_root}/src/matcher/lightglue/transformer.cpp)

add_library(auki_lightglue_lib STATIC ${_lg_sources})

target_include_directories(auki_lightglue_lib
    PUBLIC
    "${_lg_root}/include")

target_link_libraries(auki_lightglue_lib
    PUBLIC
    ${TORCH_LIBRARIES}
    ${OpenCV_LIBS}
    PRIVATE
    CUDA::cudart
    CUDA::curand
    CUDA::cublas)

target_compile_options(auki_lightglue_lib PRIVATE ${TORCH_CXX_FLAGS})
set_target_properties(auki_lightglue_lib PROPERTIES
    CUDA_SEPARABLE_COMPILATION ON
    CUDA_RESOLVE_DEVICE_SYMBOLS ON
    POSITION_INDEPENDENT_CODE ON)

set(AUKI_LIGHTGLUE_AVAILABLE TRUE)
message(STATUS "MrNeRF LightGlue lib: enabled (models ${LIGHTGLUE_MODELS_DIR})")
