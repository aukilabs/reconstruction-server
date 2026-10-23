set(URL https://github.com/strasdat/Sophus.git)
set(VERSION d5ccee836c41628ada3fa36cc2fd606a11293ab6)

set(sophus_INCLUDE_DIR ${CMAKE_CURRENT_BINARY_DIR}/3rd_party/src/sophus_external)

if (NOT EXISTS ${sophus_INCLUDE_DIR}/sophus)
    externalproject_add(sophus_external
            GIT_REPOSITORY ${URL}
            GIT_TAG        ${VERSION}
            CMAKE_ARGS
                -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
                -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
                -DCMAKE_BUILD_TYPE=Release
            CONFIGURE_COMMAND ""
            BUILD_COMMAND ""
            INSTALL_COMMAND ""
            TEST_COMMAND ""
            PREFIX 3rd_party
            EXCLUDE_FROM_ALL 1
            )
    file(MAKE_DIRECTORY ${sophus_INCLUDE_DIR})
endif()

add_library(sophus INTERFACE IMPORTED GLOBAL)
add_dependencies(sophus sophus_external)

target_include_directories(sophus INTERFACE ${sophus_INCLUDE_DIR})
