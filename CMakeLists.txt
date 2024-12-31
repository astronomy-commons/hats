cmake_minimum_required(VERSION 3.15...3.26)
project(${SKBUILD_PROJECT_NAME} LANGUAGES CXX)

set(PYBIND11_NEWPYTHON ON)
find_package(pybind11 CONFIG REQUIRED)
set(TARGET pixel_math_ext)

pybind11_add_module(${TARGET} src/cpp/pixel_math.cpp )

target_include_directories(${TARGET} PUBLIC src/cpp)
install(TARGETS ${TARGET} LIBRARY DESTINATION ${SKBUILD_PROJECT_NAME})