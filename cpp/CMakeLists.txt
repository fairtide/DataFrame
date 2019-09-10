# ============================================================================
# Copyright 2019 Fairtide Pte. Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ============================================================================

cmake_minimum_required(VERSION 3.0.0)

project(DataFrame LANGUAGES CXX VERSION 0.1.5)

if (${CMAKE_SOURCE_DIR} STREQUAL ${CMAKE_BINARY_DIR})
    message(FATAL_ERROR "In-source builds not allowed")
endif (${CMAKE_SOURCE_DIR} STREQUAL ${CMAKE_BINARY_DIR})

option(DataFrame_ENABLE_TESTS "Enable building of tests" ON)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${PROJECT_SOURCE_DIR}/cmake)

include_directories(${PROJECT_SOURCE_DIR}/include)

if (NOT DEFINED DataFrame_INSTALL_INC_DIR)
    SET(DataFrame_INSTALL_INC_DIR include)
endif (NOT DEFINED DataFrame_INSTALL_INC_DIR)

include(CMakePackageConfigHelpers)
set(prefix ${CMAKE_INSTALL_PREFIX}/lib/cmake/DataFrame)

configure_package_config_file(
    ${PROJECT_SOURCE_DIR}/DataFrameConfig.cmake.in
    ${PROJECT_BINARY_DIR}/DataFrameConfig.cmake
    INSTALL_DESTINATION ${prefix} PATH_VARS DataFrame_INSTALL_INC_DIR
    INSTALL_PREFIX ${DataFrame_INSTALL_PREFIX})

write_basic_package_version_file(
    ${PROJECT_BINARY_DIR}/DataFrameConfigVersion.cmake
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion)

install(FILES
    ${PROJECT_BINARY_DIR}/DataFrameConfig.cmake
    ${PROJECT_BINARY_DIR}/DataFrameConfigVersion.cmake
    DESTINATION ${prefix})

install(DIRECTORY ${PROJECT_SOURCE_DIR}/include/dataframe
    DESTINATION ${DataFrame_INSTALL_INC_DIR}
    PATTERN ".DS_Store" EXCLUDE PATTERN "*.swp" EXCLUDE)

if (DataFrame_ENABLE_TESTS)
    enable_testing()
    include(DataFrame)
    add_custom_target(tests)
    add_subdirectory(tests)
endif (DataFrame_ENABLE_TESTS)
