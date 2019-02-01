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

##############################################################################
# Disable in source build
##############################################################################

if(${CMAKE_SOURCE_DIR} STREQUAL ${CMAKE_BINARY_DIR})
    message(FATAL_ERROR "In-source builds not allowed")
endif(${CMAKE_SOURCE_DIR} STREQUAL ${CMAKE_BINARY_DIR})

##############################################################################
# Basics
##############################################################################

project(DataFrame C CXX)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${PROJECT_SOURCE_DIR}/cmake)

##############################################################################
# Options
##############################################################################

option(DataFrame_ENABLE_TESTS "Enable building of example" ON)

##############################################################################
# Installation
##############################################################################

if(NOT DEFINED DataFrame_INSTALL_INC_DIR)
    SET(DataFrame_INSTALL_INC_DIR include)
endif(NOT DEFINED DataFrame_INSTALL_INC_DIR)
install(DIRECTORY ${PROJECT_SOURCE_DIR}/include/dataframe
    DESTINATION ${DataFrame_INSTALL_INC_DIR}
    PATTERN ".DS_Store" EXCLUDE PATTERN "*.swp" EXCLUDE)

##############################################################################
# Check features
##############################################################################

if(DataFrame_ENABLE_TESTS)
    include(DataFrame)
else(DataFrame_ENABLE_TESTS)
    return()
endif(DataFrame_ENABLE_TESTS)

##############################################################################
# Tests
##############################################################################

if(DataFrame_ENABLE_TESTS)
    add_custom_target(tests)
    add_subdirectory(tests EXCLUDE_FROM_ALL)
endif(DataFrame_ENABLE_TESTS)
