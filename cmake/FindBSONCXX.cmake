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

if (NOT DEFINED BSONCXX_ROOT_DIR)
    set(BSONCXX_ROOT_DIR /usr/local CACHE PATH "Path to BSON")
endif (NOT DEFINED BSONCXX_ROOT_DIR)

find_package(Snappy)
find_package(Threads)

find_package(libbson-static-1.0)
find_package(libbson-1.0)

if (BSON_STATIC_LIBRARIES)
    set(BSON_LINK_LIBRARIES ${BSON_STATIC_LIBRARIES})
elseif (BSON_LIBRARIES)
    set(BSON_LINK_LIBRARIES ${BSON_LIBRARIES})
else (BSON_STATIC_LIBRARIES)
    message("BSON not found")
endif (BSON_STATIC_LIBRARIES)

if (NOT APPLE)
    find_library(RT_LINK_LIBRARIES rt)
endif (NOT APPLE)

find_path(BSONCXX_INCLUDE_DIR bsoncxx/json.hpp
    PATHS ${BSONCXX_ROOT_DIR}/include/bsoncxx/v_noabi NO_DEFAULT_PATH)

find_library(BSONCXX_LIBRARIES bsoncxx-static
    PATHS ${BSONCXX_ROOT_DIR}/lib NO_DEFAULT_PATH)

find_library(BSONCXX_LIBRARIES bsoncxx-static
    PATHS ${BSONCXX_ROOT_DIR}/lib64 NO_DEFAULT_PATH)

find_library(BSONCXX_LIBRARIES bsoncxx
    PATHS ${BSONCXX_ROOT_DIR}/lib NO_DEFAULT_PATH)

find_library(BSONCXX_LIBRARIES bsoncxx
    PATHS ${BSONCXX_ROOT_DIR}/lib64 NO_DEFAULT_PATH)

if (BSONCXX_INCLUDE_DIR)
    message(STATUS "FOUND BSONCXX headers: ${BSONCXX_INCLUDE_DIR}")
else (BSONCXX_INCLUDE_DIR)
    message(STATUS "NOT FOUND BSONCXX headers")
endif (BSONCXX_INCLUDE_DIR)

if (BSON_LINK_LIBRARIES AND BSONCXX_LIBRARIES)
    unset(BSONCXX_LINK_LIBRARIES CACHE)
    if (APPLE)
        set(BSONCXX_LINK_LIBRARIES
            ${BSONCXX_LIBRARIES}
            ${BSON_LINK_LIBRARIES}
            ${CMAKE_THREAD_LIBS_INIT}
            Snappy::snappy
            CACHE STRING "BSONCXX libraries")
    else (APPLE)
        set(BSONCXX_LINK_LIBRARIES
            ${BSONCXX_LIBRARIES}
            ${BSON_LINK_LIBRARIES}
            ${RT_LINK_LIBRARIES}
            ${CMAKE_THREAD_LIBS_INIT}
            Snappy::snappy
            CACHE STRING "BSONCXX libraries")
    endif (APPLE)
    message(STATUS "FOUND BSONCXX libraries: ${BSONCXX_LINK_LIBRARIES}")
else (BSON_LINK_LIBRARIES AND BSONCXX_LIBRARIES)
    message(STATUS "NOT FOUND BSONCXX libraries")
endif (BSON_LINK_LIBRARIES AND BSONCXX_LIBRARIES)

if (BSONCXX_INCLUDE_DIR AND DEFINED BSONCXX_LINK_LIBRARIES)
    set(BSONCXX_FOUND TRUE CACHE BOOL "BSONCXX FOUND")
else (BSONCXX_INCLUDE_DIR AND DEFINED BSONCXX_LINK_LIBRARIES)
    set(BSONCXX_FOUND FALSE CACHE BOOL "BSONCXX NOT FOUND")
endif (BSONCXX_INCLUDE_DIR AND DEFINED BSONCXX_LINK_LIBRARIES)
