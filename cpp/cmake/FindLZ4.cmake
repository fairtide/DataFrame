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

if (NOT DEFINED LZ4_ROOT_DIR)
    set(LZ4_ROOT_DIR /usr/local CACHE PATH "Path to LZ4")
endif (NOT DEFINED LZ4_ROOT_DIR)

find_path(LZ4_INCLUDE_DIR lz4.h
    PATHS ${LZ4_ROOT_DIR}/include NO_DEFAULT_PATH)

find_library(LZ4_LIBRARIES liblz4.a
    PATHS ${LZ4_ROOT_DIR}/lib64 ${LZ4_ROOT_DIR}/lib NO_DEFAULT_PATH)

find_library(LZ4_LIBRARIES libliz4.a
    PATHS ${LZ4_ROOT_DIR}/lib ${LZ4_ROOT_DIR}/lib NO_DEFAULT_PATH)

find_library(LZ4_LIBRARIES lz4
    PATHS ${LZ4_ROOT_DIR}/lib64 ${LZ4_ROOT_DIR}/lib NO_DEFAULT_PATH)

find_library(LZ4_LIBRARIES lz4
    PATHS ${LZ4_ROOT_DIR}/lib ${LZ4_ROOT_DIR}/lib NO_DEFAULT_PATH)

if (LZ4_INCLUDE_DIR)
    message(STATUS "FOUND LZ4 headers: ${LZ4_INCLUDE_DIR}")
else (LZ4_INCLUDE_DIR)
    message(STATUS "NOT FOUND LZ4 headers")
endif (LZ4_INCLUDE_DIR)

if (LZ4_LIBRARIES)
    unset(LZ4_LINK_LIBRARIES CACHE)
    set(LZ4_LINK_LIBRARIES ${LZ4_LIBRARIES} CACHE STRING "LZ4 libraries")
    message(STATUS "FOUND LZ4 libraries: ${LZ4_LINK_LIBRARIES}")
else (LZ4_LIBRARIES)
    message(STATUS "NOT FOUND LZ4 libraries")
endif (LZ4_LIBRARIES)

if (LZ4_INCLUDE_DIR AND DEFINED LZ4_LINK_LIBRARIES)
    set(LZ4_FOUND TRUE CACHE BOOL "LZ4 FOUND")
else (LZ4_INCLUDE_DIR AND DEFINED LZ4_LINK_LIBRARIES)
    set(LZ4_FOUND FALSE CACHE BOOL "LZ4 NOT FOUND")
endif (LZ4_INCLUDE_DIR AND DEFINED LZ4_LINK_LIBRARIES)
