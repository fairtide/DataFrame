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

if (NOT DEFINED Arrow_ROOT_DIR)
    set(Arrow_ROOT_DIR /usr/local CACHE PATH "Path to Arrow")
endif (NOT DEFINED Arrow_ROOT_DIR)

find_path(Arrow_INCLUDE_DIR arrow/api.h PATHS ${Arrow_ROOT_DIR}/include NO_DEFAULT_PATH)
find_library(Arrow_LIBRARIES arrow PATHS ${Arrow_ROOT_DIR}/lib64 NO_DEFAULT_PATH)
find_library(Arrow_LIBRARIES arrow PATHS ${Arrow_ROOT_DIR}/lib NO_DEFAULT_PATH)
find_library(Arrow_LIBRARIES libarrow.a PATHS ${Arrow_ROOT_DIR}/lib64 NO_DEFAULT_PATH)
find_library(Arrow_LIBRARIES libarrow.a PATHS ${Arrow_ROOT_DIR}/lib NO_DEFAULT_PATH)

find_package(Boost COMPONENTS system filesystem)
find_package(double-conversion)

if (Arrow_INCLUDE_DIR)
    message(STATUS "FOUND Arrow headers: ${Arrow_INCLUDE_DIR}")
else (Arrow_INCLUDE_DIR)
    message(STATUS "NOT FOUND Arrow headers")
endif (Arrow_INCLUDE_DIR)

if (Arrow_LIBRARIES)
    unset(Arrow_LINK_LIBRARIES CACHE)
    set(Arrow_LINK_LIBRARIES
        ${Arrow_LIBRARIES}
        Boost::system
        Boost::filesystem
        double-conversion::double-conversion
        CACHE STRING "Arrow libraries")
    message(STATUS "FOUND Arrow libraries: ${Arrow_LINK_LIBRARIES}")
else (Arrow_LIBRARIES)
    message(STATUS "NOT FOUND Arrow libraries")
endif (Arrow_LIBRARIES)

if (Arrow_INCLUDE_DIR AND DEFINED Arrow_LINK_LIBRARIES)
    set(Arrow_FOUND TRUE CACHE BOOL "Arrow FOUND")
else (Arrow_INCLUDE_DIR AND DEFINED Arrow_LINK_LIBRARIES)
    set(Arrow_FOUND FALSE CACHE BOOL "Arrow NOT FOUND")
endif (Arrow_INCLUDE_DIR AND DEFINED Arrow_LINK_LIBRARIES)
