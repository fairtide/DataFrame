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

set(CMAKE_CXX_STANDARD 17)

find_package(Arrow)
find_package(RapidJSON)
find_package(Boost COMPONENTS date_time)

include_directories(SYSTEM ${Arrow_INCLUDE_DIR})
include_directories(SYSTEM ${Boost_INCLUDE_DIR})
include_directories(SYSTEM ${RAPIDJSON_INCLUDE_DIRS})
set(link_libraries ${Arrow_LINK_LIBRARIES} Boost::date_time)

if ("${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang")
    add_compile_options(-Weverything)
    add_compile_options(-Werror)
    add_compile_options(-fcolor-diagnostics)
    add_compile_options(-Wno-c++98-compat)
    add_compile_options(-Wno-c++98-compat-pedantic)
    add_compile_options(-Wno-c++11-compat)
    add_compile_options(-Wno-c++11-compat-pedantic)
    add_compile_options(-Wno-c++14-compat)
    add_compile_options(-Wno-c++14-compat-pedantic)
    add_compile_options(-Wno-documentation)
    add_compile_options(-Wno-documentation-unknown-command)
    add_compile_options(-Wno-exit-time-destructors)
    add_compile_options(-Wno-float-equal)
    add_compile_options(-Wno-padded)
    add_compile_options(-Wno-return-std-move-in-c++11)
    add_compile_options(-Wno-unknown-warning-option)
    add_compile_options(-Wno-weak-vtables)
endif ("${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang")
