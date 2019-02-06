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

project(DataFrame-tests CXX)

add_subdirectory(${PROJECT_SOURCE_DIR}/googletest EXCLUDE_FROM_ALL)

include_directories(SYSTEM ${PROJECT_SOURCE_DIR}/googletest/googletest/include)
include_directories(SYSTEM ${PROJECT_SOURCE_DIR}/googletest/googlemock/include)

target_compile_options(gtest PRIVATE "-w")
target_compile_options(gtest_main PRIVATE "-w")
target_compile_options(gmock PRIVATE "-w")
target_compile_options(gmock_main PRIVATE "-w")

set(test_flags)
if ("${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang")
    list(APPEND test_flags
        "-Wno-global-constructors"
        "-Wno-zero-as-null-pointer-constant")
endif ("${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang")

foreach (flag ${test_flags})
    target_compile_options(gtest PUBLIC ${flag})
    target_compile_options(gtest_main PUBLIC ${flag})
    target_compile_options(gmock PUBLIC ${flag})
    target_compile_options(gmock_main PUBLIC ${flag})
endforeach (flag ${test_flags})

function(add_dataframe_test testname)
    add_executable(${testname}-test ${PROJECT_SOURCE_DIR}/src/${testname}.cpp)

    set_target_properties(${testname}-test
        PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}
        OUTPUT_NAME ${testname})

    target_link_libraries(${testname}-test gtest gtest_main ${link_libraries} ${ARGN})
    add_dependencies(tests ${testname}-test)
    add_test(NAME ${testname} COMMAND ${testname}-test)
endfunction(add_dataframe_test src)

add_dataframe_test(dataframe)
add_dataframe_test(serializer)
add_dataframe_test(sort)
