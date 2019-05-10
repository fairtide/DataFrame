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

include_directories(SYSTEM ${PROJECT_SOURCE_DIR}/Catch2/single_include)

add_definitions(-DCATCH_CONFIG_MAIN)
add_definitions(-DRAPIDJSON_HAS_STDSTRING)

function(add_dataframe_test testname)
    add_executable(${testname}-test ${PROJECT_SOURCE_DIR}/src/${testname}.cpp)

    set_target_properties(${testname}-test
        PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}
        OUTPUT_NAME ${testname})

    target_link_libraries(${testname}-test ${Arrow_LINK_LIBRARIES} ${ARGN})
    add_dependencies(tests ${testname}-test)
    add_test(NAME ${testname} COMMAND ${testname}-test)
endfunction(add_dataframe_test src)

add_subdirectory(array)
add_subdirectory(table)
add_subdirectory(serializer)

# find_program(Rscript_EXECUTABLE Rscript)

# if (Rscript_EXECUTABLE)
#     execute_process(COMMAND ${Rscript_EXECUTABLE} -e
#         "cat(R.home())" OUTPUT_VARIABLE R_HOME)
#     message(STATUS "R_HOME ${R_HOME}")
#
#     execute_process(COMMAND ${Rscript_EXECUTABLE} -e
#         "x <- installed.packages(); cat(x[x[,\"Package\"] == \"Rcpp\", \"LibPath\"])"
#         OUTPUT_VARIABLE Rcpp_ROOT)
#     message(STATUS "Rcpp_ROOT ${Rcpp_ROOT}")
#
#     execute_process(COMMAND ${Rscript_EXECUTABLE} -e
#         "x <- installed.packages(); cat(x[x[,\"Package\"] == \"RInside\", \"LibPath\"])"
#         OUTPUT_VARIABLE RInside_ROOT)
#     message(STATUS "RInside_ROOT ${RInside_ROOT}")
#
#     if (EXISTS ${R_HOME} AND EXISTS ${Rcpp_ROOT} AND EXISTS ${RInside_ROOT})
#         include_directories(SYSTEM ${R_HOME}/include)
#         include_directories(SYSTEM ${Rcpp_ROOT}/Rcpp/include)
#         include_directories(SYSTEM ${RInside_ROOT}/RInside/include)
#
#         find_library(R_LIB R PATHS ${R_HOME}/lib NO_DEFAULT_PATH)
#         message(STATUS "R_LIB ${R_LIB}")
#
#         find_library(RInside_LIB RInside PATHS ${RInside_ROOT}/RInside/lib NO_DEFAULT_PATH)
#         message(STATUS "RInside_LIB ${RInside_LIB}")
#
#         if (R_LIB AND RInside_LIB)
#             add_dataframe_test(rcpp ${R_LIB} ${RInside_LIB})
#         endif (R_LIB AND RInside_LIB)
#     endif (EXISTS ${R_HOME} AND EXISTS ${Rcpp_ROOT} AND EXISTS ${RInside_ROOT})
# endif (Rscript_EXECUTABLE)
