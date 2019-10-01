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

project(DataFrame-python)

find_package(PythonInterp 3.6)

add_custom_target(install-python
    COMMAND ${PYTHON_EXECUTABLE} -m pip
    install ${PROJECT_SOURCE_DIR}
    --prefix ${CMAKE_INSTALL_PREFIX}
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR})

install(CODE "execute_process(COMMAND ${CMAKE_MAKE_PROGRAM} install-python)")
