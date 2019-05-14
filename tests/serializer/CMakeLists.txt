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

project(DataFrame-tests-serializer CXX)

add_dataframe_test(bson ${BSONCXX_LINK_LIBRARIES} ${LZ4_LINK_LIBARIES} ${LZ4_LINK_LIBRARIES})
add_dataframe_test(feather)
add_dataframe_test(json Boost::date_time)
add_dataframe_test(record_batch_file)
add_dataframe_test(record_batch_stream)
