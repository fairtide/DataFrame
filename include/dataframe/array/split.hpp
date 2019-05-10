// ============================================================================
// Copyright 2019 Fairtide Pte. Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// ============================================================================

#ifndef DATAFRAME_ARRAY_SPLIT_HPP
#define DATAFRAME_ARRAY_SPLIT_HPP

#include <dataframe/array/type.hpp>

namespace dataframe {

inline std::vector<std::shared_ptr<::arrow::Array>> split_array(
    const ::arrow::Array &array, std::int64_t chunk_size)
{
    if (chunk_size == 0) {
        throw DataFrameException("Invalid chunk size");
    }

    auto offset = INT64_C(0);
    auto length = array.length();

    std::vector<std::shared_ptr<::arrow::Array>> ret;
    ret.reserve(static_cast<std::size_t>(
        length / chunk_size + (length % chunk_size == 0 ? 0 : 1)));

    auto end = std::min(length, offset + chunk_size);
    auto len = end - offset;
    while (offset < length) {
        ret.push_back(array.Slice(offset, len));
        offset = end;
    }

    return ret;
}

inline std::vector<std::shared_ptr<::arrow::Array>> split_array(
    const std::shared_ptr<::arrow::Array> &array, std::int64_t chunk_size)
{
    return split_array(*array, chunk_size);
}

} // namespace dataframe

#endif // DATAFRAME_ARRAY_SPLIT_HPP
