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

#ifndef DATAFRAME_ARRAY_BOOL_HPP
#define DATAFRAME_ARRAY_BOOL_HPP

#include <dataframe/array/make_array.hpp>
#include <dataframe/array/mask.hpp>
#include <dataframe/error.hpp>

namespace dataframe {

template <typename Alloc>
inline std::shared_ptr<::arrow::Array> make_array(
    const std::vector<bool, Alloc> &values)
{
    ::arrow::BooleanBuilder builder(::arrow::default_memory_pool());

    DF_ARROW_ERROR_HANDLER(
        builder.Reserve(static_cast<std::int64_t>(values.size())));

    for (auto v : values) {
        DF_ARROW_ERROR_HANDLER(builder.Append(v));
    }

    std::shared_ptr<::arrow::Array> ret;
    DF_ARROW_ERROR_HANDLER(builder.Finish(&ret));

    return ret;
}

inline std::shared_ptr<::arrow::Array> make_array(const ArrayMask &values)
{
    ::arrow::BooleanBuilder builder(::arrow::default_memory_pool());

    DF_ARROW_ERROR_HANDLER(
        builder.Reserve(static_cast<std::int64_t>(values.size())));

    for (std::size_t i = 0; i != values.size(); ++i) {
        DF_ARROW_ERROR_HANDLER(builder.Append(values[i]));
    }

    std::shared_ptr<::arrow::Array> ret;
    DF_ARROW_ERROR_HANDLER(builder.Finish(&ret));

    return ret;
}

} // namespace dataframe

#endif // DATAFRAME_ARRAY_BOOL_HPP
