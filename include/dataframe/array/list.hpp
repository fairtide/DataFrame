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

#ifndef DATAFRAME_ARRAY_LIST_HPP
#define DATAFRAME_ARRAY_LIST_HPP

#include <dataframe/array/make_array.hpp>

namespace dataframe {

template <typename DataView>
struct ListView {
    ArrayView<std::int32_t> offsets;
    DataView values;
};

template <typename DataView>
inline std::shared_ptr<::arrow::Array> make_array(
    const ListView<DataView> &view)
{
    std::shared_ptr<::arrow::Array> ret;
    DF_ARROW_ERROR_HANDLER(
        ::arrow::ListArray::FromArrays(*make_array(view.offsets),
            *make_array(view.values), ::arrow::default_memory_pool(), &ret));

    return ret;
}

template <typename DataView>
inline void cast_array(const ::arrow::Array &array, ListView<DataView> *out)
{
    auto &list_array = dynamic_cast<const ::arrow::ListArray &>(array);

    out->offsets = ArrayView<std::int32_t>(
        static_cast<std::size_t>(list_array.length() + 1),
        list_array.raw_value_offsets());

    cast_array(*list_array.values(), &out->values);
}

} // namespace dataframe

#endif // DATAFRAME_ARRAY_LIST_HPP
