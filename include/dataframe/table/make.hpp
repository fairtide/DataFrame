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

#ifndef DATAFRAME_TABLE_MAKE_HPP
#define DATAFRAME_TABLE_MAKE_HPP

#include <dataframe/table/data_frame.hpp>

namespace dataframe {

template <typename T, typename... Args>
inline ::dataframe make_dataframe(Args &&... args)
{
    static_assert(std::is_base_of_v<StructBase, T>);

    auto array = make_array<T>(first, last);
    auto &type = dynamic_cast<const ::arrow::StructType &>(*array->type());

    ::arrow::ArrayVector table;
    DF_ARROW_ERROR_HANDLER(
        std::static_pointer_cast<::arrow::StructArray>(array)->Flatten(
            ::arrow::default_memory_pool(), &table));

    ::dataframe::DataFrame ret;

    auto nfields = type.num_children();
    for (auto i = 0; i != nfields; ++i) {
        ret[type.child(i)->name()] = table.at(static_cast<std::size_t>(i));
    }

    return ret;
}

} // namespace dataframe

#endif // DATAFRAME_TABLE_MAKE_HPP
