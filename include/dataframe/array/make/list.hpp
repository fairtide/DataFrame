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

#ifndef DATAFRAME_ARRAY_MAKE_LIST_HPP
#define DATAFRAME_ARRAY_MAKE_LIST_HPP

#include <dataframe/array/make/primitive.hpp>
#include <arrow/util/concatenate.h>

namespace dataframe {

template <typename T>
struct ArrayMaker<List<T>> {
    template <typename Iter>
    static std::shared_ptr<::arrow::Array> make(Iter first, Iter last)
    {
        auto length = static_cast<std::int64_t>(std::distance(first, last));

        std::unique_ptr<::arrow::Buffer> offsets;

        DF_ARROW_ERROR_HANDLER(
            ::arrow::AllocateBuffer(::arrow::default_memory_pool(),
                (length + 1) * static_cast<std::int64_t>(sizeof(std::int32_t)),
                &offsets));

        auto p = reinterpret_cast<std::int32_t *>(
            dynamic_cast<::arrow::MutableBuffer &>(*offsets).mutable_data());

        p[0] = 0;
        auto iter = first;
        ::arrow::ArrayVector value_list;
        value_list.reserve(static_cast<std::size_t>(length));
        for (std::int64_t i = 0; i != length; ++i, ++iter) {
            auto begin = std::begin(*iter);
            auto end = std::end(*iter);
            auto count = static_cast<std::int32_t>(std::distance(begin, end));
            p[i + 1] = p[i] + count;
            if (count > 0) {
                value_list.push_back(make_array<T>(begin, end));
            }
        }

        std::shared_ptr<::arrow::Array> values;
        DF_ARROW_ERROR_HANDLER(::arrow::Concatenate(
            value_list, ::arrow::default_memory_pool(), &values));

        auto type = ::arrow::list(values->type());

        return std::make_shared<::arrow::ListArray>(
            std::move(type), length, std::move(offsets), std::move(values));
    }
};

} // namespace dataframe

#endif // DATAFRAME_ARRAY_MAKE_STRUCT_HPP
