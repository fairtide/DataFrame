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

#ifndef DATAFRAME_ARRAY_MAKE_PRIMITIVE_HPP
#define DATAFRAME_ARRAY_MAKE_PRIMITIVE_HPP

#include <dataframe/array/type.hpp>

namespace dataframe {

template <typename T>
struct ArrayMaker {
    template <typename Iter>
    static std::shared_ptr<::arrow::Array> make(Iter first, Iter last)
    {
        auto builder = make_builder<T>();
        DF_ARROW_ERROR_HANDLER(builder->AppendValues(first, last));

        std::shared_ptr<::arrow::Array> ret;
        DF_ARROW_ERROR_HANDLER(builder->Finish(&ret));

        return ret;
    }
};

template <typename T, typename Iter>
std::shared_ptr<::arrow::Array> make_array(Iter first, Iter last)
{
    return ArrayMaker<T>::make(first, last);
}

template <typename T, typename Iter, typename ValidIter>
std::shared_ptr<::arrow::Array> make_array(
    Iter first, Iter last, ValidIter valid)
{
    auto array = make_array<T>(first, last);
    auto data = array->data()->Copy();
    auto length = data->length;
    auto bytes = ::arrow::BitUtil::BytesForBits(length);

    std::unique_ptr<::arrow::Buffer> nulls;
    DF_ARROW_ERROR_HANDLER(::arrow::AllocateBuffer(
        ::arrow::default_memory_pool(), bytes, &nulls));

    auto bits = dynamic_cast<::arrow::MutableBuffer &>(*nulls).mutable_data();
    std::int64_t null_count = 0;
    for (std::int64_t i = 0; i != length; ++i) {
        auto v = static_cast<bool>(*valid++);
        null_count += !v;
        ::arrow::BitUtil::SetBitTo(bits, i, v);
    }

    if (data->buffers.empty()) {
        data->buffers.emplace_back(std::move(nulls));
    } else {
        data->buffers[0] = std::move(nulls);
    }
    data->null_count = null_count;

    return ::arrow::MakeArray(data);
}

} // namespace dataframe

#endif // DATAFRAME_ARRAY_MAKE_PRIMITIVE_HPP
