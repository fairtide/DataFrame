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

template <typename Valid>
inline std::int64_t make_null_bitmap(
    std::int64_t length, Valid valid, std::shared_ptr<::arrow::Buffer> *out)
{
    DF_ARROW_ERROR_HANDLER(
        ::arrow::AllocateBuffer(::arrow::default_memory_pool(),
            ::arrow::BitUtil::BytesForBits(length), out));

    auto bits =
        dynamic_cast<::arrow::MutableBuffer &>(*out->get()).mutable_data();

    std::int64_t null_count = 0;
    for (std::int64_t i = 0; i != length; ++i, ++valid) {
        auto is_valid = *valid;
        null_count += !is_valid;
        ::arrow::BitUtil::SetBitTo(bits, i, is_valid);
    }

    return null_count;
}

template <typename T, typename Iter>
inline std::shared_ptr<::arrow::Array> make_array(Iter first, Iter last)
{
    return ArrayMaker<T>::make(first, last);
}

template <typename T, typename Iter, typename Valid>
inline std::shared_ptr<::arrow::Array> make_array(
    Iter first, Iter last, Valid valid)
{
    auto data = make_array<T>(first, last)->data()->Copy();

    data->null_count =
        make_null_bitmap(data->length, valid, &data->buffers.at(0));

    return ::arrow::MakeArray(data);
}

} // namespace dataframe

#endif // DATAFRAME_ARRAY_MAKE_PRIMITIVE_HPP
