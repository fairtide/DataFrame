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

#include <dataframe/array/cast.hpp>
#include <dataframe/array/type.hpp>

namespace dataframe {

template <typename T>
struct ArrayMaker {
    template <typename Iter>
    static void append(BuilderType<T> *builder, Iter first, Iter last)
    {
        DF_ARROW_ERROR_HANDLER(builder->AppendValues(first, last));
    }
};

template <bool Cond>
using EnableMakeArray =
    std::enable_if_t<Cond, std::shared_ptr<::arrow::Array>>;

template <typename T, typename Iter>
inline EnableMakeArray<
    !std::is_same_v<typename std::iterator_traits<Iter>::value_type, void>>
make_array(Iter first, Iter last,
    ::arrow::MemoryPool *pool = ::arrow::default_memory_pool())
{
    auto builder = make_builder<T>(pool);
    ArrayMaker<T>::append(builder.get(), first, last);
    std::shared_ptr<::arrow::Array> ret;
    DF_ARROW_ERROR_HANDLER(builder->Finish(&ret));

    return cast_array<T>(ret, pool);
}

} // namespace dataframe

#endif // DATAFRAME_ARRAY_MAKE_PRIMITIVE_HPP
