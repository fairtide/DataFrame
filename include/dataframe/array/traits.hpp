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

#ifndef DATAFRAME_ARRAY_TRAITS_HPP
#define DATAFRAME_ARRAY_TRAITS_HPP

#include <arrow/api.h>

namespace dataframe {

template <typename T>
struct TypeTraits;

#define DF_DEFINE_TYPE_TRAITS(T, Arrow)                                       \
    template <>                                                               \
    struct TypeTraits<T> {                                                    \
        static std::shared_ptr<::arrow::Arrow##Type> data_type()              \
        {                                                                     \
            return std::make_shared<::arrow::Arrow##Type>();                  \
        }                                                                     \
                                                                              \
        static std::unique_ptr<::arrow::Arrow##Builder> builder()             \
        {                                                                     \
            return std::make_unique<::arrow::Arrow##Builder>(                 \
                ::arrow::default_memory_pool());                              \
        }                                                                     \
                                                                              \
        using ctype = T;                                                      \
        using array_type = ::arrow::Arrow##Array;                             \
    };

DF_DEFINE_TYPE_TRAITS(void, Null)
DF_DEFINE_TYPE_TRAITS(bool, Boolean)
DF_DEFINE_TYPE_TRAITS(std::int8_t, Int8)
DF_DEFINE_TYPE_TRAITS(std::int16_t, Int16)
DF_DEFINE_TYPE_TRAITS(std::int32_t, Int32)
DF_DEFINE_TYPE_TRAITS(std::int64_t, Int64)
DF_DEFINE_TYPE_TRAITS(std::uint8_t, UInt8)
DF_DEFINE_TYPE_TRAITS(std::uint16_t, UInt16)
DF_DEFINE_TYPE_TRAITS(std::uint32_t, UInt32)
DF_DEFINE_TYPE_TRAITS(std::uint64_t, UInt64)
DF_DEFINE_TYPE_TRAITS(float, Float)
DF_DEFINE_TYPE_TRAITS(double, Double)

#undef DF_DEFINE_TYPE_TRAITS

template <typename T>
using ArrayType = typename TypeTraits<T>::array_type;

template <typename T>
using BuilderType = typename TypeTraits<T>::builder_type;

} // namespace dataframe

#endif // DATAFRAME_ARRAY_TRAITS_HPP
