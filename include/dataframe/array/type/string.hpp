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

#ifndef DATAFRAME_ARRAY_TYPE_STRING_HPP
#define DATAFRAME_ARRAY_TYPE_STRING_HPP

#include <dataframe/array/type/primitive.hpp>

namespace dataframe {

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
        using array_type = ::arrow::Arrow##Array;                             \
    };

DF_DEFINE_TYPE_TRAITS(std::string, String)
DF_DEFINE_TYPE_TRAITS(std::string_view, String)

#undef DF_DEFINE_TYPE_TRAITS

} // namespace dataframe

#endif // DATAFRAME_ARRAY_TYPE_STRING_HPP
