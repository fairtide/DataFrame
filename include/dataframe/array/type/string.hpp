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

template <>
struct TypeTraits<std::string> {
    static std::shared_ptr<::arrow::StringType> data_type()
    {
        return std::make_shared<::arrow::StringType>();
    }

    static std::unique_ptr<::arrow::StringBuilder> builder()
    {
        return std::make_unique<::arrow::StringBuilder>(
            ::arrow::default_memory_pool());
    }

    using ctype = std::string;
    using array_type = ::arrow::StringArray;
};

template <>
struct TypeTraits<std::string_view> : TypeTraits<std::string> {
};

} // namespace dataframe

#endif // DATAFRAME_ARRAY_TYPE_STRING_HPP
