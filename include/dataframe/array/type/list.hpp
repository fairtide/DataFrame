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

#ifndef DATAFRAME_ARRAY_TYPE_LIST_HPP
#define DATAFRAME_ARRAY_TYPE_LIST_HPP

#include <dataframe/array/type/primitive.hpp>

namespace dataframe {

template <typename T>
struct List {
};

template <typename T>
struct TypeTraits<List<T>> {
    using scalar_type = std::vector<ScalarType<T>>;
    using data_type = ::arrow::ListType;
    using array_type = ::arrow::ListArray;
    using builder_type = ::arrow::ListBuilder;

    static std::shared_ptr<data_type> make_data_type()
    {
        return std::make_shared<data_type>(::dataframe::make_data_type<T>());
    }

    static std::unique_ptr<builder_type> make_builder(
        ::arrow::MemoryPool *pool = ::arrow::default_memory_pool())
    {
        return std::make_unique<::arrow::ListBuilder>(
            pool, ::dataframe::make_builder<T>(), make_data_type());
    }
};

template <typename T>
struct IsType<List<T>, ::arrow::ListType> {
    static bool is_type(const ::arrow::ListType &type)
    {
        return ::dataframe::is_type<T>(type.value_type());
    }
};

} // namespace dataframe

#endif // DATAFRAME_ARRAY_TYPE_LIST_HPP
