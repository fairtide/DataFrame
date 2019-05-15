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

#ifndef DATAFRAME_ARRAY_TYPE_DICT_HPP
#define DATAFRAME_ARRAY_TYPE_DICT_HPP

#include <dataframe/array/type/primitive.hpp>

namespace dataframe {

template <typename T>
struct Dict {
};

template <typename T>
struct TypeTraits<Dict<T>> {
    using scalar_type = ScalarType<T>;
    using data_type = ::arrow::DictionaryType;
    using array_type = ::arrow::DictionaryArray;
    using builder_type = ::arrow::DictionaryBuilder<DataType<T>>;

    [[noreturn]] static std::shared_ptr<data_type> make_data_type()
    {
        throw DataFrameException("Dict::data_type shall never be called");
    }

    static std::unique_ptr<builder_type> make_builder(
        ::arrow::MemoryPool *pool = ::arrow::default_memory_pool())
    {
        return std::make_unique<builder_type>(
            ::dataframe::make_data_type<T>(), pool);
    }
};

template <typename T>
struct IsType<Dict<T>, ::arrow::DictionaryType> {
    static bool is_type(const ::arrow::DictionaryType &type)
    {
        return ::dataframe::is_type<std::int32_t>(type.index_type()) &&
            ::dataframe::is_type<T>(type.dictionary()->type());
    }
};

} // namespace dataframe

#endif // DATAFRAME_ARRAY_TYPE_DICT_HPP
