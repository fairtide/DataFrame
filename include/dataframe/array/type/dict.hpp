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
#include <unordered_map>

namespace dataframe {

struct DictBase {
};

template <typename T>
struct Dict final : DictBase {
    using data_type = T;
};

template <typename T>
struct TypeTraits<Dict<T>> {
    [[noreturn]] static std::shared_ptr<::arrow::DictionaryType> data_type()
    {
        throw DataFrameException("Dict::data_type shall never be called");
    }

    static std::unique_ptr<::arrow::DictionaryBuilder<DataType<T>>> builder()
    {
        return std::make_unique<::arrow::DictionaryBuilder<DataType<T>>>(
            make_data_type<T>(), ::arrow::default_memory_pool());
    }

    using ctype = std::vector<CType<T>>;
    using array_type = ::arrow::DictionaryArray;
};

} // namespace dataframe

#endif // DATAFRAME_ARRAY_TYPE_DICT_HPP
