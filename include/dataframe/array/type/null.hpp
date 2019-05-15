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

#ifndef DATAFRAME_ARRAY_TYPE_NULL_HPP
#define DATAFRAME_ARRAY_TYPE_NULL_HPP

#include <dataframe/array/type/primitive.hpp>

namespace dataframe {

template <>
struct TypeTraits<void> {
    using scalar_type = void;
    using data_type = ::arrow::NullType;
    using array_type = ::arrow::NullArray;
    using builder_type = ::arrow::NullBuilder;

    static std::shared_ptr<data_type> make_data_type()
    {
        return std::make_shared<data_type>();
    }

    static std::unique_ptr<builder_type> make_builder(
        ::arrow::MemoryPool *pool = ::arrow::default_memory_pool())
    {
        return std::make_unique<builder_type>(pool);
    }
};

template <typename T>
struct IsType<T, ::arrow::NullType> {
    static constexpr bool is_type(const ::arrow::NullType &)
    {
        return std::is_same_v<T, void>;
    }
};

} // namespace dataframe

#endif // DATAFRAME_ARRAY_TYPE_NULL_HPP
