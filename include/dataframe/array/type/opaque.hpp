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

#ifndef DATAFRAME_ARRAY_TYPE_OPAQUE_HPP
#define DATAFRAME_ARRAY_TYPE_OPAQUE_HPP

#include <dataframe/array/type/primitive.hpp>

namespace dataframe {

template <typename T>
struct Opaque;

template <typename T>
struct TypeTraits<Opaque<T>> {
    using scalar_type = T;
    using data_type = ::arrow::FixedSizeBinaryType;
    using array_type = ::arrow::FixedSizeBinaryArray;
    using builder_type = ::arrow::FixedSizeBinaryBuilder;

    static std::shared_ptr<data_type> make_data_type()
    {
        return std::make_shared<data_type>(sizeof(T));
    }

    static std::unique_ptr<builder_type> make_builder(
        ::arrow::MemoryPool *pool = ::arrow::default_memory_pool())
    {
        return std::make_unique<builder_type>(make_data_type(), pool);
    }
};

template <typename T>
struct IsType<Opaque<T>, ::arrow::FixedSizeBinaryType> {
    static bool is_type(const ::arrow::FixedSizeBinaryType &type)
    {
        return type.byte_width() == sizeof(T);
    }
};

} // namespace dataframe

#endif // DATAFRAME_ARRAY_TYPE_OPAQUE_HPP
