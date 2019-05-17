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

#ifndef DATAFRAME_ARRAY_CAST_PRIMITIVE_HPP
#define DATAFRAME_ARRAY_CAST_PRIMITIVE_HPP

#include <dataframe/array/type.hpp>
#include <dataframe/error.hpp>
#include <iterator>

namespace dataframe {

template <typename T>
struct CastArrayVisitor : ::arrow::ArrayVisitor {
    std::shared_ptr<::arrow::Array> result;
    ::arrow::MemoryPool *pool;

    CastArrayVisitor(
        std::shared_ptr<::arrow::Array> data, ::arrow::MemoryPool *p)
        : result(std::move(data))
        , pool(p)
    {
    }

#define DF_DEFINE_VISITOR(Arrow)                                              \
    ::arrow::Status Visit(const ::arrow::Arrow##Array &array) override        \
    {                                                                         \
        using U = typename ::arrow::Arrow##Array::TypeClass::c_type;          \
                                                                              \
        if constexpr (std::is_same_v<T, U>) {                                 \
            return ::arrow::Status::OK();                                     \
        } else {                                                              \
            auto builder = make_builder<T>(pool);                             \
                                                                              \
            if (array.null_count() == 0) {                                    \
                ARROW_RETURN_NOT_OK(builder->AppendValues(array.raw_values(), \
                    array.raw_values() + array.length()));                    \
            } else {                                                          \
                auto n = array.length();                                      \
                std::vector<bool> valid;                                      \
                valid.reserve(static_cast<std::size_t>(n));                   \
                for (std::int64_t i = 0; i != n; ++i) {                       \
                    valid.push_back(array.IsValid(i));                        \
                }                                                             \
                                                                              \
                ARROW_RETURN_NOT_OK(builder->AppendValues(array.raw_values(), \
                    array.raw_values() + array.length(), valid.begin()));     \
            }                                                                 \
            ARROW_RETURN_NOT_OK(builder->Finish(&result));                    \
        }                                                                     \
                                                                              \
        return ::arrow::Status::OK();                                         \
    }

    DF_DEFINE_VISITOR(Int8)
    DF_DEFINE_VISITOR(Int16)
    DF_DEFINE_VISITOR(Int32)
    DF_DEFINE_VISITOR(Int64)
    DF_DEFINE_VISITOR(UInt8)
    DF_DEFINE_VISITOR(UInt16)
    DF_DEFINE_VISITOR(UInt32)
    DF_DEFINE_VISITOR(UInt64)
    DF_DEFINE_VISITOR(Float)
    DF_DEFINE_VISITOR(Double)

#undef DF_DEFINE_VISITOR
};

template <typename T>
inline std::shared_ptr<::arrow::Array> cast_array(
    std::shared_ptr<::arrow::Array> data,
    ::arrow::MemoryPool *pool = ::arrow::default_memory_pool())
{
    if (is_type<T>(data)) {
        return data;
    }

    CastArrayVisitor<T> visitor(std::move(data), pool);
    DF_ARROW_ERROR_HANDLER(visitor.result->Accept(&visitor));

    return std::move(visitor.result);
}

} // namespace dataframe

#endif // DATAFRAME_ARRAY_CAST_PRIMITIVE_HPP
