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

#ifndef DATAFRAME_ARRAY_TYPE_PRIMITIVE_HPP
#define DATAFRAME_ARRAY_TYPE_PRIMITIVE_HPP

#include <dataframe/error.hpp>
#include <arrow/api.h>

namespace dataframe {

template <typename T>
struct TypeTraits;

template <typename T>
using ScalarType = typename TypeTraits<T>::scalar_type;

template <typename T>
using DataType = typename TypeTraits<T>::data_type;

template <typename T>
using ArrayType = typename TypeTraits<T>::array_type;

template <typename T>
using BuilderType = typename TypeTraits<T>::builder_type;

template <typename T>
inline std::shared_ptr<DataType<T>> make_data_type()
{
    return TypeTraits<T>::make_data_type();
}

template <typename T>
inline std::unique_ptr<BuilderType<T>> make_builder(
    ::arrow::MemoryPool *pool = ::arrow::default_memory_pool())
{
    return TypeTraits<T>::make_builder(pool);
}

template <typename T, typename DataType>
struct IsType {
    static constexpr bool is_type(const DataType &) { return false; }
};

#define DF_DEFINE_TYPE_TRAITS(Arrow)                                          \
    template <>                                                               \
    struct TypeTraits<::arrow::Arrow##Type::c_type> {                         \
        using scalar_type = ::arrow::Arrow##Type::c_type;                     \
        using data_type = ::arrow::Arrow##Type;                               \
        using array_type = ::arrow::Arrow##Array;                             \
        using builder_type = ::arrow::Arrow##Builder;                         \
                                                                              \
        static std::shared_ptr<data_type> make_data_type()                    \
        {                                                                     \
            return std::make_shared<data_type>();                             \
        }                                                                     \
                                                                              \
        static std::unique_ptr<builder_type> make_builder(                    \
            ::arrow::MemoryPool *pool = ::arrow::default_memory_pool())       \
        {                                                                     \
            return std::make_unique<builder_type>(pool);                      \
        }                                                                     \
    };                                                                        \
                                                                              \
    template <typename T>                                                     \
    struct IsType<T, ::arrow::Arrow##Type> {                                  \
        static constexpr bool is_type(const ::arrow::Arrow##Type &)           \
        {                                                                     \
            return std::is_same_v<T, ::arrow::Arrow##Type::c_type>;           \
        }                                                                     \
    };

DF_DEFINE_TYPE_TRAITS(Int8)
DF_DEFINE_TYPE_TRAITS(Int16)
DF_DEFINE_TYPE_TRAITS(Int32)
DF_DEFINE_TYPE_TRAITS(Int64)
DF_DEFINE_TYPE_TRAITS(UInt8)
DF_DEFINE_TYPE_TRAITS(UInt16)
DF_DEFINE_TYPE_TRAITS(UInt32)
DF_DEFINE_TYPE_TRAITS(UInt64)
DF_DEFINE_TYPE_TRAITS(Float)
DF_DEFINE_TYPE_TRAITS(Double)

#undef DF_DEFINE_TYPE_TRAITS

template <typename T>
inline bool is_type(const ::arrow::DataType &type)
{
    struct Visitor final : ::arrow::TypeVisitor {
        bool result = false;

#define DF_DEFINE_VISITOR(Arrow)                                              \
    ::arrow::Status Visit(const ::arrow::Arrow##Type &type) final             \
    {                                                                         \
        result = IsType<T, ::arrow::Arrow##Type>::is_type(type);              \
        return ::arrow::Status::OK();                                         \
    }

        DF_DEFINE_VISITOR(Null)
        DF_DEFINE_VISITOR(Boolean)
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
        DF_DEFINE_VISITOR(HalfFloat)
        DF_DEFINE_VISITOR(String)
        DF_DEFINE_VISITOR(Binary)
        DF_DEFINE_VISITOR(FixedSizeBinary)
        DF_DEFINE_VISITOR(Date32)
        DF_DEFINE_VISITOR(Date64)
        DF_DEFINE_VISITOR(Timestamp)
        DF_DEFINE_VISITOR(Time32)
        DF_DEFINE_VISITOR(Time64)
        DF_DEFINE_VISITOR(Interval)
        DF_DEFINE_VISITOR(List)
        DF_DEFINE_VISITOR(Struct)
        DF_DEFINE_VISITOR(Decimal128)
        DF_DEFINE_VISITOR(Union)
        DF_DEFINE_VISITOR(Dictionary)

#undef DF_DEFINE_VISITOR
    };

    Visitor visitor;
    DF_ARROW_ERROR_HANDLER(type.Accept(&visitor));

    return visitor.result;
}

template <typename T>
inline bool is_type(const std::shared_ptr<::arrow::DataType> &type)
{
    return is_type<T>(*type);
}

template <typename T>
inline bool is_type(const ::arrow::Array &array)
{
    return is_type<T>(array.type());
}

template <typename T>
inline bool is_type(const std::shared_ptr<::arrow::Array> &array)
{
    return is_type<T>(*array);
}

} // namespace dataframe

#endif // DATAFRAME_ARRAY_TYPE_PRIMITIVE_HPP
