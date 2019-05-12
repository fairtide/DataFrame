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

#ifndef DATAFRAME_ARRAY_TYPE_HPP
#define DATAFRAME_ARRAY_TYPE_HPP

#include <dataframe/array/type/datetime.hpp>
#include <dataframe/array/type/dict.hpp>
#include <dataframe/array/type/iterator.hpp>
#include <dataframe/array/type/list.hpp>
#include <dataframe/array/type/primitive.hpp>
#include <dataframe/array/type/string.hpp>
#include <dataframe/array/type/struct.hpp>

namespace dataframe {

namespace internal {

template <typename>
struct IsTypeVisitor;

}

template <typename T>
inline bool is_type(const ::arrow::DataType &type)
{
    internal::IsTypeVisitor<T> visitor;
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

namespace internal {

template <typename...>
struct IsStrucType;

template <typename... Types>
struct IsStrucType<std::tuple<Types...>> {
    static constexpr std::size_t nfields = sizeof...(Types);

    static bool value(const ::arrow::StructType &type)
    {
        if (type.num_children() != nfields) {
            return false;
        }

        return value<0>(type, std::integral_constant<bool, 0 < nfields>());
    }

    template <std::size_t N>
    static bool value(const ::arrow::StructType &type, std::true_type)
    {
        using T = FieldType<N, Types...>;

        if (!is_type<T>(*type.child(N)->type())) {
            return false;
        }

        return value<N + 1>(
            type, std::integral_constant<bool, N + 1 < nfields>());
    }

    template <std::size_t>
    static bool value(const ::arrow::StructType &, std::false_type)
    {
        return true;
    }
};

template <typename T>
struct IsTypeVisitor final : ::arrow::TypeVisitor {
    bool result = false;

    ::arrow::Status Visit(const ::arrow::NullType &) final
    {
        result = std::is_same_v<T, void>;

        return ::arrow::Status::OK();
    }

    ::arrow::Status Visit(const ::arrow::BooleanType &) final
    {
        result = std::is_same_v<T, bool>;

        return ::arrow::Status::OK();
    }

#define DF_DEFINE_VISITOR(Arrow)                                              \
    ::arrow::Status Visit(const ::arrow::Arrow##Type &) final                 \
    {                                                                         \
        using U = typename ::arrow::Arrow##Type::c_type;                      \
                                                                              \
        result = std::is_same_v<T, U>;                                        \
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

    // ::arrow::Status Visit(const ::arrow::HalfFloatType &type) final {}

    ::arrow::Status Visit(const ::arrow::StringType &) final
    {
        result = std::is_same_v<T, std::string> ||
            std::is_same_v<T, std::string_view>;

        return ::arrow::Status::OK();
    }

    ::arrow::Status Visit(const ::arrow::BinaryType &) final
    {
        result = std::is_same_v<T, Bytes>;

        return ::arrow::Status::OK();
    }

    // ::arrow::Status Visit(const ::arrow::FixedSizeBinaryType &) final {}

    ::arrow::Status Visit(const ::arrow::Date32Type &) final
    {
        result = std::is_same_v<T, Datestamp<DateUnit::Day>>;

        return ::arrow::Status::OK();
    }

    ::arrow::Status Visit(const ::arrow::Date64Type &) final
    {
        result = std::is_same_v<T, Datestamp<DateUnit::Millisecond>>;

        return ::arrow::Status::OK();
    }

    ::arrow::Status Visit(const ::arrow::Time32Type &type) final
    {
        switch (type.unit()) {
            case ::arrow::TimeUnit::SECOND:
                result = std::is_same_v<T, Time<TimeUnit::Second>>;
                break;
            case ::arrow::TimeUnit::MILLI:
                result = std::is_same_v<T, Time<TimeUnit::Millisecond>>;
                break;
            case ::arrow::TimeUnit::MICRO:
                break;
            case ::arrow::TimeUnit::NANO:
                break;
        }

        return ::arrow::Status::OK();
    }

    ::arrow::Status Visit(const ::arrow::Time64Type &type) final
    {
        switch (type.unit()) {
            case ::arrow::TimeUnit::SECOND:
                break;
            case ::arrow::TimeUnit::MILLI:
                break;
            case ::arrow::TimeUnit::MICRO:
                result = std::is_same_v<T, Time<TimeUnit::Microsecond>>;
                break;
            case ::arrow::TimeUnit::NANO:
                result = std::is_same_v<T, Time<TimeUnit::Nanosecond>>;
                break;
        }

        return ::arrow::Status::OK();
    }

    ::arrow::Status Visit(const ::arrow::TimestampType &type) final
    {
        switch (type.unit()) {
            case ::arrow::TimeUnit::SECOND:
                result = std::is_same_v<T, Timestamp<TimeUnit::Second>>;
                break;
            case ::arrow::TimeUnit::MILLI:
                result = std::is_same_v<T, Timestamp<TimeUnit::Millisecond>>;
                break;
            case ::arrow::TimeUnit::MICRO:
                result = std::is_same_v<T, Timestamp<TimeUnit::Microsecond>>;
                break;
            case ::arrow::TimeUnit::NANO:
                result = std::is_same_v<T, Timestamp<TimeUnit::Nanosecond>>;
                break;
        }

        return ::arrow::Status::OK();
    }

    // ::arrow::Status Visit(const ::arrow::IntervalType &type) final {}

    ::arrow::Status Visit(const ::arrow::ListType &type) final
    {
        if constexpr (std::is_base_of_v<ListBase, T>) {
            result = is_type<typename T::data_type>(*type.value_type());
        }

        return ::arrow::Status::OK();
    }

    ::arrow::Status Visit(const ::arrow::StructType &type) final
    {
        if constexpr (std::is_base_of_v<StructBase, T>) {
            result = IsStrucType<typename T::data_type>::value(type);
        }

        return ::arrow::Status::OK();
    }

    // ::arrow::Status Visit(const ::arrow::Decimal128Type &type) final {}

    // ::arrow::Status Visit(const ::arrow::UnionType &type) final {}

    ::arrow::Status Visit(const ::arrow::DictionaryType &type) final
    {
        if constexpr (std::is_base_of_v<DictBase, T>) {
            result = is_type<std::int32_t>(*type.index_type()) &&
                is_type<typename T::data_type>(*type.dictionary()->type());
        }

        return ::arrow::Status::OK();
    }
};

} // namespace internal

} // namespace dataframe

#endif // DATAFRAME_ARRAY_TYPE_HPP
