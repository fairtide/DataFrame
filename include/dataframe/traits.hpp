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

#ifndef DATAFRAME_TRAITS_HPP
#define DATAFRAME_TRAITS_HPP

namespace dataframe {

enum class DataType : std::uint32_t {
    Unknown = 0,

    UInt8 = (1 << 8) | 1,
    Int8,
    UInt16,
    Int16,
    UInt32,
    Int32,
    UInt64,
    Int64,
    Date,
    Timestamp,

    Float = (1 << 9) | 1,
    Double,

    String = (1 << 10) | 1,
    Categorical
};

static constexpr DataType Integer = static_cast<DataType>(1 << 8);
static constexpr DataType Real = static_cast<DataType>(1 << 9);
static constexpr DataType Binary = static_cast<DataType>(1 << 10);

inline constexpr DataType operator~(DataType dtype)
{
    return static_cast<DataType>(
        ~static_cast<std::underlying_type_t<DataType>>(dtype));
}

inline constexpr DataType operator|(DataType dtype1, DataType dtype2)
{
    return static_cast<DataType>(
        static_cast<std::underlying_type_t<DataType>>(dtype1) |
        static_cast<std::underlying_type_t<DataType>>(dtype2));
}

inline constexpr DataType operator&(DataType dtype1, DataType dtype2)
{
    return static_cast<DataType>(
        static_cast<std::underlying_type_t<DataType>>(dtype1) &
        static_cast<std::underlying_type_t<DataType>>(dtype2));
}

inline constexpr DataType operator^(DataType dtype1, DataType dtype2)
{
    return static_cast<DataType>(
        static_cast<std::underlying_type_t<DataType>>(dtype1) ^
        static_cast<std::underlying_type_t<DataType>>(dtype2));
}

inline DataType &operator|=(DataType &dtype1, DataType dtype2)
{
    dtype1 = dtype1 | dtype2;
    return dtype1;
}

inline DataType &operator&=(DataType &dtype1, DataType dtype2)
{
    dtype1 = dtype1 & dtype2;
    return dtype1;
}

inline DataType &operator^=(DataType &dtype1, DataType dtype2)
{
    dtype1 = dtype1 ^ dtype2;
    return dtype1;
}

inline DataType dtype(::arrow::Type::type type)
{
    switch (type) {
        case ::arrow::Type::NA:
            return DataType::Unknown;
        case ::arrow::Type::BOOL:
            return DataType::Unknown;

        case ::arrow::Type::UINT8:
            return DataType::UInt8;
        case ::arrow::Type::INT8:
            return DataType::Int8;
        case ::arrow::Type::UINT16:
            return DataType::UInt16;
        case ::arrow::Type::INT16:
            return DataType::Int16;
        case ::arrow::Type::UINT32:
            return DataType::UInt32;
        case ::arrow::Type::INT32:
            return DataType::Int32;
        case ::arrow::Type::UINT64:
            return DataType::UInt64;
        case ::arrow::Type::INT64:
            return DataType::Int64;

        case ::arrow::Type::HALF_FLOAT:
            return DataType::Unknown;

        case ::arrow::Type::FLOAT:
            return DataType::Float;
        case ::arrow::Type::DOUBLE:
            return DataType::Double;

        case ::arrow::Type::STRING:
            return DataType::String;
        case ::arrow::Type::BINARY:
            return DataType::Unknown;
        case ::arrow::Type::FIXED_SIZE_BINARY:
            return DataType::Unknown;

        case ::arrow::Type::DATE32:
            return DataType::Date;
        case ::arrow::Type::DATE64:
            return DataType::Unknown;
        case ::arrow::Type::TIMESTAMP:
            return DataType::Timestamp;
        case ::arrow::Type::TIME32:
            return DataType::Unknown;
        case ::arrow::Type::TIME64:
            return DataType::Unknown;

        case ::arrow::Type::INTERVAL:
            return DataType::Unknown;
        case ::arrow::Type::DECIMAL:
            return DataType::Unknown;
        case ::arrow::Type::LIST:
            return DataType::Unknown;
        case ::arrow::Type::STRUCT:
            return DataType::Unknown;
        case ::arrow::Type::UNION:
            return DataType::Unknown;
        case ::arrow::Type::DICTIONARY:
            return DataType::Categorical;
        case ::arrow::Type::MAP:
            return DataType::Unknown;
    }
}

template <typename T>
inline constexpr bool is_scalar(T *)
{
    return std::is_scalar_v<T> || std::is_constructible_v<std::string_view, T>;
}

template <typename T>
inline bool is_ctype(::arrow::Type::type type, T *)
{
    switch (type) {
        case ::arrow::Type::NA:
            return false;
        case ::arrow::Type::BOOL:
            return false;

        case ::arrow::Type::UINT8:
            return std::is_same_v<::arrow::UInt8Type::c_type, T>;
        case ::arrow::Type::INT8:
            return std::is_same_v<::arrow::Int8Type::c_type, T>;
        case ::arrow::Type::UINT16:
            return std::is_same_v<::arrow::UInt16Type::c_type, T>;
        case ::arrow::Type::INT16:
            return std::is_same_v<::arrow::Int16Type::c_type, T>;
        case ::arrow::Type::UINT32:
            return std::is_same_v<::arrow::UInt32Type::c_type, T>;
        case ::arrow::Type::INT32:
            return std::is_same_v<::arrow::Int32Type::c_type, T>;
        case ::arrow::Type::UINT64:
            return std::is_same_v<::arrow::UInt64Type::c_type, T>;
        case ::arrow::Type::INT64:
            return std::is_same_v<::arrow::Int64Type::c_type, T>;

        case ::arrow::Type::HALF_FLOAT:
            return false;

        case ::arrow::Type::FLOAT:
            return std::is_same_v<::arrow::FloatType::c_type, T>;
        case ::arrow::Type::DOUBLE:
            return std::is_same_v<::arrow::DoubleType::c_type, T>;

        case ::arrow::Type::STRING:
            return false;
        case ::arrow::Type::BINARY:
            return false;
        case ::arrow::Type::FIXED_SIZE_BINARY:
            return false;

        case ::arrow::Type::DATE32:
            return std::is_same_v<::arrow::Date32Type::c_type, T>;
        case ::arrow::Type::DATE64:
            return std::is_same_v<::arrow::Date64Type::c_type, T>;
        case ::arrow::Type::TIMESTAMP:
            return std::is_same_v<::arrow::TimestampType::c_type, T>;
        case ::arrow::Type::TIME32:
            return std::is_same_v<::arrow::Time32Type::c_type, T>;
        case ::arrow::Type::TIME64:
            return std::is_same_v<::arrow::Time64Type::c_type, T>;

        case ::arrow::Type::INTERVAL:
            return false;
        case ::arrow::Type::DECIMAL:
            return false;
        case ::arrow::Type::LIST:
            return false;
        case ::arrow::Type::STRUCT:
            return false;
        case ::arrow::Type::UNION:
            return false;
        case ::arrow::Type::DICTIONARY:
            return false;
        case ::arrow::Type::MAP:
            return false;
    }
}

template <typename T>
inline bool is_convertible(::arrow::Type::type type, T *)
{
    switch (type) {
        case ::arrow::Type::NA:
            return false;
        case ::arrow::Type::BOOL:
            return false;

        case ::arrow::Type::UINT8:
            return std::is_assignable_v<T &, ::arrow::UInt8Type::c_type>;
        case ::arrow::Type::INT8:
            return std::is_assignable_v<T &, ::arrow::Int8Type::c_type>;
        case ::arrow::Type::UINT16:
            return std::is_assignable_v<T &, ::arrow::UInt16Type::c_type>;
        case ::arrow::Type::INT16:
            return std::is_assignable_v<T &, ::arrow::Int16Type::c_type>;
        case ::arrow::Type::UINT32:
            return std::is_assignable_v<T &, ::arrow::UInt32Type::c_type>;
        case ::arrow::Type::INT32:
            return std::is_assignable_v<T &, ::arrow::Int32Type::c_type>;
        case ::arrow::Type::UINT64:
            return std::is_assignable_v<T &, ::arrow::UInt64Type::c_type>;
        case ::arrow::Type::INT64:
            return std::is_assignable_v<T &, ::arrow::Int64Type::c_type>;

        case ::arrow::Type::HALF_FLOAT:
            return false;

        case ::arrow::Type::FLOAT:
            return std::is_assignable_v<T &, ::arrow::FloatType::c_type>;
        case ::arrow::Type::DOUBLE:
            return std::is_assignable_v<T &, ::arrow::DoubleType::c_type>;

        case ::arrow::Type::STRING:
            return std::is_constructible_v<T &, std::string_view>;
        case ::arrow::Type::BINARY:
            return std::is_constructible_v<T &, std::string_view>;
        case ::arrow::Type::FIXED_SIZE_BINARY:
            return std::is_constructible_v<T &, std::string_view>;

        case ::arrow::Type::DATE32:
            return std::is_assignable_v<T &, ::arrow::Date32Type::c_type>;
        case ::arrow::Type::DATE64:
            return std::is_assignable_v<T &, ::arrow::Date64Type::c_type>;

        case ::arrow::Type::TIMESTAMP:
            return std::is_assignable_v<T &, ::arrow::TimestampType::c_type>;

        case ::arrow::Type::TIME32:
            return std::is_assignable_v<T &, ::arrow::Time32Type::c_type>;
        case ::arrow::Type::TIME64:
            return std::is_assignable_v<T &, ::arrow::Time64Type::c_type>;

        case ::arrow::Type::INTERVAL:
            return false;
        case ::arrow::Type::DECIMAL:
            return false;
        case ::arrow::Type::LIST:
            return false;
        case ::arrow::Type::STRUCT:
            return false;
        case ::arrow::Type::UNION:
            return false;
        case ::arrow::Type::DICTIONARY:
            return false;
        case ::arrow::Type::MAP:
            return false;
    }
}

} // namespace dataframe

#endif // DATAFRAME_TRAITS_HPP
