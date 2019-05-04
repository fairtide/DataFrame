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

#ifndef DATAFRAME_ARRAY_DATETIME_HPP
#define DATAFRAME_ARRAY_DATETIME_HPP

#include <dataframe/array/cast.hpp>
#include <dataframe/array/traits.hpp>
#include <dataframe/array/view.hpp>

namespace dataframe {

inline constexpr std::int64_t time_unit_nanos(::arrow::TimeUnit::type unit)
{
    switch (unit) {
        case ::arrow::TimeUnit::SECOND:
            return 1'000'000'000;
        case ::arrow::TimeUnit::MILLI:
            return 1'000'000;
        case ::arrow::TimeUnit::MICRO:
            return 1'000;
        case ::arrow::TimeUnit::NANO:
            return 1;
    }
    return 1;
}

template <typename ArowType>
struct TimeType {
    using arrow_type = ArowType;
    using value_type = typename ArowType::c_type;

    value_type value;

    TimeType() noexcept = default;
    TimeType(const TimeType &) noexcept = default;
    TimeType(TimeType &&) noexcept = default;
    TimeType &operator=(const TimeType &) noexcept = default;
    TimeType &operator=(TimeType &&) noexcept = default;

    explicit TimeType(value_type v)
        : value(v)
    {
    }

    explicit operator value_type() const noexcept { return value; }
};

template <typename T>
inline bool operator==(const TimeType<T> &v1, const TimeType<T> &v2) noexcept
{
    return v1.value == v2.value;
}

template <typename T>
inline bool operator!=(const TimeType<T> &v1, const TimeType<T> &v2) noexcept
{
    return v1.value != v2.value;
}

template <typename T>
inline bool operator<(const TimeType<T> &v1, const TimeType<T> &v2) noexcept
{
    return v1.value < v2.value;
}

template <typename T>
inline bool operator>(const TimeType<T> &v1, const TimeType<T> &v2) noexcept
{
    return v1.value > v2.value;
}

template <typename T>
inline bool operator<=(const TimeType<T> &v1, const TimeType<T> &v2) noexcept
{
    return v1.value <= v2.value;
}

template <typename T>
inline bool operator>=(const TimeType<T> &v1, const TimeType<T> &v2) noexcept
{
    return v1.value >= v2.value;
}

struct Date32 : public TimeType<::arrow::Date32Type> {
    using TimeType<::arrow::Date32Type>::TimeType;
};

struct Date64 : public TimeType<::arrow::Date64Type> {
    using TimeType<::arrow::Date64Type>::TimeType;
};

template <::arrow::TimeUnit::type Unit>
struct Timestamp : public TimeType<::arrow::TimestampType> {
    static constexpr ::arrow::TimeUnit::type unit = Unit;
    using TimeType<::arrow::TimestampType>::TimeType;
};

template <::arrow::TimeUnit::type Unit>
struct Time32 : public TimeType<::arrow::Time32Type> {
    static constexpr ::arrow::TimeUnit::type unit = Unit;
    using TimeType<::arrow::Time32Type>::TimeType;
};

template <::arrow::TimeUnit::type Unit>
struct Time64 : public TimeType<::arrow::Time64Type> {
    static constexpr ::arrow::TimeUnit::type unit = Unit;
    using TimeType<::arrow::Time64Type>::TimeType;
};

template <>
struct TypeTraits<Date32> {
    static std::shared_ptr<::arrow::Date32Type> data_type()
    {
        return std::make_shared<::arrow::Date32Type>();
    }

    static std::unique_ptr<::arrow::Date32Builder> builder()
    {
        return std::make_unique<::arrow::Date32Builder>(
            ::arrow::default_memory_pool());
    }

    using ctype = Date32::value_type;
    using array_type = ::arrow::Date32Array;
};

template <>
struct TypeTraits<Date64> {
    static std::shared_ptr<::arrow::Date64Type> data_type()
    {
        return std::make_shared<::arrow::Date64Type>();
    }

    static std::unique_ptr<::arrow::Date64Builder> builder()
    {
        return std::make_unique<::arrow::Date64Builder>(
            ::arrow::default_memory_pool());
    }

    using ctype = Date64::value_type;
    using array_type = ::arrow::Date64Array;
};

template <::arrow::TimeUnit::type Unit>
struct TypeTraits<Timestamp<Unit>> {
    static std::shared_ptr<::arrow::TimestampType> data_type()
    {
        return std::make_shared<::arrow::TimestampType>(Unit);
    }

    static std::unique_ptr<::arrow::TimestampBuilder> builder()
    {
        return std::make_unique<::arrow::TimestampBuilder>(
            data_type(), ::arrow::default_memory_pool());
    }

    using ctype = typename Timestamp<Unit>::value_type;
    using array_type = ::arrow::TimestampArray;
};

template <::arrow::TimeUnit::type Unit>
struct TypeTraits<Time32<Unit>> {
    static std::shared_ptr<::arrow::Time32Type> data_type()
    {
        return std::make_shared<::arrow::Time32Type>(Unit);
    }

    static std::unique_ptr<::arrow::Time32Builder> builder()
    {
        return std::make_unique<::arrow::Time32Builder>(
            data_type(), ::arrow::default_memory_pool());
    }

    using ctype = typename Time32<Unit>::value_type;
    using array_type = ::arrow::Time32Array;
};

template <::arrow::TimeUnit::type Unit>
struct TypeTraits<Time64<Unit>> {
    static std::shared_ptr<::arrow::Time64Type> data_type()
    {
        return std::make_shared<::arrow::Time64Type>(Unit);
    }

    static std::unique_ptr<::arrow::Time64Builder> builder()
    {
        return std::make_unique<::arrow::Time64Builder>(
            data_type(), ::arrow::default_memory_pool());
    }

    using ctype = typename Time64<Unit>::value_type;
    using array_type = ::arrow::Time64Array;
};

namespace internal {

template <>
struct CastArrayVisitor<Date32> final : ::arrow::ArrayVisitor {
    std::shared_ptr<::arrow::Array> result;

    CastArrayVisitor(std::shared_ptr<::arrow::Array> data, bool)
        : result(std::move(data))
    {
    }

    ::arrow::Status Visit(const ::arrow::Date32Array &) final
    {
        return ::arrow::Status::OK();
    }
};

template <>
struct CastArrayVisitor<Date64> final : ::arrow::ArrayVisitor {
    std::shared_ptr<::arrow::Array> result;

    CastArrayVisitor(std::shared_ptr<::arrow::Array> data, bool)
        : result(std::move(data))
    {
    }

    ::arrow::Status Visit(const ::arrow::Date64Array &) final
    {
        return ::arrow::Status::OK();
    }
};

template <typename T>
struct CastTimeArrayVisitor : public ::arrow::ArrayVisitor {
    std::shared_ptr<::arrow::Array> result;
    bool nocast;

    CastTimeArrayVisitor(std::shared_ptr<::arrow::Array> data, bool nc)
        : result(std::move(data))
        , nocast(nc)
    {
    }

    ::arrow::Status Visit(const ArrayType<T> &array) final
    {
        using U = typename T::value_type;

        auto &type = static_cast<typename T::arrow_type &>(*array.type());

        if (type.unit() == T::unit) {
            return ::arrow::Status::OK();
        }

        if (nocast) {
            return ::arrow::Status::Invalid("Time unit mismatch");
        }

        auto n = array.length();
        auto v = array.raw_values();

        std::unique_ptr<::arrow::Buffer> buf;
        ARROW_RETURN_NOT_OK(
            ::arrow::AllocateBuffer(::arrow::default_memory_pool(),
                n * static_cast<std::int64_t>(sizeof(U)), &buf));

        auto values = reinterpret_cast<U *>(
            dynamic_cast<::arrow::MutableBuffer &>(*buf).mutable_data());

        auto from_nanos = time_unit_nanos(type.unit());
        auto to_nanos = time_unit_nanos(T::unit);

        if (from_nanos > to_nanos) {
            auto ratio = static_cast<U>(from_nanos / to_nanos);
            for (std::int64_t i = 0; i != n; ++i) {
                values[i] = v[i] * ratio;
            }
        } else {
            auto ratio = static_cast<U>(to_nanos / from_nanos);
            for (std::int64_t i = 0; i != n; ++i) {
                values[i] = v[i] / ratio;
            }
        }

        auto builder = TypeTraits<T>::builder();

        if (array.null_count() == 0) {
            ARROW_RETURN_NOT_OK(builder->AppendValues(values, values + n));
        } else {
            std::vector<bool> valid;
            valid.reserve(static_cast<std::size_t>(n));
            for (std::int64_t i = 0; i != n; ++i) {
                valid.push_back(array.IsValid(i));
            }

            ARROW_RETURN_NOT_OK(
                builder->AppendValues(values, values + n, valid.begin()));
        }

        ARROW_RETURN_NOT_OK(builder->Finish(&result));

        return ::arrow::Status::OK();
    }
};

template <::arrow::TimeUnit::type Unit>
struct CastArrayVisitor<Timestamp<Unit>> final
    : public CastTimeArrayVisitor<Timestamp<Unit>> {
    using CastTimeArrayVisitor<Timestamp<Unit>>::CastTimeArrayVisitor;
};

template <::arrow::TimeUnit::type Unit>
struct CastArrayVisitor<Time32<Unit>> final
    : public CastTimeArrayVisitor<Time32<Unit>> {
    using CastTimeArrayVisitor<Time32<Unit>>::CastTimeArrayVisitor;
};

template <::arrow::TimeUnit::type Unit>
struct CastArrayVisitor<Time64<Unit>> final
    : public CastTimeArrayVisitor<Time64<Unit>> {
    using CastTimeArrayVisitor<Time64<Unit>>::CastTimeArrayVisitor;
};

} // namespace internal

} // namespace dataframe

#endif // DATAFRAME_ARRAY_DATETIME_HPP
