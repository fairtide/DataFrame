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

#ifndef DATAFRAME_ARRAY_TYPE_DATETIME_HPP
#define DATAFRAME_ARRAY_TYPE_DATETIME_HPP

#include <dataframe/array/type/primitive.hpp>

namespace dataframe {

enum class DateUnit { Day, Millisecond };

enum class TimeUnit {
    Second = ::arrow::TimeUnit::SECOND,
    Millisecond = ::arrow::TimeUnit::MILLI,
    Microsecond = ::arrow::TimeUnit::MICRO,
    Nanosecond = ::arrow::TimeUnit::NANO
};

inline constexpr std::int64_t time_unit_nanos(TimeUnit unit)
{
    switch (unit) {
        case TimeUnit::Second:
            return 1'000'000'000;
        case TimeUnit::Millisecond:
            return 1'000'000;
        case TimeUnit::Microsecond:
            return 1'000;
        case TimeUnit::Nanosecond:
            return 1;
    }
    return 1;
}

inline constexpr std::int64_t time_unit_nanos(::arrow::TimeUnit::type unit)
{
    return time_unit_nanos(static_cast<TimeUnit>(unit));
}

struct TimeTypeBase {
};

template <typename ArrowType>
struct TimeType final : TimeTypeBase {
    using arrow_type = ArrowType;
    using value_type = typename ArrowType::c_type;

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

template <DateUnit>
struct Datestamp;

template <>
struct Datestamp<DateUnit::Day> : TimeType<::arrow::Date32Type> {
    using TimeType<::arrow::Date32Type>::TimeType;
};

template <>
struct Datestamp<DateUnit::Millisecond> : TimeType<::arrow::Date64Type> {
    using TimeType<::arrow::Date64Type>::TimeType;
};

template <TimeUnit Unit>
struct Timestamp : TimeType<::arrow::TimestampType> {
    static constexpr TimeUnit unit = Unit;
    using TimeType<::arrow::TimestampType>::TimeType;
};

template <TimeUnit>
struct Time;

template <>
struct Time<TimeUnit::Second> : TimeType<::arrow::Time32Type> {
    static constexpr TimeUnit unit = TimeUnit::Second;
    using TimeType<::arrow::Time32Type>::TimeType;
};

template <>
struct Time<TimeUnit::Millisecond> : TimeType<::arrow::Time32Type> {
    static constexpr TimeUnit unit = TimeUnit::Millisecond;
    using TimeType<::arrow::Time32Type>::TimeType;
};

template <>
struct Time<TimeUnit::Microsecond> : TimeType<::arrow::Time64Type> {
    static constexpr TimeUnit unit = TimeUnit::Microsecond;
    using TimeType<::arrow::Time64Type>::TimeType;
};

template <>
struct Time<TimeUnit::Nanosecond> : TimeType<::arrow::Time64Type> {
    static constexpr TimeUnit unit = TimeUnit::Nanosecond;
    using TimeType<::arrow::Time64Type>::TimeType;
};

template <>
struct TypeTraits<Datestamp<DateUnit::Day>> {
    static std::shared_ptr<::arrow::Date32Type> data_type()
    {
        return std::make_shared<::arrow::Date32Type>();
    }

    static auto builder()
    {
        return std::make_unique<::arrow::Date32Builder>(
            ::arrow::default_memory_pool());
    }

    using ctype = Datestamp<DateUnit::Day>::value_type;
    using array_type = ::arrow::Date32Array;
};

template <>
struct TypeTraits<Datestamp<DateUnit::Millisecond>> {
    static std::shared_ptr<::arrow::Date64Type> data_type()
    {
        return std::make_shared<::arrow::Date64Type>();
    }

    static auto builder()
    {
        return std::make_unique<::arrow::Date64Builder>(
            ::arrow::default_memory_pool());
    }

    using ctype = Datestamp<DateUnit::Millisecond>::value_type;
    using array_type = ::arrow::Date64Array;
};

template <TimeUnit Unit>
struct TypeTraits<Timestamp<Unit>> {
    static std::shared_ptr<::arrow::TimestampType> data_type()
    {
        return std::make_shared<::arrow::TimestampType>(
            static_cast<::arrow::TimeUnit::type>(Unit));
    }

    static std::unique_ptr<::arrow::TimestampBuilder> builder()
    {
        return std::make_unique<::arrow::TimestampBuilder>(
            data_type(), ::arrow::default_memory_pool());
    }

    using ctype = typename Timestamp<Unit>::value_type;
    using array_type = ::arrow::TimestampArray;
};

template <>
struct TypeTraits<Time<TimeUnit::Second>> {
    static std::shared_ptr<::arrow::Time32Type> data_type()
    {
        return std::make_shared<::arrow::Time32Type>(
            ::arrow::TimeUnit::SECOND);
    }

    static std::unique_ptr<::arrow::Time32Builder> builder()
    {
        return std::make_unique<::arrow::Time32Builder>(
            data_type(), ::arrow::default_memory_pool());
    }

    using ctype = typename ::arrow::Time32Type::c_type;
    using array_type = ::arrow::Time32Array;
};

template <>
struct TypeTraits<Time<TimeUnit::Millisecond>> {
    static std::shared_ptr<::arrow::Time32Type> data_type()
    {
        return std::make_shared<::arrow::Time32Type>(::arrow::TimeUnit::MILLI);
    }

    static std::unique_ptr<::arrow::Time32Builder> builder()
    {
        return std::make_unique<::arrow::Time32Builder>(
            data_type(), ::arrow::default_memory_pool());
    }

    using ctype = typename ::arrow::Time32Type::c_type;
    using array_type = ::arrow::Time32Array;
};

template <>
struct TypeTraits<Time<TimeUnit::Microsecond>> {
    static std::shared_ptr<::arrow::Time64Type> data_type()
    {
        return std::make_shared<::arrow::Time64Type>(::arrow::TimeUnit::MICRO);
    }

    static std::unique_ptr<::arrow::Time64Builder> builder()
    {
        return std::make_unique<::arrow::Time64Builder>(
            data_type(), ::arrow::default_memory_pool());
    }

    using ctype = typename ::arrow::Time64Type::c_type;
    using array_type = ::arrow::Time64Array;
};

template <>
struct TypeTraits<Time<TimeUnit::Nanosecond>> {
    static std::shared_ptr<::arrow::Time64Type> data_type()
    {
        return std::make_shared<::arrow::Time64Type>(::arrow::TimeUnit::NANO);
    }

    static std::unique_ptr<::arrow::Time64Builder> builder()
    {
        return std::make_unique<::arrow::Time64Builder>(
            data_type(), ::arrow::default_memory_pool());
    }

    using ctype = typename ::arrow::Time64Type::c_type;
    using array_type = ::arrow::Time64Array;
};

} // namespace dataframe

#endif // DATAFRAME_ARRAY_TYPE_DATETIME_HPP
