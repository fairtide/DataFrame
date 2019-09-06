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

#include <dataframe/array/type/iterator.hpp>
#include <dataframe/array/type/primitive.hpp>

namespace dataframe {

enum class DateUnit { Day, Millisecond = ::arrow::TimeUnit::MILLI };

enum class TimeUnit {
    Second = ::arrow::TimeUnit::SECOND,
    Millisecond = ::arrow::TimeUnit::MILLI,
    Microsecond = ::arrow::TimeUnit::MICRO,
    Nanosecond = ::arrow::TimeUnit::NANO
};

inline constexpr std::int64_t time_unit_nanos(DateUnit unit)
{
    switch (unit) {
        case DateUnit::Day:
            return INT64_C(24) * 3600 * 1'000'000'000;
        case DateUnit::Millisecond:
            return 1'000'000;
    }
    return 1;
}

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

inline constexpr std::int64_t time_unit_nanos(const ::arrow::Date32Array &)
{
    return time_unit_nanos(DateUnit::Day);
}

inline constexpr std::int64_t time_unit_nanos(const ::arrow::Date64Array &)
{
    return time_unit_nanos(DateUnit::Millisecond);
}

inline std::int64_t time_unit_nanos(const ::arrow::TimestampArray &array)
{
    return time_unit_nanos(
        std::static_pointer_cast<::arrow::TimestampType>(array.type())
            ->unit());
}

inline std::int64_t time_unit_nanos(const ::arrow::Time32Array &array)
{
    return time_unit_nanos(
        std::static_pointer_cast<::arrow::Time32Type>(array.type())->unit());
}

inline std::int64_t time_unit_nanos(const ::arrow::Time64Array &array)
{
    return time_unit_nanos(
        std::static_pointer_cast<::arrow::Time64Type>(array.type())->unit());
}

template <typename T>
struct TimeType {
    using value_type = T;

    value_type value;

    TimeType() = default;
    TimeType(const TimeType &) = default;
    TimeType(TimeType &&) noexcept = default;
    TimeType &operator=(const TimeType &) = default;
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
struct Datestamp<DateUnit::Day> : TimeType<::arrow::Date32Type::c_type> {
    static constexpr DateUnit unit = DateUnit::Day;
    using TimeType<::arrow::Date32Type::c_type>::TimeType;
};

template <>
struct Datestamp<DateUnit::Millisecond>
    : TimeType<::arrow::Date64Type::c_type> {
    static constexpr DateUnit unit = DateUnit::Millisecond;
    using TimeType<::arrow::Date64Type::c_type>::TimeType;
};

template <TimeUnit Unit>
struct Timestamp : TimeType<::arrow::TimestampType::c_type> {
    static constexpr TimeUnit unit = Unit;
    using TimeType<::arrow::TimestampType::c_type>::TimeType;
};

template <TimeUnit>
struct Time;

template <>
struct Time<TimeUnit::Second> : TimeType<::arrow::Time32Type::c_type> {
    static constexpr TimeUnit unit = TimeUnit::Second;
    using TimeType<::arrow::Time32Type::c_type>::TimeType;
};

template <>
struct Time<TimeUnit::Millisecond> : TimeType<::arrow::Time32Type::c_type> {
    static constexpr TimeUnit unit = TimeUnit::Millisecond;
    using TimeType<::arrow::Time32Type::c_type>::TimeType;
};

template <>
struct Time<TimeUnit::Microsecond> : TimeType<::arrow::Time64Type::c_type> {
    static constexpr TimeUnit unit = TimeUnit::Microsecond;
    using TimeType<::arrow::Time64Type::c_type>::TimeType;
};

template <>
struct Time<TimeUnit::Nanosecond> : TimeType<::arrow::Time64Type::c_type> {
    static constexpr TimeUnit unit = TimeUnit::Nanosecond;
    using TimeType<::arrow::Time64Type::c_type>::TimeType;
};

template <>
struct TypeTraits<Datestamp<DateUnit::Day>> {
    using scalar_type = ::arrow::Date32Type::c_type;
    using data_type = ::arrow::Date32Type;
    using array_type = ::arrow::Date32Array;
    using builder_type = ::arrow::Date32Builder;

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

template <>
struct TypeTraits<Datestamp<DateUnit::Millisecond>> {
    using scalar_type = ::arrow::Date64Type::c_type;
    using data_type = ::arrow::Date64Type;
    using array_type = ::arrow::Date64Array;
    using builder_type = ::arrow::Date64Builder;

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

template <TimeUnit Unit>
struct TypeTraits<Timestamp<Unit>> {
    using scalar_type = ::arrow::TimestampType::c_type;
    using data_type = ::arrow::TimestampType;
    using array_type = ::arrow::TimestampArray;
    using builder_type = ::arrow::TimestampBuilder;

    static std::shared_ptr<data_type> make_data_type()
    {
        return std::make_shared<data_type>(
            static_cast<::arrow::TimeUnit::type>(Unit));
    }

    static std::unique_ptr<builder_type> make_builder(
        ::arrow::MemoryPool *pool = ::arrow::default_memory_pool())
    {
        return std::make_unique<builder_type>(make_data_type(), pool);
    }
};

template <>
struct TypeTraits<Time<TimeUnit::Second>> {
    using scalar_type = ::arrow::Time32Type::c_type;
    using data_type = ::arrow::Time32Type;
    using array_type = ::arrow::Time32Array;
    using builder_type = ::arrow::Time32Builder;

    static std::shared_ptr<data_type> make_data_type()
    {
        return std::make_shared<data_type>(::arrow::TimeUnit::SECOND);
    }

    static std::unique_ptr<builder_type> make_builder(
        ::arrow::MemoryPool *pool = ::arrow::default_memory_pool())
    {
        return std::make_unique<builder_type>(make_data_type(), pool);
    }
};

template <>
struct TypeTraits<Time<TimeUnit::Millisecond>> {
    using scalar_type = ::arrow::Time32Type::c_type;
    using data_type = ::arrow::Time32Type;
    using array_type = ::arrow::Time32Array;
    using builder_type = ::arrow::Time32Builder;

    static std::shared_ptr<data_type> make_data_type()
    {
        return std::make_shared<data_type>(::arrow::TimeUnit::MILLI);
    }

    static std::unique_ptr<builder_type> make_builder(
        ::arrow::MemoryPool *pool = ::arrow::default_memory_pool())
    {
        return std::make_unique<builder_type>(make_data_type(), pool);
    }
};

template <>
struct TypeTraits<Time<TimeUnit::Microsecond>> {
    using scalar_type = ::arrow::Time64Type::c_type;
    using data_type = ::arrow::Time64Type;
    using array_type = ::arrow::Time64Array;
    using builder_type = ::arrow::Time64Builder;

    static std::shared_ptr<data_type> make_data_type()
    {
        return std::make_shared<data_type>(::arrow::TimeUnit::MICRO);
    }

    static std::unique_ptr<builder_type> make_builder(
        ::arrow::MemoryPool *pool = ::arrow::default_memory_pool())
    {
        return std::make_unique<builder_type>(make_data_type(), pool);
    }
};

template <>
struct TypeTraits<Time<TimeUnit::Nanosecond>> {
    using scalar_type = ::arrow::Time64Type::c_type;
    using data_type = ::arrow::Time64Type;
    using array_type = ::arrow::Time64Array;
    using builder_type = ::arrow::Time64Builder;

    static std::shared_ptr<data_type> make_data_type()
    {
        return std::make_shared<data_type>(::arrow::TimeUnit::NANO);
    }

    static std::unique_ptr<builder_type> make_builder(
        ::arrow::MemoryPool *pool = ::arrow::default_memory_pool())
    {
        return std::make_unique<builder_type>(make_data_type(), pool);
    }
};

template <typename T>
struct IsType<T, ::arrow::Date32Type> {
    static constexpr bool is_type(const ::arrow::Date32Type &)
    {
        return std::is_same_v<T, Datestamp<DateUnit::Day>>;
    }
};

template <typename T>
struct IsType<T, ::arrow::Date64Type> {
    static constexpr bool is_type(const ::arrow::Date64Type &)
    {
        return std::is_same_v<T, Datestamp<DateUnit::Millisecond>>;
    }
};

template <TimeUnit Unit>
struct IsType<Timestamp<Unit>, ::arrow::TimestampType> {
    static bool is_type(const ::arrow::TimestampType &type)
    {
        return Unit == static_cast<TimeUnit>(type.unit());
    }
};

template <TimeUnit Unit>
struct IsType<Time<Unit>, ::arrow::Time32Type> {
    static bool is_type(const ::arrow::Time32Type &type)
    {
        return Unit == static_cast<TimeUnit>(type.unit());
    }
};

template <TimeUnit Unit>
struct IsType<Time<Unit>, ::arrow::Time64Type> {
    static bool is_type(const ::arrow::Time64Type &type)
    {
        return Unit == static_cast<TimeUnit>(type.unit());
    }
};

template <typename T, typename Iter>
class DatetimeValueIterator
{
  public:
    using value_type = typename T::value_type;
    using reference = value_type;
    using iterator_category =
        typename std::iterator_traits<Iter>::iterator_category;

    DatetimeValueIterator(Iter iter)
        : iter_(iter)
    {
    }

    reference operator*() const { return (*iter_).value; }

    DF_DEFINE_ITERATOR_MEMBERS(DatetimeValueIterator, iter_)

  private:
    Iter iter_;
};

} // namespace dataframe

#endif // DATAFRAME_ARRAY_TYPE_DATETIME_HPP
