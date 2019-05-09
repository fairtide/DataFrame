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

#include <dataframe/array/cast/primitive.hpp>

namespace dataframe {

template <>
struct CastArrayVisitor<Datestamp<DateUnit::Day>> final
    : ::arrow::ArrayVisitor {
    std::shared_ptr<::arrow::Array> result;

    CastArrayVisitor(std::shared_ptr<::arrow::Array> data)
        : result(std::move(data))
    {
    }

    ::arrow::Status Visit(const ::arrow::Date32Array &) final
    {
        return ::arrow::Status::OK();
    }
};

template <>
struct CastArrayVisitor<Datestamp<DateUnit::Millisecond>> final
    : ::arrow::ArrayVisitor {
    std::shared_ptr<::arrow::Array> result;

    CastArrayVisitor(std::shared_ptr<::arrow::Array> data)
        : result(std::move(data))
    {
    }

    ::arrow::Status Visit(const ::arrow::Date64Array &) final
    {
        return ::arrow::Status::OK();
    }
};

namespace internal {

template <typename T, typename Array>
::arrow::Status cast_time_array(
    const Array &array, std::shared_ptr<::arrow::Array> &result)
{
    using U = typename T::value_type;

    auto &type = static_cast<typename T::arrow_type &>(*array.type());
    auto unit = static_cast<TimeUnit>(type.unit());

    if (unit == T::unit) {
        return ::arrow::Status::OK();
    }

    auto n = array.length();
    auto v = array.raw_values();

    std::unique_ptr<::arrow::Buffer> buf;
    ARROW_RETURN_NOT_OK(::arrow::AllocateBuffer(::arrow::default_memory_pool(),
        n * static_cast<std::int64_t>(sizeof(U)), &buf));

    auto values = reinterpret_cast<U *>(
        dynamic_cast<::arrow::MutableBuffer &>(*buf).mutable_data());

    auto from_nanos = time_unit_nanos(unit);
    auto to_nanos = time_unit_nanos(T::unit);

    if (from_nanos > to_nanos) {
        auto ratio = from_nanos / to_nanos;
        for (std::int64_t i = 0; i != n; ++i) {
            values[i] = static_cast<U>(v[i] * ratio);
        }
    } else {
        auto ratio = to_nanos / from_nanos;
        for (std::int64_t i = 0; i != n; ++i) {
            values[i] = static_cast<U>(v[i] / ratio);
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

} // namespace internal

template <TimeUnit Unit>
struct CastArrayVisitor<Timestamp<Unit>> final : ::arrow::ArrayVisitor {
    std::shared_ptr<::arrow::Array> result;

    CastArrayVisitor(std::shared_ptr<::arrow::Array> data)
        : result(std::move(data))
    {
    }

    ::arrow::Status Visit(const ::arrow::TimestampArray &array) final
    {
        return internal::cast_time_array<Timestamp<Unit>>(array, result);
    }
};

template <TimeUnit Unit>
struct CastArrayVisitor<TimeOfDay<Unit>> final : ::arrow::ArrayVisitor {
    std::shared_ptr<::arrow::Array> result;

    CastArrayVisitor(std::shared_ptr<::arrow::Array> data)
        : result(std::move(data))
    {
    }

    ::arrow::Status Visit(const ::arrow::Time32Array &array) final
    {
        return internal::cast_time_array<TimeOfDay<Unit>>(array, result);
    }

    ::arrow::Status Visit(const ::arrow::Time64Array &array) final
    {
        return internal::cast_time_array<TimeOfDay<Unit>>(array, result);
    }
};

} // namespace dataframe

#endif // DATAFRAME_ARRAY_DATETIME_HPP
