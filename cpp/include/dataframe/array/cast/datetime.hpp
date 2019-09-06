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

#ifndef DATAFRAME_ARRAY_CAST_DATETIME_HPP
#define DATAFRAME_ARRAY_CAST_DATETIME_HPP

#include <dataframe/array/cast/primitive.hpp>

namespace dataframe {

namespace internal {

template <typename T, typename Array>
::arrow::Status cast_time_array(const Array &array,
    std::shared_ptr<::arrow::Array> &result, ::arrow::MemoryPool *pool)
{
    using U = typename T::value_type;

    auto from_nanos = time_unit_nanos(array);
    auto to_nanos = time_unit_nanos(T::unit);

    if (from_nanos == to_nanos) {
        auto data = array.data()->Copy();
        data->type = make_data_type<T>();
        result = ::arrow::MakeArray(data);
    }

    auto n = array.length();
    auto v = array.raw_values();

    std::unique_ptr<::arrow::Buffer> value_buffer;
    ARROW_RETURN_NOT_OK(::arrow::AllocateBuffer(pool,
        n * static_cast<std::int64_t>(sizeof(typename T::value_type)),
        &value_buffer));

    auto values = reinterpret_cast<typename T::value_type *>(
        dynamic_cast<::arrow::MutableBuffer &>(*value_buffer).mutable_data());

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

    ::arrow::ArrayData data(
        make_data_type<T>(), array.length(), array.null_count());

    data.buffers.reserve(2);
    data.buffers.emplace_back(nullptr);
    data.buffers.emplace_back(std::move(value_buffer));

    if (data.null_count != 0) {
        ARROW_RETURN_NOT_OK(
            ::arrow::internal::CopyBitmap(pool, array.null_bitmap()->data(),
                array.offset(), array.length(), &data.buffers[0]));
    }

    result = ::arrow::MakeArray(
        std::make_shared<::arrow::ArrayData>(std::move(data)));

    return ::arrow::Status::OK();
}

} // namespace internal

template <DateUnit Unit>
struct CastArrayVisitor<Datestamp<Unit>> : ::arrow::ArrayVisitor {
    std::shared_ptr<::arrow::Array> result;
    ::arrow::MemoryPool *pool;

    CastArrayVisitor(
        std::shared_ptr<::arrow::Array> data, ::arrow::MemoryPool *p)
        : result(std::move(data))
        , pool(p)
    {
    }

    ::arrow::Status Visit(const ::arrow::Date32Array &array) override
    {
        return internal::cast_time_array<Datestamp<Unit>>(array, result, pool);
    }

    ::arrow::Status Visit(const ::arrow::Date64Array &array) override
    {
        return internal::cast_time_array<Datestamp<Unit>>(array, result, pool);
    }

    ::arrow::Status Visit(const ::arrow::TimestampArray &array) override
    {
        return internal::cast_time_array<Datestamp<Unit>>(array, result, pool);
    }
};

template <TimeUnit Unit>
struct CastArrayVisitor<Timestamp<Unit>> : ::arrow::ArrayVisitor {
    std::shared_ptr<::arrow::Array> result;
    ::arrow::MemoryPool *pool;

    CastArrayVisitor(
        std::shared_ptr<::arrow::Array> data, ::arrow::MemoryPool *p)
        : result(std::move(data))
        , pool(p)
    {
    }

    ::arrow::Status Visit(const ::arrow::Date32Array &array) override
    {
        return internal::cast_time_array<Timestamp<Unit>>(array, result, pool);
    }

    ::arrow::Status Visit(const ::arrow::Date64Array &array) override
    {
        return internal::cast_time_array<Timestamp<Unit>>(array, result, pool);
    }

    ::arrow::Status Visit(const ::arrow::TimestampArray &array) override
    {
        return internal::cast_time_array<Timestamp<Unit>>(array, result, pool);
    }
};

template <TimeUnit Unit>
struct CastArrayVisitor<Time<Unit>> : ::arrow::ArrayVisitor {
    std::shared_ptr<::arrow::Array> result;
    ::arrow::MemoryPool *pool;

    CastArrayVisitor(
        std::shared_ptr<::arrow::Array> data, ::arrow::MemoryPool *p)
        : result(std::move(data))
        , pool(p)
    {
    }

    ::arrow::Status Visit(const ::arrow::Time32Array &array) override
    {
        return internal::cast_time_array<Time<Unit>>(array, result, pool);
    }

    ::arrow::Status Visit(const ::arrow::Time64Array &array) override
    {
        return internal::cast_time_array<Time<Unit>>(array, result, pool);
    }
};

} // namespace dataframe

#endif // DATAFRAME_ARRAY_CAST_DATETIME_HPP
