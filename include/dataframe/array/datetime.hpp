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

namespace internal {

template <>
struct CastArrayVisitor<Date32> final : ::arrow::ArrayVisitor {
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
struct CastArrayVisitor<Date64> final : ::arrow::ArrayVisitor {
    std::shared_ptr<::arrow::Array> result;

    CastArrayVisitor(std::shared_ptr<::arrow::Array> data)
        : result(std::move(data))
    {
    }

    ::arrow::Status Visit(const ::arrow::Date64Array &) final
    {
        return ::arrow::Status::OK();
    }
}; // namespace internal

template <typename T>
struct CastTimeArrayVisitor : public ::arrow::ArrayVisitor {
    std::shared_ptr<::arrow::Array> result;

    CastTimeArrayVisitor(std::shared_ptr<::arrow::Array> data)
        : result(std::move(data))
    {
    }

    ::arrow::Status Visit(const ArrayType<T> &array) final
    {
        using U = typename T::value_type;

        auto &type = static_cast<typename T::arrow_type &>(*array.type());

        if (type.unit() == T::unit) {
            return ::arrow::Status::OK();
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
