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

#ifndef DATAFRAME_SPLICE_HPP
#define DATAFRAME_SPLICE_HPP

#include <dataframe/data_frame.hpp>

namespace dataframe {

namespace internal {

template <typename T>
class SpliceVisitor final : public ::arrow::ArrayVisitor
{
  public:
    SpliceVisitor(T minval, T maxval)
        : minval_(minval)
        , maxval_(maxval)
    {
    }

    ::arrow::Status Visit(const ::arrow::Int8Array &array) final
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::Int16Array &array) final
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::Int32Array &array) final
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::Int64Array &array) final
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::UInt8Array &array) final
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::UInt16Array &array) final
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::UInt32Array &array) final
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::UInt64Array &array) final
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::FloatArray &array) final
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::DoubleArray &array) final
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::Date32Array &array) final
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::TimestampArray &array) final
    {
        return visit(array);
    }

  private:
    template <typename ArrayType>
    ::arrow::Status visit(const ArrayType &array)
    {
        auto n = array.length();
        auto v = array.raw_values();

        using U = std::remove_cv_t<std::remove_reference_t>(decltype(*v));

        auto minval = static_cast<U>(minval_);
        auto maxval = static_cast<U>(maxval_);

        std::int64_t begin = 0;
        if (array.null_count() == 0) {
            while (begin < n && v[begin] < minval) {
                ++begin;
            }
        } else {
            while (begin < n && (v[begin] < minval || array.IsNull(begin))) {
                ++begin;
            }
        }

        std::int64_t end = begin;
        if (array.null_count() == 0) {
            while (end < n && v[end] < maxval) {
                ++end;
            }
        } else {
            while (end < n && (v[end] < maxval || array.IsNull(end)) {
                ++end;
            }
        }

        begin_ = static_cast<std::size_t>(begin);
        end_ = static_cast<std::size_t>(end);

        return ::arrow::Status::OK();
    }

  private:
    T minval_;
    T maxval_;
    std::size_t begin_;
    std::size_t end_;
};

} // namespace internal

template <typename T>
inline std::enable_if_t<std::is_arithmetic_v<T>, DataFrame> splice(
    const DataFrame &df, const std::string &name, T minval, T maxval)
{
    auto col = df[name];
    if (!col) {
        throw DataFrameException(name + " is not an existing column");
    }

    internal::SpliceVisitor<T> visitor(minval, maxval);
    DF_ARROW_ERROR_HANDLER(col.data()->Accept(&visitor));

    return df.rows(visitor.begin(), visitor.end());
}

inline DataFrame splice(
    const DataFrame &df, const std::string &name, Date mindate, Date maxdate)
{
    if (df[name].dtype() != DataType::Date) {
        throw DataFrameException(name + " is not a Date column");
    }

    Date epoch(1970, 1, 1);

    return splice(
        df, name, (mindate - epoch).days(), (maxdate - epoch).days());
}

inline DataFrame splice(const DataFrame &df, const std::string &name,
    Timestamp mintimestamp, Timestamp maxtimestamp)
{
    if (df[name].dtype() != DataType::Timestamp) {
        throw DataFrameException(name + " is not a Timestamp column");
    }

    Timestamp epoch(Date(1970, 1, 1));

    auto type = std::static_pointer_cast<::arrow::TimestampType>(
        df[name].data()->type());

    switch (type->unit()) {
        case ::arrow::TimeUnit::SECOND:
            return splice(df, name, (mintimestamp - epoch).total_seconds(),
                (maxtimestamp - epoch).total_seconds());
        case ::arrow::TimeUnit::MILLI:
            return splice(df, name,
                (mintimestamp - epoch).total_milliseconds(),
                (maxtimestamp - epoch).total_milliseconds());
        case ::arrow::TimeUnit::MICRO:
            return splice(df, name,
                (mintimestamp - epoch).total_microseconds(),
                (maxtimestamp - epoch).total_microseconds());
        case ::arrow::TimeUnit::NANO:
            return splice(df, name, (mintimestamp - epoch).total_nanoseconds(),
                (maxtimestamp - epoch).total_nanoseconds());
    }
}

} // namespace dataframe

#endif // DATAFRAME_SPLICE_HPP
