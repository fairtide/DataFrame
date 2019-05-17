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

#include <dataframe/table/data_frame.hpp>

namespace dataframe {

namespace internal {

template <typename T>
class SpliceVisitor : public ::arrow::ArrayVisitor
{
  public:
    SpliceVisitor(T minval, T maxval)
        : minval_(minval)
        , maxval_(maxval)
    {
    }

    ::arrow::Status Visit(const ::arrow::Int8Array &array) override
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::Int16Array &array) override
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::Int32Array &array) override
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::Int64Array &array) override
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::UInt8Array &array) override
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::UInt16Array &array) override
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::UInt32Array &array) override
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::UInt64Array &array) override
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::FloatArray &array) override
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::DoubleArray &array) override
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::Date32Array &array) override
    {
        return visit(array);
    }

    ::arrow::Status Visit(const ::arrow::TimestampArray &array) override
    {
        return visit(array);
    }

    std::size_t begin() const { return begin_; }
    std::size_t end() const { return end_; }

  private:
    template <typename ArrayType>
    ::arrow::Status visit(const ArrayType &array)
    {
        auto n = array.length();
        auto v = array.raw_values();

        using U = std::remove_cv_t<std::remove_reference_t<decltype(*v)>>;

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
            while (end < n && (v[end] < maxval || array.IsNull(end))) {
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

inline DataFrame splice(const DataFrame &df, const std::string &name,
    Datestamp<DateUnit::Day> mindate, Datestamp<DateUnit::Day> maxdate)
{
    if (!df[name].is_type<Datestamp<DateUnit::Day>>()) {
        throw DataFrameException(name + " is not a Date column");
    }

    return splice(df, name, mindate.value, maxdate.value);
}

inline DataFrame splice(const DataFrame &df, const std::string &name,
    Datestamp<DateUnit::Millisecond> mindate,
    Datestamp<DateUnit::Millisecond> maxdate)
{
    if (!df[name].is_type<Datestamp<DateUnit::Millisecond>>()) {
        throw DataFrameException(name + " is not a Date column");
    }

    return splice(df, name, mindate.value, maxdate.value);
}

template <TimeUnit Unit>
inline DataFrame splice(const DataFrame &df, const std::string &name,
    Timestamp<Unit> mintimestamp, Timestamp<Unit> maxtimestamp)
{
    if (!df[name].is_timestamp()) {
        throw DataFrameException(name + " is not a Timestamp column");
    }

    auto time_nanos = time_unit_nanos(Unit);

    auto data_nanos =
        time_unit_nanos(std::static_pointer_cast<::arrow::TimestampType>(
            df[name].data()->type())
                            ->unit());

    if (data_nanos > time_nanos) {
        // data has higher resolution
        auto ratio = data_nanos / time_nanos;
        return splice(
            df, name, mintimestamp.value * ratio, maxtimestamp.value * ratio);
    } else {
        // boundary has higher resolution
        auto ratio = time_nanos / data_nanos;
        return splice(
            df, name, mintimestamp.value / ratio, maxtimestamp.value / ratio);
    }
}

} // namespace dataframe

#endif // DATAFRAME_SPLICE_HPP
