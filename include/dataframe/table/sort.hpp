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

#ifndef DATAFRAME_TABLE_SORT_HPP
#define DATAFRAME_TABLE_SORT_HPP

#include <dataframe/table/select.hpp>

namespace dataframe {

namespace internal {

class SortVisitor : public ::arrow::ArrayVisitor
{
  public:
    std::vector<std::int64_t> index;

    explicit SortVisitor(bool rev)
        : rev_(rev)
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

    ::arrow::Status Visit(const ::arrow::StringArray &array) override
    {
        if (array.null_count() != 0) {
            return ::arrow::Status::Invalid(
                "Cannot sort array with missing values");
        }

        auto n = array.length();

        index.reserve(static_cast<std::size_t>(n));
        for (std::int64_t i = 0; i != n; ++i) {
            index.push_back(i);
        }

        if (rev_) {
            std::sort(index.begin(), index.end(), [&](auto &&i1, auto &&i2) {
                return array.GetView(i1) > array.GetView(i2);
            });
        } else {
            std::sort(index.begin(), index.end(), [&](auto &&i1, auto &&i2) {
                return array.GetView(i1) < array.GetView(i2);
            });
        }

        return ::arrow::Status::OK();
    }

  private:
    template <typename ArrayType>
    ::arrow::Status visit(const ArrayType &array)
    {
        if (array.null_count() != 0) {
            return ::arrow::Status::Invalid(
                "Cannot sort array with missing values");
        }

        auto n = array.length();
        auto v = array.raw_values();

        index.reserve(static_cast<std::size_t>(n));
        for (std::int64_t i = 0; i != n; ++i) {
            index.push_back(i);
        }

        if (rev_) {
            std::sort(index.begin(), index.end(),
                [&](auto &&i1, auto &&i2) { return v[i1] > v[i2]; });
        } else {
            std::sort(index.begin(), index.end(),
                [&](auto &&i1, auto &&i2) { return v[i1] < v[i2]; });
        }

        return ::arrow::Status::OK();
    }

  private:
    bool rev_;
};

} // namespace internal

inline DataFrame sort(
    const DataFrame &df, const std::string &by, bool rev = false)
{
    if (!df[by]) {
        throw DataFrameException("Column " + by + " is not valid");
    }

    internal::SortVisitor visitor(rev);
    DF_ARROW_ERROR_HANDLER(df[by].data()->Accept(&visitor));

    if (std::is_sorted(visitor.index.begin(), visitor.index.end())) {
        return df;
    }

    return select(df, visitor.index.begin(), visitor.index.end());
}

} // namespace dataframe

#endif // DATAFRAME_TABLE_SORT_HPP
