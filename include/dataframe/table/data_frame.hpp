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

#ifndef DATAFRAME_TABLE_DATA_FRAME_HPP
#define DATAFRAME_TABLE_DATA_FRAME_HPP

#include <dataframe/table/column.hpp>

namespace dataframe {

/// \brief DataFrame in C++
class DataFrame
{
  public:
    using size_type = std::size_t;

    DataFrame() = default;
    DataFrame(DataFrame &&) noexcept = default;
    DataFrame &operator=(DataFrame &&) noexcept = default;

    DataFrame(const DataFrame &other)
    {
        auto ncol = other.ncol();
        for (size_type i = 0; i != ncol; ++i) {
            auto col = other[i];
            operator[](col.name()) = col;
        }
    }

    DataFrame &operator=(const DataFrame &other)
    {
        if (this != &other) {
            clear();
            auto ncol = other.ncol();
            for (size_type i = 0; i != ncol; ++i) {
                auto col = other[i];
                operator[](col.name()) = col;
            }
        }

        return *this;
    }

    explicit DataFrame(std::shared_ptr<::arrow::Table> &&table)
        : table_(std::move(table))
    {
    }

    explicit DataFrame(const ::arrow::Table &table)
    {
        auto nrows = table.num_rows();
        std::vector<std::shared_ptr<::arrow::Column>> columns;
        std::vector<std::shared_ptr<::arrow::Field>> fields;
        columns.reserve(static_cast<size_type>(table.num_columns()));
        fields.reserve(static_cast<size_type>(table.num_columns()));
        for (auto i = 0; i != table.num_columns(); ++i) {
            auto col = table.column(i);
            columns.push_back(col);
            fields.push_back(col->field());
        }
        table_ = ::arrow::Table::Make(
            std::make_shared<::arrow::Schema>(fields), columns, nrows);
    }

    ConstColumnProxy operator[](const std::string &name) const
    {
        return ConstColumnProxy(name, table_);
    }

    ColumnProxy operator[](std::string name)
    {
        return ColumnProxy(std::move(name), table_);
    }

    ConstColumnProxy operator[](size_type j) const
    {
        return ConstColumnProxy(table_->column(static_cast<int>(j)));
    }

    ColumnProxy operator[](size_type j)
    {
        return ColumnProxy(
            table_->column(static_cast<int>(j))->name(), table_);
    }

    ConstColumnProxy at(size_type j) const
    {
        if (j >= ncol()) {
            throw std::out_of_range("DataFrame::at");
        }

        return operator[](j);
    }

    ColumnProxy at(size_type j)
    {
        if (j >= ncol()) {
            throw std::out_of_range("DataFrame::at");
        }

        return operator[](j);
    }

    const ::arrow::Table &table() const { return *table_; }

    size_type nrow() const
    {
        return table_ == nullptr ? static_cast<size_type>(0) :
                                   static_cast<size_type>(table_->num_rows());
    }

    size_type ncol() const
    {
        return table_ == nullptr ?
            static_cast<size_type>(0) :
            static_cast<size_type>(table_->num_columns());
    }

    size_type memory_usage() const
    {
        size_type ret = 0;
        auto n = ncol();
        for (size_type i = 0; i != n; ++i) {
            ret += operator[](i).memory_usage();
        }

        return ret;
    }

    void clear() { table_.reset(); }

    bool empty() const { return nrow() * ncol() == 0; }

    explicit operator bool() const { return table_ != nullptr; }

    /// \brief Select a range of rows
    DataFrame rows(size_type begin, size_type end) const
    {
        if (begin >= end) {
            return DataFrame();
        }

        DataFrame ret;
        for (size_type k = 0; k != ncol(); ++k) {
            auto col = operator[](k);
            ret[col.name()] = col(begin, end);
        }

        return ret;
    }

    /// \brief Select a range of columns
    DataFrame cols(size_type begin, size_type end) const
    {
        if (begin >= end) {
            return DataFrame();
        }

        DataFrame ret;
        for (size_type k = begin; k != end; ++k) {
            auto col = operator[](k);
            ret[col.name()] = col;
        }

        return ret;
    }

  private:
    std::shared_ptr<::arrow::Table> table_;
};

inline bool operator==(const DataFrame &df1, const DataFrame &df2)
{
    if (df1.nrow() != df2.nrow()) {
        return false;
    }

    if (df1.ncol() != df2.ncol()) {
        return false;
    }

    for (std::size_t i = 0; i != df1.ncol(); ++i) {
        auto col1 = df1[i];
        auto col2 = df2[col1.name()];
        if (!col2) {
            return false;
        }
        if (col1 != col2) {
            return false;
        }
    }

    return true;
}

inline bool operator!=(const DataFrame &df1, const DataFrame &df2)
{
    return !(df1 == df2);
}

} // namespace dataframe

#endif // DATAFRAME_TABLE_DATA_FRAME_HPP
