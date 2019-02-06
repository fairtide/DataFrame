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

#ifndef DATAFRAME_DATAFRAME_HPP
#define DATAFRAME_DATAFRAME_HPP

#include <dataframe/column.hpp>

namespace dataframe {

/// \brief DataFrame in C++
class DataFrame
{
  public:
    DataFrame() = default;

    DataFrame(const DataFrame &) = delete;

    DataFrame(DataFrame &&) noexcept = default;

    DataFrame &operator=(const DataFrame &) = delete;

    DataFrame &operator=(DataFrame &&) noexcept = default;

    DataFrame(std::shared_ptr<::arrow::Table> table)
        : table_(std::move(table))
    {
    }

    ConstColumnProxy operator[](const std::string &name) const
    {
        return ConstColumnProxy(name, table_);
    }

    ColumnProxy operator[](std::string name)
    {
        return ColumnProxy(std::move(name), table_);
    }

    ConstColumnProxy operator[](std::size_t j) const
    {
        return ConstColumnProxy(table_->column(static_cast<int>(j)));
    }

    ColumnProxy operator[](std::size_t j)
    {
        return ColumnProxy(
            table_->column(static_cast<int>(j))->name(), table_);
    }

    const std::shared_ptr<::arrow::Table> &table() const { return table_; }

    std::size_t nrow() const
    {
        return table_ == nullptr ?
            static_cast<std::size_t>(0) :
            static_cast<std::size_t>(table_->num_rows());
    }

    std::size_t ncol() const
    {
        return table_ == nullptr ?
            static_cast<std::size_t>(0) :
            static_cast<std::size_t>(table_->num_columns());
    }

    void clear() { table_.reset(); }

    bool empty() const { return nrow() * ncol() == 0; }

    explicit operator bool() const { return table_ != nullptr; }

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

} // namespace dataframe

#endif // DATAFRAME_DATAFRAME_HPP
