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

    DataFrame(DataFrame &&) = default;

    DataFrame &operator=(const DataFrame &) = delete;

    DataFrame &operator=(DataFrame &&) = delete;

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

  private:
    std::shared_ptr<::arrow::Table> table_;
};

} // namespace dataframe

#endif // DATAFRAME_DATAFRAME_HPP
