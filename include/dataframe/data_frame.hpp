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

#ifndef DATAFRAME_DATA_FRAME_HPP
#define DATAFRAME_DATA_FRAME_HPP

#include <dataframe/column.hpp>

namespace dataframe {

/// \brief DataFrame in C++
class DataFrame
{
  public:
    DataFrame() = default;

    DataFrame(DataFrame &&) noexcept = default;

    DataFrame &operator=(DataFrame &&) noexcept = default;

    DataFrame(const DataFrame &other)
    {
        auto ncol = other.ncol();
        for (std::size_t i = 0; i != ncol; ++i) {
            auto col = other[i];
            operator[](col.name()) = col;
        }
    }

    DataFrame &operator=(const DataFrame &other)
    {
        if (this != &other) {
            clear();
            auto ncol = other.ncol();
            for (std::size_t i = 0; i != ncol; ++i) {
                auto col = other[i];
                operator[](col.name()) = col;
            }
        }

        return *this;
    }

    explicit DataFrame(std::shared_ptr<::arrow::Table> table)
        : table_(std::move(table))
    {
    }

    template <typename K, typename V, typename... Args>
    DataFrame(std::pair<K, V> kv, Args &&... args)
    {
        insert_pair(std::move(kv), std::forward<Args>(args)...);
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

    /// \brief Select a range of rows
    DataFrame rows(std::size_t begin, std::size_t end) const
    {
        DataFrame ret;
        for (std::size_t k = 0; k != ncol(); ++k) {
            auto col = operator[](k);
            ret[col.name()] = col(begin, end);
        }

        return ret;
    }

    /// \brief Select a range of columns
    DataFrame cols(std::size_t begin, std::size_t end) const
    {
        if (begin > end) {
            return DataFrame();
        }

        DataFrame ret;
        for (std::size_t k = begin; k != end; ++k) {
            auto col = operator[](k);
            ret[col.name()] = col;
        }

        return ret;
    }

  private:
    template <typename K, typename V, typename... Args>
    void insert_pair(std::pair<K, V> kv, Args &&... args)
    {
        operator[](std::move(kv.first)) = std::move(kv.second);
        insert_pair(std::forward<Args>(args)...);
    }

    void insert_pair() {}

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

#endif // DATAFRAME_DATA_FRAME_HPP
