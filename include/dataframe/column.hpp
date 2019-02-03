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

#ifndef DATAFRAME_COLUMN_HPP
#define DATAFRAME_COLUMN_HPP

#include <dataframe/array.hpp>
#include <dataframe/traits.hpp>

namespace dataframe {

/// \brief Constant proxy class of DataFrame column
class ConstColumnProxy
{
  public:
    ConstColumnProxy() = default;

    ConstColumnProxy(std::string name, std::shared_ptr<::arrow::Array> data)
        : name_(std::move(name))
        , data_(std::move(data))
    {
    }

    ConstColumnProxy(const std::shared_ptr<::arrow::Column> &column)
    {
        if (column == nullptr) {
            return;
        }

        auto &chunks = column->data()->chunks();
        if (chunks.size() != 1) {
            throw DataFrameException("Chunked array not supported");
        }

        name_ = column->name();
        data_ = chunks.front();
    }

    ConstColumnProxy(
        std::string name, const std::shared_ptr<::arrow::Table> &table)
        : name_(std::move(name))
    {
        if (table == nullptr) {
            return;
        }

        auto index = table->schema()->GetFieldIndex(name_);
        if (index < 0) {
            return;
        }

        auto &chunks =
            table->column(static_cast<int>(index))->data()->chunks();
        if (chunks.size() != 1) {
            throw DataFrameException("Chunked array not supported");
        }

        data_ = chunks.front();
    }

    explicit operator bool() const { return data_ != nullptr; }

    /// \brief Cast the column to a given destination type
    template <typename T>
    std::enable_if_t<!is_scalar(static_cast<T *>(nullptr)), T> as() const
    {
        if (data_ == nullptr) {
            throw DataFrameException(
                "Attempt to access an empty column '" + name_ + "'");
        }

        T ret;
        cast_array(*data_, &ret);

        return ret;
    }

    /// \brief Cast the column to a vector of destination scalar type
    template <typename T>
    std::enable_if_t<is_scalar(static_cast<T *>(nullptr)), std::vector<T>>
    as() const
    {
        return as<std::vector<T>>();
    }

    /// \brief Slice the column with close-open interval `[begin, end)`
    ConstColumnProxy operator()(std::size_t begin, std::size_t end) const
    {
        if (static_cast<std::int64_t>(end) > data_->length()) {
            throw DataFrameException("Slicing out of range");
        }

        if (end <= begin) {
            return ConstColumnProxy();
        }

        if (static_cast<std::int64_t>(begin) == data_->length()) {
            return ConstColumnProxy();
        }

        return ConstColumnProxy(name_,
            data_->Slice(static_cast<std::int64_t>(begin),
                static_cast<std::int64_t>(end - begin)));
    }

    /// \brief Same as `as<ArrayView<T>>()`
    template <typename T>
    ArrayView<T> view() const
    {
        ArrayView<T> ret;
        view_array(*data_, &ret);

        if (data_->null_count() == 0) {
            return ret;
        } else {
            return ArrayView<T>(ret.size(), ret.data(), mask());
        }
    }

    template <typename T, typename Storage>
    ArrayView<T, Storage> as_view() const
    {
        if (is_ctype<T>()) {
            return ArrayView<T, Storage>{view<T>()};
        } else {
            return ArrayView<T, Storage>{as<Storage>()};
        }
    }

    template <typename T>
    ArrayView<T, std::vector<T>> as_view() const
    {
        return as_view<T, std::vector<T>>();
    }

    const std::string &name() const { return name_; }

    const std::shared_ptr<::arrow::Array> &data() const { return data_; }

    std::vector<bool> mask() const
    {
        auto n = static_cast<std::size_t>(data_->length());
        auto v = data_->null_bitmap_data();
        std::vector<bool> ret(n);
        if (data_->null_count() == 0 || v == nullptr) {
            std::fill(ret.begin(), ret.end(), true);
        } else {
            for (std::size_t i = 0; i != n; ++i) {
                ret[i] = v[i / 8] & (1 << (i % 8));
            }
        }

        return ret;
    }

    template <typename T>
    bool is_ctype() const
    {
        if (data_ == nullptr) {
            return false;
        }

        return ::dataframe::is_ctype(
            data_->type()->id(), static_cast<T *>(nullptr));
    }

    template <typename T>
    bool is_convertible() const
    {
        if (data_ == nullptr) {
            return false;
        }

        return ::dataframe::is_convertible(
            data_->type()->id(), static_cast<T *>(nullptr));
    }

    template <typename T>
    operator T() const
    {
        return as<T>();
    }

    /// \brief Return the underlying Arrow array
    ///
    /// \note
    /// It may be empty the column haven't been assgined to
    const ::arrow::Array &array() const { return *data_; }

  protected:
    std::string name_;
    std::shared_ptr<::arrow::Array> data_;
};

/// \brief Mutable proxy class of DataFrame column
class ColumnProxy : public ConstColumnProxy
{
  public:
    ColumnProxy(std::string name, std::shared_ptr<::arrow::Table> &table)
        : ConstColumnProxy(name, table)
        , table_(table)
    {
    }

    template <typename... Args>
    void emplace(Args &&... args)
    {
        operator=(make_array(std::forward<Args>(args)...));
    }

    ColumnProxy &operator=(const char *v)
    {
        return operator=(std::string_view(v));
    }

    /// \brief Assign to the column, create the column if it does not exist yet
    template <typename T>
    std::enable_if_t<!is_scalar(static_cast<T *>(nullptr)), ColumnProxy &>
    operator=(const T &v)
    {
        return operator=(make_array(v));
    }

    /// \brief Assign to the column with repeated value of a scalar
    template <typename T>
    std::enable_if_t<is_scalar(static_cast<T *>(nullptr)), ColumnProxy &>
    operator=(const T &v)
    {
        if (table_ == nullptr || table_->num_columns() == 0) {
            throw DataFrameException(
                "Cannot assign scalar to an empty DataFrame");
        }

        std::vector<T> data(static_cast<std::size_t>(table_->num_rows()));
        std::fill_n(data.data(), data.size(), v);

        return operator=(data);
    }

    /// \brief Assign a pre-constructed Arrow array
    ColumnProxy &operator=(const std::shared_ptr<::arrow::Array> &data)
    {
        if (table_ != nullptr && table_->num_columns() != 0 &&
            table_->num_rows() != data->length()) {
            throw DataFrameException(
                "New column length is not the same as the old column");
        }

        data_ = data;
        auto fld = ::arrow::field(name_, data->type());
        auto col = std::make_shared<::arrow::Column>(fld, data);

        if (table_ == nullptr || table_->num_columns() == 0) {
            std::vector<std::shared_ptr<::arrow::Field>> fields = {fld};
            std::vector<std::shared_ptr<::arrow::Column>> columns = {col};
            table_ = ::arrow::Table::Make(
                std::make_shared<::arrow::Schema>(fields), columns);
            return *this;
        }

        auto index = table_->schema()->GetFieldIndex(name_);
        if (index >= 0) {
            DF_ARROW_ERROR_HANDLER(
                table_->SetColumn(static_cast<int>(index), col, &table_));
        } else {
            DF_ARROW_ERROR_HANDLER(
                table_->AddColumn(table_->num_columns(), col, &table_));
        }

        return *this;
    }

  private:
    std::shared_ptr<::arrow::Table> &table_;
};

} // namespace dataframe

#endif // DATAFRAME_COLUMN_HPP
