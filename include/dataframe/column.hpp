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

namespace dataframe {

namespace internal {

template <typename T>
constexpr bool is_arrow_scalar =
    std::is_scalar_v<T> || std::is_constructible_v<std::string_view, T>;

}

/// \brief Constant proxy class of DataFrame column
class ConstColumnProxy
{
  public:
    ConstColumnProxy() = default;

    ConstColumnProxy(std::shared_ptr<::arrow::Array> values)
        : values_(std::move(values))
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

        values_ = chunks.front();
    }

    ConstColumnProxy(
        const std::string &name, const std::shared_ptr<::arrow::Table> &table)
    {
        if (table == nullptr) {
            return;
        }

        auto index = table->schema()->GetFieldIndex(name);
        if (index < 0) {
            return;
        }

        auto &chunks = table->column(index)->data()->chunks();
        if (chunks.size() != 1) {
            throw DataFrameException("Chunked array not supported");
        }

        values_ = chunks.front();
    }

    /// \brief Cast the column to a given destination type
    template <typename T>
    std::enable_if_t<!internal::is_arrow_scalar<T>, T> as() const
    {
        if (values_ == nullptr) {
            throw DataFrameException("Attempt to access an empty column");
        }

        T ret;
        cast_array(*values_, &ret);

        return ret;
    }

    /// \brief Cast the column to a vector of destination scalar type
    template <typename T>
    std::enable_if_t<internal::is_arrow_scalar<T>, std::vector<T>> as() const
    {
        return as<std::vector<T>>();
    }

    /// \brief Slice the column with close-open interval `[begin, end)`
    ConstColumnProxy operator()(std::size_t begin, std::size_t end) const
    {
        if (static_cast<std::int64_t>(end) > values_->length()) {
            throw DataFrameException("Slicing out of range");
        }

        if (end <= begin) {
            return ConstColumnProxy();
        }

        if (static_cast<std::int64_t>(begin) == values_->length()) {
            return ConstColumnProxy();
        }

        return ConstColumnProxy(values_->Slice(begin, end - begin));
    }

    /// \brief Same as `as<ArrayView<T>>()`
    template <typename T>
    ArrayView<T> view() const
    {
        return as<ArrayView<T>>();
    }

    template <typename T>
    bool is_ctype() const
    {
        switch (values_->type()->id()) {
            case ::arrow::Type::NA:
                return false;
            case ::arrow::Type::BOOL:
                return false;
            case ::arrow::Type::UINT8:
                return std::is_same_v<::arrow::UInt8Type::c_type, T>;
            case ::arrow::Type::INT8:
                return std::is_same_v<::arrow::Int8Type::c_type, T>;
            case ::arrow::Type::UINT16:
                return std::is_same_v<::arrow::UInt16Type::c_type, T>;
            case ::arrow::Type::INT16:
                return std::is_same_v<::arrow::Int16Type::c_type, T>;
            case ::arrow::Type::UINT32:
                return std::is_same_v<::arrow::UInt32Type::c_type, T>;
            case ::arrow::Type::INT32:
                return std::is_same_v<::arrow::Int32Type::c_type, T>;
            case ::arrow::Type::UINT64:
                return std::is_same_v<::arrow::UInt64Type::c_type, T>;
            case ::arrow::Type::INT64:
                return std::is_same_v<::arrow::Int64Type::c_type, T>;
            case ::arrow::Type::HALF_FLOAT:
                return false;
            case ::arrow::Type::FLOAT:
                return std::is_same_v<::arrow::FloatType::c_type, T>;
            case ::arrow::Type::DOUBLE:
                return std::is_same_v<::arrow::FloatType::c_type, T>;
            case ::arrow::Type::STRING:
                return false;
            case ::arrow::Type::BINARY:
                return false;
            case ::arrow::Type::FIXED_SIZE_BINARY:
                return false;
            case ::arrow::Type::DATE32:
                return std::is_same_v<::arrow::Date32Type::c_type, T>;
            case ::arrow::Type::DATE64:
                return std::is_same_v<::arrow::Date64Type::c_type, T>;
            case ::arrow::Type::TIMESTAMP:
                return std::is_same_v<::arrow::TimestampType::c_type, T>;
            case ::arrow::Type::TIME32:
                return std::is_same_v<::arrow::Time32Type::c_type, T>;
            case ::arrow::Type::TIME64:
                return std::is_same_v<::arrow::Time64Type::c_type, T>;
            case ::arrow::Type::INTERVAL:
                return false;
            case ::arrow::Type::DECIMAL:
                return false;
            case ::arrow::Type::LIST:
                return false;
            case ::arrow::Type::STRUCT:
                return false;
            case ::arrow::Type::UNION:
                return false;
            case ::arrow::Type::DICTIONARY:
                return false;
            case ::arrow::Type::MAP:
                return false;
        }
    }

    template <typename T>
    bool is_convertible() const
    {
        switch (values_->type()->id()) {
            case ::arrow::Type::NA:
                return false;
            case ::arrow::Type::BOOL:
                return false;
            case ::arrow::Type::UINT8:
                return std::is_constructible_v<T, ::arrow::UInt8Type::c_type>;
            case ::arrow::Type::INT8:
                return std::is_constructible_v<T, ::arrow::Int8Type::c_type>;
            case ::arrow::Type::UINT16:
                return std::is_constructible_v<T, ::arrow::UInt16Type::c_type>;
            case ::arrow::Type::INT16:
                return std::is_constructible_v<T, ::arrow::Int16Type::c_type>;
            case ::arrow::Type::UINT32:
                return std::is_constructible_v<T, ::arrow::UInt32Type::c_type>;
            case ::arrow::Type::INT32:
                return std::is_constructible_v<T, ::arrow::Int32Type::c_type>;
            case ::arrow::Type::UINT64:
                return std::is_constructible_v<T, ::arrow::UInt64Type::c_type>;
            case ::arrow::Type::INT64:
                return std::is_constructible_v<T, ::arrow::Int64Type::c_type>;
            case ::arrow::Type::HALF_FLOAT:
                return false;
            case ::arrow::Type::FLOAT:
                return std::is_constructible_v<T, ::arrow::FloatType::c_type>;
            case ::arrow::Type::DOUBLE:
                return std::is_constructible_v<T, ::arrow::FloatType::c_type>;
            case ::arrow::Type::STRING:
                return std::is_constructible_v<T, std::string_view>;
            case ::arrow::Type::BINARY:
                return std::is_constructible_v<T, std::string_view>;
            case ::arrow::Type::FIXED_SIZE_BINARY:
                return std::is_constructible_v<T, std::string_view>;
            case ::arrow::Type::DATE32:
                return std::is_constructible_v<T, ::arrow::Date32Type::c_type>;
            case ::arrow::Type::DATE64:
                return std::is_constructible_v<T, ::arrow::Date64Type::c_type>;
            case ::arrow::Type::TIMESTAMP:
                return std::is_constructible_v<T,
                    ::arrow::TimestampType::c_type>;
            case ::arrow::Type::TIME32:
                return std::is_constructible_v<T, ::arrow::Time32Type::c_type>;
            case ::arrow::Type::TIME64:
                return std::is_constructible_v<T, ::arrow::Time64Type::c_type>;
            case ::arrow::Type::INTERVAL:
                return false;
            case ::arrow::Type::DECIMAL:
                return false;
            case ::arrow::Type::LIST:
                return false;
            case ::arrow::Type::STRUCT:
                return false;
            case ::arrow::Type::UNION:
                return false;
            case ::arrow::Type::DICTIONARY:
                return false;
            case ::arrow::Type::MAP:
                return false;
        }
    }

    /// \brief Return the underlying Arrow array
    ///
    /// \note
    /// It may be empty the column haven't been assgined to
    const ::arrow::Array &array() const { return *values_; }

  protected:
    std::shared_ptr<::arrow::Array> values_;
}; // namespace dataframe

/// \brief Mutable proxy class of DataFrame column
class ColumnProxy : public ConstColumnProxy
{
  public:
    ColumnProxy(
        const std::string &name, std::shared_ptr<::arrow::Table> &table)
        : ConstColumnProxy(name, table)
        , name_(name)
        , table_(table)
    {
    }

    ColumnProxy &operator=(const char *v)
    {
        return operator=(std::string_view(v));
    }

    /// \brief Assign to the column, create the column if it does not exist yet
    template <typename T>
    std::enable_if_t<!internal::is_arrow_scalar<T>, ColumnProxy &> operator=(
        const T &v)
    {
        return operator=(make_array(v));
    }

    /// \brief Assign to the column with repeated value of a scalar
    template <typename T>
    std::enable_if_t<internal::is_arrow_scalar<T>, ColumnProxy &> operator=(
        const T &v)
    {
        if (table_ == nullptr || table_->num_columns() == 0) {
            throw DataFrameException(
                "Cannot assign scalar to an empty DataFrame");
        }

        std::vector<T> values(static_cast<std::size_t>(table_->num_rows()));
        std::fill_n(values.data(), values.size(), v);

        return operator=(values);
    }

    /// \brief Assign a pre-constructed Arrow array
    ColumnProxy &operator=(const std::shared_ptr<::arrow::Array> &values)
    {
        if (table_ != nullptr && table_->num_columns() != 0 &&
            table_->num_rows() != values->length()) {
            throw DataFrameException(
                "New column length is not the same as the old column");
        }

        values_ = values;
        auto fld = ::arrow::field(name_, values->type());
        auto col = std::make_shared<::arrow::Column>(fld, values);

        if (table_ == nullptr || table_->num_columns() == 0) {
            std::vector<std::shared_ptr<::arrow::Field>> fields = {fld};
            std::vector<std::shared_ptr<::arrow::Column>> columns = {col};
            table_ = ::arrow::Table::Make(
                std::make_shared<::arrow::Schema>(fields), columns);
            return *this;
        }

        auto index = table_->schema()->GetFieldIndex(name_);
        if (index >= 0) {
            DF_ARROW_ERROR_HANDLER(table_->SetColumn(index, col, &table_));
        } else {
            DF_ARROW_ERROR_HANDLER(
                table_->AddColumn(table_->num_columns(), col, &table_));
        }

        return *this;
    }

  private:
    std::string name_;
    std::shared_ptr<::arrow::Table> &table_;
};

} // namespace dataframe

#endif // DATAFRAME_COLUMN_HPP
