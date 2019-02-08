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
#include <dataframe/data_type.hpp>

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

    ArrayMask mask() const
    {
        if (data_->null_count() == 0) {
            return ArrayMask();
        }

        return ArrayMask(static_cast<std::size_t>(data_->length()),
            data_->null_bitmap_data());
    }

    // attributes

    std::size_t size() const
    {
        if (data_ == nullptr) {
            return 0;
        }

        return static_cast<std::size_t>(data_->length());
    }

    // type information

    DataType dtype() const
    {
        if (data_ == nullptr) {
            return DataType::Unknown;
        }

        return ::dataframe::dtype(data_->type()->id());
    }

    template <typename T>
    bool is_ctype() const
    {
        switch (dtype()) {
            case DataType::Unknown:
                return false;

            case DataType::Bool:
                return false;

            case DataType::UInt8:
                return std::is_same_v<T, std::uint8_t>;
            case DataType::Int8:
                return std::is_same_v<T, std::int8_t>;
            case DataType::UInt16:
                return std::is_same_v<T, std::uint16_t>;
            case DataType::Int16:
                return std::is_same_v<T, std::int16_t>;
            case DataType::UInt32:
                return std::is_same_v<T, std::uint32_t>;
            case DataType::Int32:
                return std::is_same_v<T, std::int32_t>;
            case DataType::UInt64:
                return std::is_same_v<T, std::uint64_t>;
            case DataType::Int64:
                return std::is_same_v<T, std::int64_t>;
            case DataType::Date:
                return std::is_same_v<T, std::int32_t>;
            case DataType::Timestamp:
                return std::is_same_v<T, std::int64_t>;

            case DataType::Float:
                return std::is_same_v<T, float>;
            case DataType::Double:
                return std::is_same_v<T, double>;

            case DataType::String:
                return false;
            case DataType::Categorical:
                return false;
        }
    }

    template <typename T>
    bool is_convertible() const
    {
        switch (dtype()) {
            case DataType::Unknown:
                return false;

            case DataType::Bool:
                return std::is_assignable_v<T &, bool>;

            case DataType::UInt8:
                return std::is_assignable_v<T &, std::uint8_t>;
            case DataType::Int8:
                return std::is_assignable_v<T &, std::int8_t>;
            case DataType::UInt16:
                return std::is_assignable_v<T &, std::uint16_t>;
            case DataType::Int16:
                return std::is_assignable_v<T &, std::int16_t>;
            case DataType::UInt32:
                return std::is_assignable_v<T &, std::uint32_t>;
            case DataType::Int32:
                return std::is_assignable_v<T &, std::int32_t>;
            case DataType::UInt64:
                return std::is_assignable_v<T &, std::uint64_t>;
            case DataType::Int64:
                return std::is_assignable_v<T &, std::int64_t>;
            case DataType::Date:
                return std::is_assignable_v<T &, std::int32_t> ||
                    std::is_same_v<T, Date>;
            case DataType::Timestamp:
                return std::is_assignable_v<T &, std::int64_t> ||
                    std::is_same_v<T, Timestamp>;

            case DataType::Float:
                return std::is_assignable_v<T &, float>;
            case DataType::Double:
                return std::is_assignable_v<T &, double>;

            case DataType::String:
                return std::is_constructible_v<T, std::string_view>;
            case DataType::Categorical:
                return std::is_constructible_v<T, std::string_view> ||
                    std::is_same_v<T, CategoricalArray>;
        }
    }

    bool is_unknown() const { return dtype() == DataType::Unknown; }
    bool is_bool() const { return dtype() == DataType::Bool; }
    bool is_uint8() const { return dtype() == DataType::UInt8; }
    bool is_int8() const { return dtype() == DataType::Int8; }
    bool is_uint16() const { return dtype() == DataType::UInt16; }
    bool is_int16() const { return dtype() == DataType::Int16; }
    bool is_uint32() const { return dtype() == DataType::UInt32; }
    bool is_int32() const { return dtype() == DataType::Int32; }
    bool is_uint64() const { return dtype() == DataType::UInt64; }
    bool is_int64() const { return dtype() == DataType::Int64; }
    bool is_float() const { return dtype() == DataType::Float; }
    bool is_double() const { return dtype() == DataType::Double; }
    bool is_string() const { return dtype() == DataType::String; }
    bool is_date() const { return dtype() == DataType::Date; }
    bool is_timestamp() const { return dtype() == DataType::Timestamp; }
    bool is_categorical() const { return dtype() == DataType::Categorical; }

    bool is_integer() const { return static_cast<bool>(dtype() & Integer); }
    bool is_real() const { return static_cast<bool>(dtype() & Real); }
    bool is_binary() const { return static_cast<bool>(dtype() & Binary); }

  protected:
    std::string name_;
    std::shared_ptr<::arrow::Array> data_;
};

inline bool operator==(
    const ConstColumnProxy &col1, const ConstColumnProxy &col2)
{
    if (col1.name() != col2.name()) {
        return false;
    }

    if (col1.size() != col2.size()) {
        return false;
    }

    if (col1.dtype() != col2.dtype()) {
        return false;
    }

    switch (col1.dtype()) {
        case DataType::Bool:
            return col1.as<bool>() == col2.as<bool>();
        case DataType::UInt8:
            return col1.as_view<std::uint8_t>() ==
                col2.as_view<std::uint8_t>();
        case DataType::Int8:
            return col1.as_view<std::int8_t>() == col2.as_view<std::int8_t>();
        case DataType::UInt16:
            return col1.as_view<std::uint16_t>() ==
                col2.as_view<std::uint16_t>();
        case DataType::Int16:
            return col1.as_view<std::int16_t>() ==
                col2.as_view<std::int16_t>();
        case DataType::UInt32:
            return col1.as_view<std::uint32_t>() ==
                col2.as_view<std::uint32_t>();
        case DataType::Int32:
            return col1.as_view<std::int32_t>() ==
                col2.as_view<std::int32_t>();
        case DataType::UInt64:
            return col1.as_view<std::uint64_t>() ==
                col2.as_view<std::uint64_t>();
        case DataType::Int64:
            return col1.as_view<std::int64_t>() ==
                col2.as_view<std::int64_t>();
        case DataType::Float:
            return col1.as_view<float>() == col2.as_view<float>();
        case DataType::Double:
            return col1.as_view<double>() == col2.as_view<double>();
        case DataType::String:
            return col1.as<std::string_view>() == col2.as<std::string_view>();
        case DataType::Date:
            return col1.as<Date>() == col2.as<Date>();
        case DataType::Timestamp:
            return col1.as<Timestamp>() == col2.as<Timestamp>();
        case DataType::Categorical:
            return col1.as<std::string_view>() == col2.as<std::string_view>();
        case DataType::Unknown:
            throw DataFrameException("Unknown dtype cannot be compared");
    }

    return true;
}

inline bool operator!=(
    const ConstColumnProxy &col1, const ConstColumnProxy &col2)
{
    return !(col1 == col2);
}

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
    std::enable_if_t<
        !is_scalar(static_cast<std::remove_cv_t<std::remove_reference_t<T>> *>(
            nullptr)),
        ColumnProxy &>
    operator=(T &&v)
    {
        return operator=(make_array(std::forward<T>(v)));
    }

    /// \brief Assign to the column with repeated value of a scalar
    template <typename T>
    std::enable_if_t<
        is_scalar(static_cast<std::remove_cv_t<std::remove_reference_t<T>> *>(
            nullptr)),
        ColumnProxy &>
    operator=(T &&v)
    {
        std::size_t nrow = 1;
        if (table_ != nullptr && table_->num_columns() != 0) {
            nrow = static_cast<std::size_t>(table_->num_rows());
        }

        std::vector<T> data(nrow);
        std::fill_n(data.data(), data.size(), v);

        return operator=(std::move(data));
    }

    /// \brief Assign another column
    ColumnProxy &operator=(ConstColumnProxy col)
    {
        return operator=(col.data());
    }

    /// \brief Assign a pre-constructed Arrow array
    ColumnProxy &operator=(std::shared_ptr<::arrow::Array> data)
    {
        if (data == nullptr) {
            throw DataFrameException("Cannot assign a null array");
        }

        if (table_ != nullptr && table_->num_columns() != 0 &&
            table_->num_rows() != data->length()) {
            throw DataFrameException("Length of new column " + name_ + " (" +
                std::to_string(data->length()) +
                ") is not the same as the old columns (" +
                std::to_string(table_->num_rows()) + ")");
        }

        data_ = std::move(data);
        auto fld = ::arrow::field(name_, data_->type());
        auto col = std::make_shared<::arrow::Column>(fld, data_);

        if (table_ == nullptr || table_->num_columns() == 0) {
            std::vector<std::shared_ptr<::arrow::Field>> fields = {fld};
            std::vector<std::shared_ptr<::arrow::Column>> columns = {col};
            table_ = ::arrow::Table::Make(
                std::make_shared<::arrow::Schema>(fields), columns);
            return *this;
        }

        auto index = static_cast<int>(table_->schema()->GetFieldIndex(name_));
        if (index >= 0) {
            DF_ARROW_ERROR_HANDLER(table_->SetColumn(index, col, &table_));
        } else {
            DF_ARROW_ERROR_HANDLER(
                table_->AddColumn(table_->num_columns(), col, &table_));
        }

        return *this;
    }

    void rename(const std::string &name)
    {
        if (table_ == nullptr) {
            throw DataFrameException("Empty DataFrame");
        }

        auto index = static_cast<int>(table_->schema()->GetFieldIndex(name_));
        if (index < 0) {
            throw DataFrameException("Column does not exist");
        }

        auto fld = ::arrow::field(name, data_->type());
        auto col = std::make_shared<::arrow::Column>(fld, data_);
        DF_ARROW_ERROR_HANDLER(table_->SetColumn(index, col, &table_));
    }

    void remove()
    {
        if (table_ == nullptr) {
            return;
        }

        auto index = static_cast<int>(table_->schema()->GetFieldIndex(name_));
        if (index < 0) {
            return;
        }

        DF_ARROW_ERROR_HANDLER(table_->RemoveColumn(index, &table_));
    }

  private:
    std::shared_ptr<::arrow::Table> &table_;
};

} // namespace dataframe

#endif // DATAFRAME_COLUMN_HPP
