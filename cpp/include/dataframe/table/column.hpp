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

#ifndef DATAFRAME_TABLE_COLUMN_HPP
#define DATAFRAME_TABLE_COLUMN_HPP

#include <dataframe/array.hpp>

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

    template <typename T>
    operator ArrayView<T>() const
    {
        return as<T>();
    }

    /// \brief Cast the column to a given destination type
    template <typename T>
    ArrayView<T> as(
        ::arrow::MemoryPool *pool = ::arrow::default_memory_pool()) const
    {
        if (data_ == nullptr) {
            throw DataFrameException(
                "Attempt to access an empty column '" + name_ + "'");
        }

        return make_view<T>(cast_array<T>(data_, pool));
    }

    /// \brief Same as `as` but will throw if not aready of the destination
    /// type
    template <typename T>
    ArrayView<T> view() const
    {
        if (data_ == nullptr) {
            throw DataFrameException(
                "Attempt to access an empty column '" + name_ + "'");
        }

        return make_view<T>(data_);
    }

    template <typename T>
    bool is_type() const
    {
        if (data_ == nullptr) {
            throw DataFrameException(
                "Attempt to access an empty column '" + name_ + "'");
        }

        return ::dataframe::is_type<T>(data_);
    }

    bool is_integer() const
    {
        return is_type<std::int8_t>() || is_type<std::int16_t>() ||
            is_type<std::int32_t>() || is_type<std::int64_t>() ||
            is_type<std::uint8_t>() || is_type<std::uint16_t>() ||
            is_type<std::uint32_t>() || is_type<std::uint64_t>();
    }

    bool is_real() const { return is_type<float>() || is_type<double>(); }

    bool is_binary() const { return is_type<std::string>(); }

    bool is_timestamp() const
    {
        return is_type<Timestamp<TimeUnit::Second>>() ||
            is_type<Timestamp<TimeUnit::Millisecond>>() ||
            is_type<Timestamp<TimeUnit::Microsecond>>() ||
            is_type<Timestamp<TimeUnit::Nanosecond>>();
    }

    bool is_time() const
    {
        return is_type<Time<TimeUnit::Second>>() ||
            is_type<Time<TimeUnit::Millisecond>>() ||
            is_type<Time<TimeUnit::Microsecond>>() ||
            is_type<Time<TimeUnit::Nanosecond>>();
    }

    /// \brief Slice the column with close-open interval `[begin, end)`
    ConstColumnProxy operator()(std::size_t begin, std::size_t end) const
    {
        if (static_cast<std::int64_t>(end) > data_->length()) {
            throw DataFrameException("Slicing out of range, begin: " +
                std::to_string(begin) + ", end: " + std::to_string(end) +
                ", length: " + std::to_string(data_->length()));
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

    const std::string &name() const { return name_; }

    const std::shared_ptr<::arrow::Array> &data() const { return data_; }

    // attributes

    std::size_t size() const
    {
        if (data_ == nullptr) {
            return 0;
        }

        return static_cast<std::size_t>(data_->length());
    }

    // type information

    std::size_t memory_usage() const
    {
        return data_ == nullptr ? 0 : memory_usage(*data_->data());
    }

  protected:
    std::string name_;
    std::shared_ptr<::arrow::Array> data_;

  private:
    static std::size_t memory_usage(const ::arrow::ArrayData &data)
    {
        std::size_t ret = 0;

        for (auto &buf : data.buffers) {
            if (buf != nullptr) {
                ret += static_cast<std::size_t>(buf->size());
            }
        }

        for (auto &child : data.child_data) {
            if (child != nullptr) {
                ret += memory_usage(*child);
            }
        }

        return ret;
    }
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

    if (col1.data() == col2.data()) {
        return true;
    }

    return col1.data()->Equals(col2.data());
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

    template <typename T, typename... Args>
    void emplace(Args &&... args)
    {
        operator=(make_array<T>(std::forward<Args>(args)...));
    }

    template <typename... Args>
    void emplace(Args &&... args)
    {
        operator=(make_array(std::forward<Args>(args)...));
    }

    /// \brief Assign another column
    ColumnProxy &operator=(ConstColumnProxy col)
    {
        return operator=(col.data());
    }

    template <typename T, typename Alloc>
    ColumnProxy &operator=(const std::vector<T, Alloc> &vec)
    {
        return operator=(make_array<T>(vec));
    }

    template <typename T>
    ColumnProxy &operator=(Repeat<T> rep)
    {
        if (table_ == nullptr) {
            if (rep.size() > 0) {
                return operator=(make_array<T>(rep.begin(), rep.end()));
            } else {
                throw DataFrameException(
                    "Cannot assign a empty Repeat as the first column");
            }
        } else {
            rep = repeat(
                rep.front(), static_cast<std::size_t>(table_->num_rows()));
            return operator=(make_array<T>(rep.begin(), rep.end()));
        }
    }

    /// \brief Assign a pre-constructed Arrow array
    ColumnProxy &operator=(std::shared_ptr<::arrow::Array> data)
    {
        if (data == nullptr) {
            throw DataFrameException(
                "Cannot assign a null array to column " + name_);
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

#endif // DATAFRAME_TABLE_COLUMN_HPP
