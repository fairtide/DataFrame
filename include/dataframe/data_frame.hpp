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

#define DF_FIELD_PAIR(T, field)                                               \
    std::pair { #field, &T::field }

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

    explicit DataFrame(std::shared_ptr<::arrow::Table> &&table)
        : table_(std::move(table))
    {
    }

    explicit DataFrame(const ::arrow::Table &table)
    {
        auto nrows = table.num_rows();
        std::vector<std::shared_ptr<::arrow::Column>> columns;
        std::vector<std::shared_ptr<::arrow::Field>> fields;
        columns.reserve(static_cast<std::size_t>(table.num_columns()));
        fields.reserve(static_cast<std::size_t>(table.num_columns()));
        for (auto i = 0; i != table.num_columns(); ++i) {
            auto col = table.column(i);
            columns.push_back(col);
            fields.push_back(col->field());
        }
        table_ = ::arrow::Table::Make(
            std::make_shared<::arrow::Schema>(fields), columns, nrows);
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

    const ::arrow::Table &table() const { return *table_; }

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

namespace internal {

template <typename InputIter>
inline void insert_pair(DataFrame &, InputIter, InputIter)
{
}

template <typename InputIter, typename K, typename V>
inline std::enable_if_t<!std::is_member_pointer_v<V> &&
    std::is_invocable_v<V,
        typename std::iterator_traits<InputIter>::reference>>
insert_pair1(
    DataFrame &ret, InputIter first, InputIter last, std::pair<K, V> kv)
{
    ret[std::move(kv.first)].emplace(
        first, last, [&](auto &&v) { return kv.second(v); });
}

template <typename InputIter, typename K, typename V>
inline std::enable_if_t<std::is_member_function_pointer_v<V>> insert_pair1(
    DataFrame &ret, InputIter first, InputIter last, std::pair<K, V> kv)
{
    ret[std::move(kv.first)].emplace(
        first, last, [&](auto &&v) { return (v.*kv.second)(); });
}

template <typename InputIter, typename K, typename V>
inline std::enable_if_t<std::is_member_object_pointer_v<V>> insert_pair1(
    DataFrame &ret, InputIter first, InputIter last, std::pair<K, V> kv)
{
    ret[std::move(kv.first)].emplace(
        first, last, [&](auto &&v) { return v.*kv.second; });
}

template <typename InputIter, typename K, typename V, typename... Args>
inline void insert_pair(DataFrame &ret, InputIter first, InputIter last,
    std::pair<K, V> kv, Args &&... args)
{
    insert_pair1(ret, first, last, std::move(kv));
    internal::insert_pair(ret, first, last, std::forward<Args>(args)...);
}

} // namespace internal

/// \brief Make a DataFrame from class objects, given member access functions,
/// pointers, or callback
template <typename InputIter, typename K, typename V, typename... Args>
inline DataFrame make_dataframe(
    InputIter first, InputIter last, std::pair<K, V> kv, Args &&... args)
{
    DataFrame ret;
    internal::insert_pair(
        ret, first, last, std::move(kv), std::forward<Args>(args)...);
    return ret;
}

/// \brief Make a DataFrame from class objects, given member access functions,
/// pointers, or callback
template <typename T, typename K, typename V, typename... Args>
inline DataFrame make_dataframe(
    std::size_t n, const T *data, std::pair<K, V> kv, Args &&... args)
{
    return make_dataframe(
        data, data + n, std::move(kv), std::forward<Args>(args)...);
}

/// \brief Make a DataFrame from class objects, given member access functions,
/// pointers, or callback
template <typename T, typename Alloc, typename K, typename V, typename... Args>
inline DataFrame make_dataframe(
    const std::vector<T, Alloc> &data, std::pair<K, V> kv, Args &&... args)
{
    return make_dataframe(
        data.begin(), data.end(), std::move(kv), std::forward<Args>(args)...);
}

namespace internal {

template <typename T>
inline void cast_pair(const DataFrame &, T *)
{
}

template <typename T, typename K, typename V>
inline std::enable_if_t<std::is_member_object_pointer_v<V>> cast_pair1(
    const DataFrame &df, T *ret, std::pair<K, V> kv)
{
    using U =
        std::remove_cv_t<std::remove_reference_t<decltype(ret->*kv.second)>>;

    df[std::move(kv.first)].template as_view<U>().set(
        ret, [&](auto &&v, auto *data) {
            data->*kv.second = std::forward<decltype(v)>(v);
        });
}

template <typename T, typename K, typename V>
inline std::enable_if_t<std::is_member_function_pointer_v<V>> cast_pair1(
    const DataFrame &df, T *ret, std::pair<K, V> kv)
{
    using U = std::remove_cv_t<
        std::remove_reference_t<decltype((ret->*kv.second)())>>;

    df[std::move(kv.first)].template as_view<U>().set(
        ret, [&](auto &&v, auto *data) {
            (data->*kv.second)() = std::forward<decltype(v)>(v);
        });
}

template <typename T, typename K, typename V, typename... Args>
inline void cast_pair(
    const DataFrame &df, T *data, std::pair<K, V> kv, Args &&... args)
{
    cast_pair1(df, data, std::move(kv));
    cast_pair(df, data, std::forward<Args>(args)...);
}

} // namespace internal

/// \brief Cast a DataFrame to a vector of class objects, given member access
/// functions, pointers, or callback
template <typename T, typename Alloc, typename... Args>
inline void cast_dataframe(
    const DataFrame &df, std::vector<T, Alloc> *ret, Args &&... args)
{
    ret->resize(df.nrow());
    internal::cast_pair(df, ret->data(), std::forward<Args>(args)...);
}

} // namespace dataframe

#endif // DATAFRAME_DATA_FRAME_HPP
