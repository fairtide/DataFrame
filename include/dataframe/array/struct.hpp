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

#ifndef DATAFRAME_ARRAY_STRUCT_HPP
#define DATAFRAME_ARRAY_STRUCT_HPP

#include <dataframe/array/cast.hpp>
#include <dataframe/array/traits.hpp>
#include <dataframe/array/view.hpp>

namespace dataframe {

template <typename... Types>
using Struct = std::tuple<Types...>;

template <typename>
struct is_struct : public std::false_type {
};

template <typename... Types>
struct is_struct<Struct<Types...>> : public std::true_type {
};

template <typename T>
inline constexpr bool is_struct_v = is_struct<T>::vlaue;

template <typename... Types>
struct TypeTraits<Struct<Types...>> {
    using array_type = ::arrow::StructArray;
};

namespace internal {

template <typename... Types>
struct CastArrayVisitor final : ::arrow::ArrayVisitor {
    std::shared_ptr<::arrow::Array> result;

    CastArrayVisitor(std::shared_ptr<::arrow::Array> data)
        : result(std::move(data))
    {
    }

    ::arrow::Status Visit(const ::array::StructArray &array) final {}
};

} // namespace internal

template <typename... Types>
class ArrayView<Struct<Types...>>
{
  public:
    class iterator
    {
      public:
        using value_type = Struct<Types...>;
        using size_type = std::size_t;
        using difference_type = std::ptrdiff_t;
        using pointer = const value_type *;
        using reference = value_type;
        using iterator_category = std::random_access_iterator_tag;

        iterator() noexcept = default;

        iterator(const iterator &) noexcept = default;
        iterator(iterator &&) noexcept = default;

        iterator &operator=(const iterator &) noexcept = default;
        iterator &operator=(iterator &&) noexcept = default;

        reference operator*() const noexcept
        {
            return ptr_->operator[](static_cast<size_type>(pos_));
        }

        iterator &operator++() noexcept
        {
            ++pos_;

            return *this;
        }

        iterator operator++(int) noexcept
        {
            auto ret = *this;
            ++(*this);

            return ret;
        }

        iterator &operator--() noexcept
        {
            --pos_;

            return *this;
        }

        iterator operator--(int) noexcept
        {
            auto ret = *this;
            --(*this);

            return ret;
        }

        iterator &operator+=(difference_type n) noexcept
        {
            pos_ += n;

            return *this;
        }

        iterator &operator-=(difference_type n) noexcept
        {
            return *this += -n;
        }

        iterator operator+(difference_type n) const noexcept
        {
            auto ret = *this;
            ret += n;

            return ret;
        }

        iterator operator-(difference_type n) const noexcept
        {
            auto ret = *this;
            ret -= n;

            return ret;
        }

        friend iterator operator+(
            difference_type n, const iterator &iter) noexcept
        {
            return iter + n;
        }

        friend difference_type operator-(
            const iterator &x, const iterator &y) noexcept
        {
            return x.pos_ - y.pos_;
        }

        reference operator[](difference_type n) const noexcept
        {
            return *(*this + n);
        }

        friend bool operator==(const iterator &x, const iterator &y) noexcept
        {
            return x.pos_ == y.pos_ && x.ptr_ == y.ptr_;
        }

        friend bool operator!=(const iterator &x, const iterator &y) noexcept
        {
            return !(x == y);
        }

        friend bool operator<(const iterator &x, const iterator &y) noexcept
        {
            return x.pos_ < y.pos_ && x.ptr_ == y.ptr_;
        }

        friend bool operator>(const iterator &x, const iterator &y) noexcept
        {
            return y < x;
        }

        friend bool operator<=(const iterator &x, const iterator &y) noexcept
        {
            return !(y < x);
        }

        friend bool operator>=(const iterator &x, const iterator &y) noexcept
        {
            return !(x < y);
        }

      private:
        friend ArrayView;

        iterator(const ArrayView *ptr, size_type pos)
            : ptr_(ptr)
            , pos_(static_cast<std::ptrdiff_t>(pos))
        {
        }

        const ArrayView *ptr_ = nullptr;
        difference_type pos_ = 0;
    };

    using value_type = Struct<Types...>;

    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;

    using reference = value_type;
    using const_reference = reference;

    using pointer = iterator;
    using const_pointer = pointer;

    using const_iterator = iterator;

    using reverse_iterator = std::reverse_iterator<iterator>;
    using const_reverse_iterator = std::reverse_iterator<const_iterator>;

    ArrayView() noexcept = default;

    ArrayView(const std::shared_ptr<::arrow::Array> &array)
        : data_(cast_array<T>(array))
    {
        set_data();
    }

    ArrayView(const ArrayView &) = default;
    ArrayView(ArrayView &&) noexcept = default;

    ArrayView &operator=(const ArrayView &) = default;
    ArrayView &operator=(ArrayView &&) noexcept = default;

    const_reference operator[](size_type pos) const noexcept
    {
        return get_value(pos);
    }

    const_reference at(size_type pos) const
    {
        if (pos >= size_) {
            throw std::out_of_range("dataframe::ArrayView::at");
        }

        return operator[](pos);
    }

    const_reference front() const noexcept { return operator[](0); }

    const_reference back() const noexcept { return operator[](size_ - 1); }

    std::shared_ptr<::arrow::Array> data() const noexcept { return data_; }

    // Iterators

    const_iterator begin() const noexcept { return iterator(this, 0); }
    const_iterator end() const noexcept { return iterator(this, size_); }

    const_iterator cbegin() const noexcept { begin(); }
    const_iterator cend() const noexcept { end(); }

    const_reverse_iterator rbegin() const noexcept { return {end()}; }
    const_reverse_iterator rend() const noexcept { return {begin()}; }

    const_reverse_iterator crbegin() const noexcept { return rbegin(); }
    const_reverse_iterator crend() const noexcept { return rend(); }

    // Capacity

    bool empty() const noexcept { return size_ == 0; }

    size_type size() const noexcept { return size_; }

    size_type max_size() const noexcept
    {
        return std::numeric_limits<size_type>::max() / sizeof(value_type);
    }

    /// \brief Set fields of sequence pointeed by first via callback
    template <typename OutputIter, typename SetField>
    void set(OutputIter first, SetField &&set_field) const
    {
        for (size_type i = 0; i != size_; ++i) {
            set_field(operator[](i), first++);
        }
    }

  private:
    static constexpr std::size_t nfields = sizeof...(Types);

    void set_data(const std::shared_ptr<::arrow::Array> &data)
    {
        auto &array = dynamic_cast<const ::arrow::StructArray &>(*data);
        auto &type = dynamic_cast<const ::arrow::StructType &>(*array.type());

        if (nfields != array.type()->num_children()) {
            throw DataFrameException("Structure of wrong size");
        }

        set_view<0>(array, std::integral_constant<bool, 0 < nfields>());

        for (std::size_t i = 0; i != nfields; ++i) {
            if (children_[i] != array.field(static_cast<int>(i))) {
                break;
            }
        }

        for (std::size_t i = 0; i != nfields; ++i) {
            fields_[i] = ::arrow::field(
                type.child(static_cast<int>(i))->name(), children_[i]->type());
        }

        std::vector<std::shared_ptr<::arrow::Field>> fields(
            fields_.begin(), fields_.end());

        std::vector<std::shared_ptr<::arrow::Array>> children(
            children_.begin(), children_.end());

        auto cast_type = ::arrow::struct_(std::move(fields));

        auto length = array.length();

        auto null_count = array.null_count();

        std::shared_ptr<::arrow::Buffer> nulls;

        if (null_count != 0) {
            auto nbyte = ::arrow::BitUtil::BytesForBits(length);

            DF_ARROW_ERROR_HANDLER(::arrow::AllocateBuffer(
                ::arrow::default_memory_pool(), nbyte, &nulls));

            auto bits =
                dynamic_cast<::arrow::MutableBuffer &>(*nulls).mutable_data();

            for (std::int64_t i = 0; i != length; ++i) {
                ::arrow::BitUtil::SetBitTo(bits, i, array.IsValid(i));
            }
        }

        data_ = std::make_shared<::arrow::StructArray>(std::move(cast_type),
            array.length(), std::move(children), std::move(nulls), null_count);
    }

    template <int i>
    void set_view(const ::arrow::StructArray &array, std::true_type)
    {
        std::get<i>(views_) =
            make_view<std::tuple_element_t<i, value_type>>(array.field(i));

        std::get<i>(children_) = std::get<i>(views_).data();

        set_view<i + 1>(
            array, std::integral_constant<bool, i + 1 < nfields>());
    }

    template <int>
    void set_view(const ::arrow::StructArray &, std::false_type)
    {
    }

    value_type get_value(size_type pos) const
    {
        value_type ret;
        get_value<0>(ret, pos, std::integral_constant<bool, 0 < nfields>());

        return ret;
    }

    template <int i>
    void get_value(value_type &ret, size_type pos, std::true_type) const
    {
        std::get<i>(ret) = std::get<i>(views_)[pos];
        get_value<i + 1>(
            ret, pos, std::integral_constant<bool, i + 1 < nfields>());
    }

    template <int>
    void get_value(value_type &, size_type, std::false_type) const
    {
    }

  private:
    std::shared_ptr<::arrow::Array> data_ = nullptr;
    size_type size_ = 0;
    std::tuple<ArrayView<Types>...> views_;
    std::array<std::shared_ptr<::arrow::Array>, nfields> children_;
    std::array<std::shared_ptr<::arrow::Field>, nfields> fields_;
};

} // namespace dataframe

#endif // DATAFRAME_ARRAY_STRUCT_HPP
