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

#ifndef DATAFRAME_ARRAY_VIEW_STRUCT_HPP
#define DATAFRAME_ARRAY_VIEW_STRUCT_HPP

#include <dataframe/array/view/primitive.hpp>

namespace dataframe {

template <typename... Types>
class StructView : public StructBase
{
  public:
    using value_type = std::tuple<Types...>;

    StructView(std::size_t pos, const std::tuple<ArrayView<Types>...> &views)
        : pos_(pos)
        , views_(views)
    {
    }

    template <std::size_t N>
    typename ArrayView<std::tuple_element_t<N, value_type>>::const_reference
    get() const
    {
        return std::get<N>(views_)[pos_];
    }

    value_type get() const
    {
        value_type ret;
        get_value<0>(ret, std::integral_constant<bool, 0 < nfields>());

        return ret;
    }

  private:
    template <std::size_t N>
    void get_value(value_type &ret, std::true_type) const
    {
        std::get<N>(ret) = get<N>();
        get_value<N + 1>(ret, std::integral_constant<bool, N + 1 < nfields>());
    }

    template <std::size_t>
    void get_value(value_type &, std::false_type) const
    {
    }

  private:
    static constexpr std::size_t nfields = sizeof...(Types);

    std::size_t pos_;
    const std::tuple<ArrayView<Types>...> &views_;
};

template <typename... Types>
class ArrayView<StructView<Types...>>
{
  public:
    class iterator
    {
      public:
        using value_type = StructView<Types...>;
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

    using value_type = StructView<Types...>;

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

    ArrayView(std::shared_ptr<::arrow::Array> data)
        : data_(std::move(data))
    {
        set_data();
    }

    ArrayView(const ArrayView &) = default;
    ArrayView(ArrayView &&) noexcept = default;

    ArrayView &operator=(const ArrayView &) = default;
    ArrayView &operator=(ArrayView &&) noexcept = default;

    const_reference operator[](size_type pos) const noexcept
    {
        return StructView<Types...>(pos, views_);
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

    const_iterator cbegin() const noexcept { return begin(); }
    const_iterator cend() const noexcept { return end(); }

    const_reverse_iterator rbegin() const noexcept
    {
        return const_reverse_iterator{end()};
    }

    const_reverse_iterator rend() const noexcept
    {
        return const_reverse_iterator{begin()};
    }

    const_reverse_iterator crbegin() const noexcept { return rbegin(); }
    const_reverse_iterator crend() const noexcept { return rend(); }

    // Capacity

    bool empty() const noexcept { return size_ == 0; }

    size_type size() const noexcept { return size_; }

    size_type max_size() const noexcept
    {
        return std::numeric_limits<size_type>::max() / sizeof(value_type);
    }

  private:
    void set_data()
    {
        auto &array = dynamic_cast<const ::arrow::StructArray &>(*data_);
        auto &type = dynamic_cast<const ::arrow::StructType &>(*array.type());

        size_ = static_cast<size_type>(data_->length());

        if (nfields != type.num_children()) {
            throw DataFrameException("Structure of wrong size");
        }

        set_data<0>(array, type, std::integral_constant<bool, 0 < nfields>());
    }

    template <std::size_t N>
    void set_data(const ::arrow::StructArray &array,
        const ::arrow::StructType &type, std::true_type)
    {
        std::get<N>(views_) =
            make_view<std::tuple_element_t<N, std::tuple<Types...>>>(
                array.field(N));

        std::get<N>(names_) = std::string_view(type.child(N)->name());

        set_data<N + 1>(
            array, type, std::integral_constant<bool, N + 1 < nfields>());
    }

    template <std::size_t>
    void set_data(const ::arrow::StructArray &, const ::arrow::StructType &,
        std::false_type)
    {
    }

  private:
    static constexpr std::size_t nfields = sizeof...(Types);

    std::shared_ptr<::arrow::Array> data_;
    size_type size_ = 0;
    std::tuple<ArrayView<Types>...> views_;
    std::array<std::string_view, nfields> names_;
};

} // namespace dataframe

#endif // DATAFRAME_ARRAY_VIEW_STRUCT_HPP
