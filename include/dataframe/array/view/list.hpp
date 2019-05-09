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

#ifndef DATAFRAME_ARRAY_VIEW_LIST_HPP
#define DATAFRAME_ARRAY_VIEW_LIST_HPP

#include <dataframe/array/view/primitive.hpp>

namespace dataframe {

template <typename T>
class ListView final : public ListBase
{
  public:
    using value_type = typename ArrayView<T>::value_type;

    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;

    using reference = typename ArrayView<T>::reference;
    using const_reference = typename ArrayView<T>::const_reference;

    using pointer = typename ArrayView<T>::pointer;
    using const_pointer = typename ArrayView<T>::const_pointer;

    using iterator = typename ArrayView<T>::iterator;
    using const_iterator = typename ArrayView<T>::const_iterator;

    using reverse_iterator = std::reverse_iterator<iterator>;
    using const_reverse_iterator = reverse_iterator;

    ListView() noexcept = default;

    ListView(size_type size, iterator iter) noexcept
        : size_(size)
        , iter_(iter)
    {
    }

    ListView(iterator begin, iterator end) noexcept
        : size_(static_cast<size_type>(std::distance(begin, end)))
        , iter_(begin)
    {
    }

    ListView(const ListView &) = default;
    ListView(ListView &&) noexcept = default;

    ListView &operator=(const ListView &) = default;
    ListView &operator=(ListView &&) noexcept = default;

    const_reference operator[](size_type pos) const noexcept
    {
        return iter_[pos];
    }

    const_reference at(size_type pos) const
    {
        if (pos >= size_) {
            throw std::out_of_range("dataframe::ListView::at");
        }

        return operator[](pos);
    }

    const_reference front() const noexcept { return operator[](0); }

    const_reference back() const noexcept { return operator[](size_ - 1); }

    // Iterators

    const_iterator begin() const noexcept { return iter_; }

    const_iterator end() const noexcept
    {
        return iter_ + static_cast<difference_type>(size_);
    }

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

  private:
    size_type size_ = 0;
    iterator iter_;
};

template <typename T>
class ArrayView<ListView<T>>
{
  public:
    class iterator
    {
      public:
        using value_type = ListView<T>;
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

        const ArrayView *ptr_;
        difference_type pos_;
    };

    using value_type = ListView<T>;

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
        , size_(static_cast<size_type>(data_->length()))
        , view_(make_view<T>(
              dynamic_cast<const ::arrow::ListArray &>(*data_).values()))
        , offsets_(dynamic_cast<const ::arrow::ListArray &>(*data_)
                       .raw_value_offsets())
    {
    }

    ArrayView(const ArrayView &) = default;
    ArrayView(ArrayView &&) noexcept = default;

    ArrayView &operator=(const ArrayView &) = default;
    ArrayView &operator=(ArrayView &&) noexcept = default;

    const_reference operator[](size_type pos) const noexcept
    {
        return ListView<T>(
            view_.begin() + offsets_[pos], view_.begin() + offsets_[pos + 1]);
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
    std::shared_ptr<::arrow::Array> data_;
    size_type size_;
    ArrayView<T> view_;
    const std::int32_t *offsets_;
};

} // namespace dataframe

#endif // DATAFRAME_ARRAY_VIEW_LIST_HPP
