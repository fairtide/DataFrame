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

#ifndef DATAFRAME_TABLE_DATA_VIEW_HPP
#define DATAFRAME_TABLE_DATA_VIEW_HPP

#include <dataframe/table/data_frame.hpp>

namespace dataframe {

template <typename Proxy>
class DataView
{
  public:
    class iterator
    {
      public:
        using value_type = typename Proxy::value_type;
        using reference = typename Proxy::reference;
        using difference_type = std::ptrdiff_t;
        using pointer = iterator;
        using iterator_category = std::random_access_iterator_tag;

        iterator() = default;

        iterator(const DataView *ptr, std::size_t pos) noexcept
            : ptr_(ptr)
            , pos_(static_cast<std::ptrdiff_t>(pos))
        {
        }

        reference operator*() const noexcept
        {
            return ptr_->proxy_[static_cast<std::size_t>(pos_)];
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
        const DataView *ptr_;
        std::ptrdiff_t pos_;
    };

    using value_type = typename Proxy::value_type;
    using reference = typename Proxy::reference;

    DataView(::dataframe::DataFrame data)
        : data_(std::move(data))
        , proxy_(data_)
    {
    }

    std::size_t size() const { return data_.nrow(); }

    const ::dataframe::DataFrame &data() const { return data_; }

    reference front() const { return proxy_[0]; }

    reference back() const { return proxy_[size() - 1]; }

    reference operator[](std::size_t i) const { return proxy_[i]; }

    reference at(std::size_t i) const
    {
        if (i >= size()) {
            throw std::out_of_range("ArrayView::at");
        }

        return data_[i];
    }

    iterator begin() const { return iterator(this, 0); }

    iterator end() const { return iterator(this, size()); }

  private:
    const ::dataframe::DataFrame data_;
    const Proxy proxy_;
};

template <typename Proxy>
inline DataView<Proxy> make_view(::dataframe::DataFrame data)
{
    return DataView<Proxy>(std::move(data));
}

} // namespace dataframe

#endif // DATAFRAME_TABLE_DATA_VIEW_HPP
