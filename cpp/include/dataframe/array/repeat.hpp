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

#ifndef DATAFRAME_ARRAY_REPEAT_HPP
#define DATAFRAME_ARRAY_REPEAT_HPP

#include <dataframe/array/type.hpp>

namespace dataframe {

template <typename T>
class Repeat
{
  public:
    using value_type = T;

    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;

    using reference = const value_type &;
    using const_reference = reference;

    class iterator
    {
      public:
        using value_type = T;
        using reference = const value_type &;
        using iterator_category = std::random_access_iterator_tag;

        reference operator*() const noexcept { return *ptr_; }

        DF_DEFINE_ITERATOR_MEMBERS(iterator, pos_)

      private:
        friend Repeat;

        iterator(pointer ptr, size_type pos)
            : ptr_(ptr)
            , pos_(static_cast<std::ptrdiff_t>(pos))
        {
        }

        pointer ptr_;
        difference_type pos_;
    };

    using pointer = iterator;
    using const_pointer = pointer;

    using const_iterator = iterator;

    using reverse_iterator = std::reverse_iterator<iterator>;
    using const_reverse_iterator = std::reverse_iterator<const_iterator>;

    explicit Repeat(const value_type &value, size_type size = 0)
        : value_(value)
        , size_(size)
    {
    }

    Repeat(const Repeat &) = default;
    Repeat(Repeat &&) noexcept = default;

    Repeat &operator=(const Repeat &) = default;
    Repeat &operator=(Repeat &&) noexcept = default;

    const_reference operator[](size_type) const noexcept { return value_; }

    const_reference at(size_type pos) const
    {
        if (pos >= size_) {
            throw std::out_of_range("dataframe::Repeat::at");
        }

        return operator[](pos);
    }

    const_reference front() const noexcept { return value_; }

    const_reference back() const noexcept { return value_; }

    // Iterators

    const_iterator begin() const noexcept { return iterator(&value_, 0); }
    const_iterator end() const noexcept { return iterator(&value_, size_); }

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

    template <typename OutputIter, typename Setter>
    OutputIter set(OutputIter out, Setter &&setter)
    {
        for (std::size_t i = 0; i != size_; ++i, ++out) {
            setter(value_, out);
        }

        return out;
    }

  private:
    value_type value_;
    size_type size_;
};

template <typename T>
inline Repeat<T> repeat(const T &value, std::size_t size = 0)
{
    return Repeat<T>(value, size);
}

} // namespace dataframe

#endif // DATAFRAME_ARRAY_REPEAT_HPP
