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

#ifndef DATAFRAME_ARRAY_VIEW_HPP
#define DATAFRAME_ARRAY_VIEW_HPP

#include <dataframe/array/cast.hpp>
#include <dataframe/array/traits.hpp>
#include <iterator>

namespace dataframe {

/// \brief Simple class that wrap the size and pointer of a raw array
template <typename T>
class ArrayView
{
    static_assert(
        sizeof(T) == sizeof(typename ArrayType<T>::TypeClass::c_type));

  public:
    using value_type = T;

    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;

    using reference = const T &;
    using const_reference = reference;

    using pointer = const T *;
    using const_pointer = pointer;

    using iterator = pointer;
    using const_iterator = iterator;

    using reverse_iterator = std::reverse_iterator<iterator>;
    using const_reverse_iterator = reverse_iterator;

    ArrayView() noexcept
        : array_(nullptr)
        , size_(0)
        , data_(nullptr)
    {
    }

    ArrayView(const std::shared_ptr<::arrow::Array> &array)
        : array_(cast_array<T>(array))
        , size_(static_cast<size_type>(array_->length()))
        , data_(reinterpret_cast<const T *>(
              dynamic_cast<const ArrayType<T> &>(*array_).raw_values()))
    {
    }

    ArrayView(std::shared_ptr<ArrayType<T>> array)
        : array_(std::move(array))
        , size_(static_cast<size_type>(array_->length()))
        , data_(reinterpret_cast<const T *>(
              dynamic_cast<const ArrayType<T> &>(*array_).raw_values()))
    {
    }

    ArrayView(const ArrayView &) = default;
    ArrayView(ArrayView &&) noexcept = default;

    ArrayView &operator=(const ArrayView &) = default;
    ArrayView &operator=(ArrayView &&) noexcept = default;

    const_reference operator[](size_type pos) const noexcept
    {
        return data_[pos];
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

    std::shared_ptr<::arrow::Array> data() const { return array_; }

    // Iterators

    const_iterator begin() const noexcept { return data_; }
    const_iterator end() const noexcept { return data_ + size_; }

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
    std::shared_ptr<::arrow::Array> array_;
    size_type size_;
    const T *data_;
};

template <typename T, typename Array>
inline ArrayView<T> make_view(std::shared_ptr<Array> array)
{
    return ArrayView<T>(std::move(array));
}

} // namespace dataframe

#endif // DATAFRAME_ARRAY_VIEW_HPP
