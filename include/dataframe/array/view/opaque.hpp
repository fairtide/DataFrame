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

#ifndef DATAFRAME_ARRAY_VIEW_OPAQUE_HPP
#define DATAFRAME_ARRAY_VIEW_OPAQUE_HPP

#include <dataframe/array/type.hpp>

namespace dataframe {

/// \brief Simple class that wrap the size and pointer of a raw array
template <typename T>
class ArrayView<Opaque<T>>
{
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

    ArrayView() = default;

    explicit ArrayView(std::shared_ptr<::arrow::Array> data)
        : data_(std::move(data))
        , size_(static_cast<size_type>(data_->length()))
        , begin_(reinterpret_cast<const T *>(
              dynamic_cast<const ArrayType<Opaque<T>> &>(*data_).raw_values()))
        , end_(begin_ + size_)
    {
        if (!is_type<Opaque<T>>(data_)) {
            throw DataFrameException("Mismatch type for view type");
        }
    }

    ArrayView(const ArrayView &) = default;
    ArrayView(ArrayView &&) noexcept = default;

    ArrayView &operator=(const ArrayView &) = default;
    ArrayView &operator=(ArrayView &&) noexcept = default;

    const_reference operator[](size_type pos) const noexcept
    {
        return begin_[pos];
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

    const_iterator begin() const noexcept { return begin_; }
    const_iterator end() const noexcept { return end_; }

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
            setter(operator[](i), out);
        }

        return out;
    }

  private:
    std::shared_ptr<::arrow::Array> data_;
    size_type size_ = 0;
    const T *begin_ = nullptr;
    const T *end_ = nullptr;
};

} // namespace dataframe

#endif // DATAFRAME_ARRAY_VIEW_OPAQUE_HPP
