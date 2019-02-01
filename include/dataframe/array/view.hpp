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

#include <dataframe/array/mask.hpp>
#include <dataframe/error.hpp>
#include <array>
#include <vector>

namespace dataframe {

/// \brief Simple class that wrap the size and pointer of a raw array
template <typename T>
class ArrayViewBase : public ArrayMask
{
  public:
    ArrayViewBase()
        : size_(0)
        , data_(nullptr)
    {
    }

    ArrayViewBase(std::size_t size, const T *data, std::vector<bool> mask = {})
        : ArrayMask(std::move(mask))
        , size_(size)
        , data_(data)
    {
    }

    template <typename Alloc>
    ArrayViewBase(
        const std::vector<T, Alloc> &values, std::vector<bool> mask = {})
        : ArrayMask(std::move(mask))
        , size_(values.size())
        , data_(values.data())
    {
    }

    template <std::size_t N>
    ArrayViewBase(const std::array<T, N> &values, std::vector<bool> mask = {})
        : ArrayMask(std::move(mask))
        , size_(values.size())
        , data_(values.data())
    {
    }

    std::size_t size() const { return size_; }

    const T *data() const { return data_; }

    const T &front() const { return data_[0]; }

    const T &back() const { return data_[size_ - 1]; }

    const T &operator[](std::size_t i) const { return data_[i]; }

    const T &at(std::size_t i) const
    {
        if (i >= size_) {
            throw std::out_of_range("ArrayView::at");
        }

        return data_[i];
    }

    /// \brief Set fields of sequence pointeed by first via callback
    template <typename OutputIter, typename SetField>
    void set(OutputIter first, SetField &&set_field) const
    {
        for (std::size_t i = 0; i != size_; ++i) {
            set_field(data_[i], first++);
        }
    }

  protected:
    void reset(std::size_t size, const T *data)
    {
        size_ = size;
        data_ = data;
    }

  private:
    std::size_t size_;
    const T *data_;
};

struct NullStorage;

template <typename T, typename Storage = NullStorage>
class ArrayView;

template <typename T>
class ArrayView<T, NullStorage> : public ArrayViewBase<T>
{
  public:
    using ArrayViewBase<T>::ArrayViewBase;
};

template <typename T, typename Storage>
class ArrayView : public ArrayViewBase<T>
{
  public:
    using ArrayViewBase<T>::ArrayViewBase;

    ArrayView(const ArrayView<T, NullStorage> &view)
        : ArrayViewBase<T>(view)
    {
    }

    ArrayView(std::unique_ptr<Storage> ptr)
        : ptr_(std::move(ptr))
    {
        this->reset(ptr_->size(), ptr_->data());
    }

    ArrayView(Storage &&data)
        : ptr_(std::make_unique<Storage>(std::move(data)))
    {
        this->reset(ptr_->size(), ptr_->data());
    }

    explicit operator bool() const { return ptr_ != nullptr; }

    const std::unique_ptr<Storage> &ptr() const
    {
        if (ptr_ == nullptr) {
            throw DataFrameException("Attempt to access non-owning view");
        }

        return ptr_;
    }

    std::unique_ptr<Storage> &ptr()
    {
        if (ptr_ == nullptr) {
            throw DataFrameException("Attempt to access non-owning view");
        }

        return ptr_;
    }

  private:
    std::unique_ptr<Storage> ptr_;
};

} // namespace dataframe

#endif // DATAFRAME_ARRAY_VIEW_HPP
