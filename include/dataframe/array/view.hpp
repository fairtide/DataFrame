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
#include <array>
#include <vector>

namespace dataframe {

/// \brief Simple class that wrap the size and pointer of a raw array
template <typename T>
class ArrayView : public ArrayMask
{
  public:
    ArrayView()
        : size_(0)
        , data_(nullptr)
    {
    }

    ArrayView(std::size_t size, const T *data, std::vector<bool> mask = {})
        : ArrayMask(std::move(mask))
        , size_(size)
        , data_(data)
    {
    }

    template <typename Alloc>
    ArrayView(const std::vector<T, Alloc> &values, std::vector<bool> mask = {})
        : ArrayMask(std::move(mask))
        , size_(values.size())
        , data_(values.data())
    {
    }

    template <std::size_t N>
    ArrayView(const std::array<T, N> &values, std::vector<bool> mask = {})
        : ArrayMask(std::move(mask))
        , size_(values.size())
        , data_(values.data())
    {
    }

    std::size_t size() const { return size_; }

    const T *data() const { return data_; }

    const T &front() { return data_[0]; }

    const T &back() { return data_[size_ - 1]; }

    const T &operator[](std::size_t i) { return data_[i]; }

  private:
    std::size_t size_;
    const T *data_;
};

template <typename T, typename Alloc>
std::shared_ptr<::arrow::Array> make_array(const std::vector<T, Alloc> &vec)
{
    return make_array(ArrayView<T>(vec));
}

template <typename T, std::size_t N>
std::shared_ptr<::arrow::Array> make_array(const std::array<T, N> &vec)
{
    return make_array(ArrayView<T>(vec));
}

} // namespace dataframe

#endif // DATAFRAME_ARRAY_VIEW_HPP
