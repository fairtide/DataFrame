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

#ifndef DATAFRAME_ARRAY_TYPE_LIST_HPP
#define DATAFRAME_ARRAY_TYPE_LIST_HPP

#include <dataframe/array/type/primitive.hpp>

namespace dataframe {

class ListBase
{
};

template <typename T, typename Iter = const T *>
class List final : public ListBase
{
  public:
    using value_type = T;

    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;

    using reference = typename std::iterator_traits<Iter>::reference;
    using const_reference = reference;

    using pointer = const value_type *;
    using const_pointer = pointer;

    using iterator = Iter;
    using const_iterator = iterator;

    using reverse_iterator = std::reverse_iterator<iterator>;
    using const_reverse_iterator = reverse_iterator;

    List() noexcept = default;

    List(size_type size, iterator iter) noexcept
        : size_(size)
        , iter_(iter)
    {
    }

    List(iterator begin, iterator end) noexcept
        : size_(static_cast<size_type>(std::distance(begin, end)))
        , iter_(begin)
    {
    }

    List(const List &) = default;
    List(List &&) noexcept = default;

    List &operator=(const List &) = default;
    List &operator=(List &&) noexcept = default;

    const_reference operator[](size_type pos) const noexcept
    {
        return iter_[pos];
    }

    const_reference at(size_type pos) const
    {
        if (pos >= size_) {
            throw std::out_of_range("dataframe::List::at");
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
bool operator==(const List<T> &v1, const List<T> &v2)
{
    return std::equal(v1.begin(), v1.end(), v2.begin(), v2.end());
}

template <typename T>
bool operator!=(const List<T> &v1, const List<T> &v2)
{
    return !(v1 == v2);
}

template <typename T>
struct TypeTraits<List<T>> {
    static std::shared_ptr<::arrow::ListType> data_type()
    {
        auto ret = ::arrow::list(make_data_type<T>());

        return std::static_pointer_cast<::arrow::ListType>(ret);
    }

    static std::unique_ptr<::arrow::ListBuilder> builder()
    {
        return std::make_unique<::arrow::ListBuilder>(
            ::arrow::default_memory_pool(), make_builder<T>(), data_type());
    }

    using array_type = ::arrow::ListArray;
};

} // namespace dataframe

#endif // DATAFRAME_ARRAY_TYPE_LIST_HPP
