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

#ifndef DATAFRAME_ARRAY_TYPE_ITERATOR_HPP
#define DATAFRAME_ARRAY_TYPE_ITERATOR_HPP

#define DF_DEFINE_ITERATOR_MEMBERS(Iter, iter)                                \
    using difference_type = std::ptrdiff_t;                                   \
    using pointer = const value_type *;                                       \
                                                                              \
    Iter() noexcept = default;                                                \
                                                                              \
    Iter(const Iter &) noexcept = default;                                    \
                                                                              \
    Iter(Iter &&) noexcept = default;                                         \
                                                                              \
    Iter &operator=(const Iter &) noexcept = default;                         \
                                                                              \
    Iter &operator=(Iter &&) noexcept = default;                              \
                                                                              \
    Iter &operator++() noexcept                                               \
    {                                                                         \
        ++iter;                                                               \
                                                                              \
        return *this;                                                         \
    }                                                                         \
                                                                              \
    Iter operator++(int) noexcept                                             \
    {                                                                         \
        auto ret = *this;                                                     \
        ++(*this);                                                            \
                                                                              \
        return ret;                                                           \
    }                                                                         \
                                                                              \
    Iter &operator--() noexcept                                               \
    {                                                                         \
        --iter;                                                               \
                                                                              \
        return *this;                                                         \
    }                                                                         \
                                                                              \
    Iter operator--(int) noexcept                                             \
    {                                                                         \
        auto ret = *this;                                                     \
        --(*this);                                                            \
                                                                              \
        return ret;                                                           \
    }                                                                         \
                                                                              \
    Iter &operator+=(difference_type n) noexcept                              \
    {                                                                         \
        iter += n;                                                            \
                                                                              \
        return *this;                                                         \
    }                                                                         \
                                                                              \
    Iter &operator-=(difference_type n) noexcept { return *this += -n; }      \
                                                                              \
    Iter operator+(difference_type n) const noexcept                          \
    {                                                                         \
        auto ret = *this;                                                     \
        ret += n;                                                             \
                                                                              \
        return ret;                                                           \
    }                                                                         \
                                                                              \
    Iter operator-(difference_type n) const noexcept                          \
    {                                                                         \
        auto ret = *this;                                                     \
        ret -= n;                                                             \
                                                                              \
        return ret;                                                           \
    }                                                                         \
                                                                              \
    difference_type operator-(const Iter &other) noexcept                     \
    {                                                                         \
        return iter - other.iter;                                             \
    }                                                                         \
                                                                              \
    reference operator[](difference_type n) const noexcept                    \
    {                                                                         \
        return *(*this + n);                                                  \
    }                                                                         \
                                                                              \
    bool operator==(const Iter &other) noexcept                               \
    {                                                                         \
        return iter == other.iter;                                            \
    }                                                                         \
                                                                              \
    bool operator!=(const Iter &other) noexcept                               \
    {                                                                         \
        return iter != other.iter;                                            \
    }                                                                         \
                                                                              \
    bool operator<(const Iter &other) noexcept { return iter < other.iter; }  \
                                                                              \
    bool operator>(const Iter &other) noexcept { return iter > other.iter; }  \
                                                                              \
    bool operator<=(const Iter &other) noexcept                               \
    {                                                                         \
        return iter <= other.iter;                                            \
    }                                                                         \
                                                                              \
    bool operator>=(const Iter &other) noexcept { return iter >= other.iter; }

#endif // DATAFRAME_ARRAY_TYPE_ITERATOR_HPP
