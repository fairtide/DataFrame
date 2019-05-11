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

#ifndef DATAFRAME_ARRAY_MAKE_DATETIME_HPP
#define DATAFRAME_ARRAY_MAKE_DATETIME_HPP

#include <dataframe/array/make/primitive.hpp>

namespace dataframe {

namespace internal {

template <typename T>
struct DatetimeArrayMaker {
    template <typename Iter>
    static std::shared_ptr<::arrow::Array> make(Iter first, Iter last)
    {
        auto builder = make_builder<T>();

        constexpr bool is_time_type = std::is_base_of_v<TimeTypeBase,
            typename std::iterator_traits<Iter>::value_type>;

        if constexpr (is_time_type) {

            struct iterator {
                using value_type = typename T::value_type;
                using difference_type = std::ptrdiff_t;
                using pointer = const value_type *;
                using reference = const value_type &;
                using iterator_category =
                    typename std::iterator_traits<Iter>::iterator_category;

                iterator(Iter i)
                    : iter(i)
                {
                }

                Iter iter;

                reference operator*() const { return iter->value; }

                iterator &operator++() noexcept
                {
                    ++iter;

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
                    --iter;

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
                    iter += n;

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

                difference_type operator-(const iterator &other) noexcept
                {
                    return iter - other.iter;
                }

                reference operator[](difference_type n) const noexcept
                {
                    return *(*this + n);
                }

                bool operator==(const iterator &other) noexcept
                {
                    return iter == other.iter;
                }

                bool operator!=(const iterator &other) noexcept
                {
                    return iter != other.iter;
                }

                bool operator<(const iterator &other) noexcept
                {
                    return iter < other.iter;
                }

                bool operator>(const iterator &other) noexcept
                {
                    return iter > other.iter;
                }

                bool operator<=(const iterator &other) noexcept
                {
                    return iter <= other.iter;
                }

                bool operator>=(const iterator &other) noexcept
                {
                    return iter >= other.iter;
                }
            };

            DF_ARROW_ERROR_HANDLER(
                builder->AppendValues(iterator(first), iterator(last)));
        } else {
            DF_ARROW_ERROR_HANDLER(builder->AppendValues(first, last));
        }

        std::shared_ptr<::arrow::Array> ret;
        DF_ARROW_ERROR_HANDLER(builder->Finish(&ret));

        return ret;
    }
};

} // namespace internal

template <DateUnit Unit>
struct ArrayMaker<Datestamp<Unit>>
    : internal::DatetimeArrayMaker<Datestamp<Unit>> {
};

template <TimeUnit Unit>
struct ArrayMaker<Timestamp<Unit>>
    : internal::DatetimeArrayMaker<Timestamp<Unit>> {
};

template <TimeUnit Unit>
struct ArrayMaker<Time<Unit>> : internal::DatetimeArrayMaker<Time<Unit>> {
};

} // namespace dataframe

#endif // DATAFRAME_ARRAY_MAKE_DATETIME_HPP
