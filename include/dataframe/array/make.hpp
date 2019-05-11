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

#ifndef DATAFRAME_ARRAY_MAKE_HPP
#define DATAFRAME_ARRAY_MAKE_HPP

#include <dataframe/array/make/datetime.hpp>
#include <dataframe/array/make/list.hpp>
#include <dataframe/array/make/primitive.hpp>
#include <dataframe/array/make/string.hpp>
#include <dataframe/array/make/struct.hpp>

namespace dataframe {

template <typename T, typename Container>
inline std::shared_ptr<::arrow::Array> make_array(Container &&container)
{
    return make_array<T>(std::begin(container), std::end(container));
}

template <typename T, typename Container, typename Alloc>
inline std::shared_ptr<::arrow::Array> make_array(
    Container &&container, const std::vector<bool, Alloc> &valids)
{
    return make_array<T>(
        std::begin(container), std::end(container), valids.begin());
}

template <typename InputIter>
inline std::shared_ptr<::arrow::Array> make_array(
    InputIter first, InputIter last)
{
    using T = typename std::iterator_traits<InputIter>::value_type;

    return make_array<T>(first, last);
}

template <typename InputIter, typename Getter>
inline std::shared_ptr<::arrow::Array> make_array(
    InputIter first, InputIter last, Getter &&getter)
{
    using T =
        std::remove_cv_t<internal::UnwrapReference<decltype(getter(*first))>>;

    return make_array<T>(first, last, std::forward<Getter>(getter));
}

template <typename T, typename... Args>
inline std::shared_ptr<::arrow::Array> make_array(
    std::size_t n, const T *data, Args &&... args)
{
    return make_array(data, data + n, std::forward<Args>(args)...);
}

template <typename T, typename U, typename... Args>
inline std::shared_ptr<::arrow::Array> make_array(
    std::size_t n, const U *data, Args &&... args)
{
    return make_array<T>(data, data + n, std::forward<Args>(args)...);
}

} // namespace dataframe

#endif // DATAFRAME_ARRAY_MAKE_HPP
