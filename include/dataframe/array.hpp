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

#ifndef DATAFRAME_ARRYAY_HPP
#define DATAFRAME_ARRYAY_HPP

#include <dataframe/array/categorical.hpp>
#include <dataframe/array/primitive.hpp>
#include <dataframe/array/string.hpp>
#include <dataframe/array/view.hpp>

namespace dataframe {

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

template <typename InputIter, typename GetField>
inline std::shared_ptr<::arrow::Array> make_array(
    InputIter first, InputIter last, GetField &&get_field)
{
    using U =
        std::remove_cv_t<std::remove_reference_t<decltype(get_field(*first))>>;

    std::vector<U> values;
    values.reserve(static_cast<std::size_t>(std::distance(first, last)));

    while (first != last) {
        values.emplace_back(get_field(*first++));
    }

    return make_array(values);
}

template <typename T, typename GetField>
inline std::shared_ptr<::arrow::Array> make_array(
    std::size_t n, const T *data, GetField &&get_field)
{
    return make_array(data, data + n, std::forward<GetField>(get_field));
}

} // namespace dataframe

#endif // DATAFRAME_ARRYAY_HPP
