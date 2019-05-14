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

#ifndef DATAFRAME_TABLE_MAKE_HPP
#define DATAFRAME_TABLE_MAKE_HPP

#include <dataframe/table/data_frame.hpp>

#define DF_FIELD_PAIR(T, field) std::make_pair(#field, &T::field)

namespace dataframe {

namespace internal {

template <typename Iter>
inline void make_column(DataFrame &, Iter, Iter)
{
}

template <typename Iter, typename K, typename V, typename... Args>
inline void make_column(DataFrame &ret, Iter first, Iter last,
    const std::pair<K, V> &kv, Args &&... args)
{
    ret[kv.first].emplace(first, last, kv.second);
    make_column(ret, first, last, std::forward<Args>(args)...);
}

} // namespace internal

template <typename Iter, typename... Args>
inline DataFrame make_dataframe(Iter first, Iter last, Args &&... args)
{
    DataFrame ret;
    internal::make_column(ret, first, last, std::forward<Args>(args)...);

    return ret;
}

template <typename T, typename Alloc, typename... Args>
inline DataFrame make_dataframe(
    const std::vector<T, Alloc> &vec, Args &&... args)
{
    return make_dataframe(vec.begin(), vec.end(), std::forward<Args>(args)...);
}

template <typename T, typename... Args>
inline DataFrame make_dataframe(std::size_t n, const T *data, Args &&... args)
{
    return make_dataframe(data, data + n, std::forward<Args>(args)...);
}

} // namespace dataframe

#endif // DATAFRAME_TABLE_MAKE_HPP
