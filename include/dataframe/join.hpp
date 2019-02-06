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

#ifndef DATAFRAME_JOIN_HPP
#define DATAFRAME_JOIN_HPP

#include <dataframe/select.hpp>
#include <set>

namespace dataframe {

namespace internal {

inline void check_join_keys(const DataFrame &df1, const DataFrame &df2,
    const std::string &key, make_unique = false)
{
    if (!df1[key]) {
        throw DataFrameException(
            "key " + key + " does not exist on left DataFrame");
    }

    if (!df2[key]) {
        throw DataFrameException(
            "key " + key + " does not exist on left DataFrame");
    }

    if (make_unqiue) {
        return;
    }

    auto ncol1 = df1.ncol();
    std::set<std::string> keys;
    for (std::size_t i = 0; i != ncol1; ++i) {
        auto col = df1[i];
        if (col.name() != key) {
            if (!keys.insert(col.name()).second) {
                throw DataFrameException("key " + key + " is not unique");
            }
        }
    }

    auto ncol2 = df2.ncol();
    for (std::size_t i = 0; i != ncol2; ++i) {
        auto col = df2[i];
        if (col.name() != key) {
            if (!keys.insert(col.name()).second) {
                throw DataFrameException("key " + key + " is not unique");
            }
        }
    }
}

} // namespace internal

inline DataFrame inner_join(const DataFrame &df1, const DataFrame &df2,
    const std::string &key, make_unique = false)
{
    internal::check_join_keys(df1, df2, key, make_unique);
}

inline DataFrame outer_join(const DataFrame &df1, const DataFrame &df2,
    const std::string &key, make_unique = false)
{
    internal::check_join_keys(df1, df2, key, make_unique);
}

inline DataFrame left_join(const DataFrame &df1, const DataFrame &df2,
    const std::string &key, make_unique = false)
{
    internal::check_join_keys(df1, df2, key, make_unique);
}

inline DataFrame right_join(const DataFrame &df1, const DataFrame &df2,
    const std::string &key, make_unique = false)
{
    internal::check_join_keys(df1, df2, key, make_unique);
}

inline DataFrame semi_join(const DataFrame &df1, const DataFrame &df2,
    const std::string &key, make_unique = false)
{
    internal::check_join_keys(df1, df2, key, make_unique);
}

inline DataFrame anti_join(const DataFrame &df1, const DataFrame &df2,
    const std::string &key, make_unique = false)
{
    internal::check_join_keys(df1, df2, key, make_unique);
}

inline DataFrame cross_join(const DataFrame &df1, const DataFrame &df2,
    const std::string &key, make_unique = false)
{
    internal::check_join_keys(df1, df2, key, make_unique);
}

} // namespace dataframe

#endif // DATAFRAME_JOIN_HPP
