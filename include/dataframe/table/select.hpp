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

#ifndef DATAFRAME_TABLE_SELECT_HPP
#define DATAFRAME_TABLE_SELECT_HPP

#include <dataframe/array/select.hpp>
#include <dataframe/table/data_frame.hpp>

namespace dataframe {

template <typename Iter>
inline DataFrame select(const DataFrame &df, Iter first, Iter last)
{
    DataFrame ret;
    auto ncol = df.ncol();
    for (std::size_t i = 0; i != ncol; ++i) {
        auto col = df[i];
        ret[col.name()] = select_array(col.data(), first, last);
    }

    return ret;
}

template <typename Alloc>
inline DataFrame select(
    const DataFrame &df, const std::vector<bool, Alloc> &mask)
{
    if (df.nrow() != mask.size()) {
        throw DataFrameException("mask does not have the correct size");
    }

    std::vector<std::size_t> index;
    for (std::size_t i = 0; i != mask.size(); ++i) {
        if (mask[i]) {
            index.push_back(i);
        }
    }

    return select(df, index.begin(), index.end());
}

} // namespace dataframe

#endif // DATAFRAME_TABLE_SELECT_HPP
