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

#ifndef DATAFRAME_CONCATENATE_HPP
#define DATAFRAME_CONCATENATE_HPP

#include <dataframe/table/data_frame.hpp>
#include <map>

namespace dataframe {

template <typename InputIter>
inline DataFrame bind_rows(InputIter first, InputIter last)
{
    DataFrame ret;

    auto count = std::distance(first, last);
    if (count == 0) {
        return ret;
    }

    if (count == 1) {
        ret = *first;
        return ret;
    }

    while (first != last && first->empty()) {
        ++first;
    }

    if (first == last) {
        return ret;
    }

    std::vector<std::string> keys;
    std::map<std::string, std::vector<std::shared_ptr<::arrow::Array>>> chunks;

    auto ncol = first->ncol();
    for (std::size_t i = 0; i != ncol; ++i) {
        auto col = first->at(i);
        keys.push_back(col.name());
        chunks[keys.back()] = {col.data()};
    }
    ++first;

    while (first != last) {
        if (first->empty()) {
            ++first;
            continue;
        }

        if (first->ncol() != ncol) {
            throw DataFrameException("Different number of columns");
        }

        for (std::size_t i = 0; i != ncol; ++i) {
            auto col = first->at(i);
            chunks.at(col.name()).push_back(col.data());
        }

        ++first;
    }

    for (auto &key : keys) {
        ret[key] = bind_array(chunks.at(key));
    }

    return ret;
}

template <typename InputIter>
inline DataFrame bind_cols(InputIter first, InputIter last)
{
    DataFrame ret;

    auto count = std::distance(first, last);
    if (count == 0) {
        return ret;
    }

    if (count == 1) {
        ret = *first;
        return ret;
    }

    while (first != last) {
        const auto &df = *first++;

        if (df.empty()) {
            continue;
        }

        auto ncol = df.ncol();
        for (std::size_t i = 0; i != ncol; ++i) {
            auto col = df[i];
            if (ret[col.name()]) {
                throw DataFrameException(
                    "Duplicate column name " + col.name());
            }
            ret[col.name()] = col;
        }
    }

    return ret;
}

inline DataFrame bind_rows(const std::vector<DataFrame> &dfs)
{
    return bind_rows(dfs.begin(), dfs.end());
}

inline DataFrame bind_cols(const std::vector<DataFrame> &dfs)
{
    return bind_cols(dfs.begin(), dfs.end());
}

} // namespace dataframe

#endif // DATAFRAME_CONCATENATE_HPP
