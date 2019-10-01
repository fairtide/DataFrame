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

#ifndef DATAFRAME_TABLE_SPLIT_HPP
#define DATAFRAME_TABLE_SPLIT_HPP

#include <dataframe/table/data_frame.hpp>

namespace dataframe {

namespace internal {

inline std::vector<std::shared_ptr<::arrow::Table>> split_rows(
    const ::arrow::Table &table, std::int64_t nrows)
{
    std::vector<std::shared_ptr<::arrow::Table>> ret;

    auto offset = INT64_C(0);
    while (offset < table.num_rows()) {
        auto end = std::min(table.num_rows(), offset + nrows);
        auto len = end - offset;
        ret.push_back(table.Slice(offset, len));
        offset = end;
    }

    return ret;
}

} // namespace internal

inline std::vector<DataFrame> split_rows(
    const DataFrame &df, std::size_t nrows)
{
    if (df.nrow() == 0 || df.nrow() <= nrows) {
        return std::vector{df};
    }

    if (nrows <= 0) {
        throw DataFrameException("Non-positive split chunk size");
    }

    auto tables =
        internal::split_rows(df.table(), static_cast<std::int64_t>(nrows));

    std::vector<DataFrame> ret;
    ret.reserve(tables.size());
    for (auto &t : tables) {
        ret.emplace_back(std::move(t));
    }

    return ret;
}

} // namespace dataframe

#endif // DATAFRAME_TABLE_SPLIT_HPP
