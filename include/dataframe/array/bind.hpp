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

#ifndef DATAFRAME_ARRAY_BIND_HPP
#define DATAFRAME_ARRAY_BIND_HPP

#include <dataframe/array/type.hpp>
#include <arrow/util/concatenate.h>

namespace dataframe {

inline std::shared_ptr<::arrow::Array> bind_array(
    const ::arrow::ArrayVector &chunks)
{
    if (chunks.empty()) {
        return nullptr;
    }

    if (chunks.size() == 1) {
        return chunks.front();
    }

    std::shared_ptr<::arrow::Array> ret;
    DF_ARROW_ERROR_HANDLER(
        ::arrow::Concatenate(chunks, ::arrow::default_memory_pool(), &ret));

    return ret;
}

template <typename InputIter>
inline std::shared_ptr<::arrow::Array> bind_array(
    InputIter first, InputIter last)
{
    ::arrow::ArrayVector chunks;
    for (auto iter = first; iter != last; ++iter) {
        chunks.emplace_back(*iter);
    }

    return bind_array(chunks);
}

} // namespace dataframe

#endif // DATAFRAME_ARRAY_BIND_HPP
