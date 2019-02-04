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

#ifndef DATAFRAME_ARRAY_MAKE_ARRAY_HPP
#define DATAFRAME_ARRAY_MAKE_ARRAY_HPP

#include <dataframe/array/view.hpp>
#include <dataframe/error.hpp>
#include <arrow/api.h>

namespace dataframe {

template <typename T, typename ArrowType>
inline std::enable_if_t<std::is_base_of_v<::arrow::DataType, ArrowType>,
    std::shared_ptr<::arrow::Array>>
make_array(
    const ArrayViewBase<T> &view, const std::shared_ptr<ArrowType> &type)
{
    typename ::arrow::TypeTraits<ArrowType>::BuilderType builder(
        type, ::arrow::default_memory_pool());

    if (view.null_count() == 0) {
        DF_ARROW_ERROR_HANDLER(builder.AppendValues(
            view.data(), static_cast<std::int64_t>(view.size())));
    } else {
        DF_ARROW_ERROR_HANDLER(builder.AppendValues(
            view.data(), static_cast<std::int64_t>(view.size()), view.mask()));
    }

    std::shared_ptr<::arrow::Array> ret;
    DF_ARROW_ERROR_HANDLER(builder.Finish(&ret));

    return ret;
}

} // namespace dataframe

#endif // DATAFRAME_ARRAY_MAKE_ARRAY_HPP
