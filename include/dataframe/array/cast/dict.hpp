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

#ifndef DATAFRAME_ARRAY_CAST_DICT_HPP
#define DATAFRAME_ARRAY_CAST_DICT_HPP

#include <dataframe/array/cast/primitive.hpp>

namespace dataframe {

template <typename T, typename Index, bool Ordered>
struct CastArrayVisitor<Dict<T, Index, Ordered>> : ::arrow::ArrayVisitor {
    std::shared_ptr<::arrow::Array> result;
    ::arrow::MemoryPool *pool;

    CastArrayVisitor(
        std::shared_ptr<::arrow::Array> data, ::arrow::MemoryPool *p)
        : result(std::move(data))
        , pool(p)
    {
    }

    ::arrow::Status Visit(const ::arrow::DictionaryArray &array) override
    {
        auto index = cast_array<Index>(array.indices(), pool);
        auto dicts = cast_array<T>(array.dictionary(), pool);

#if ARROW_VERSION >= 14000
        auto type = ::arrow::dictionary(index->type(), dicts->type(), Ordered);
        result =
            std::make_shared<::arrow::DictionaryArray>(type, index, dicts);
#else
        auto type = ::arrow::dictionary(index->type(), dicts, Ordered);
        result = std::make_shared<::arrow::DictionaryArray>(type, index);
#endif

        if (array.null_count() != 0) {
            auto data = result->data()->Copy();

            data->null_count = array.null_count();

            ARROW_RETURN_NOT_OK(::arrow::internal::CopyBitmap(pool,
                array.null_bitmap()->data(), array.offset(), array.length(),
                &data->buffers[0]));

            result = ::arrow::MakeArray(std::move(data));
        }

        return ::arrow::Status::OK();
    }
};

} // namespace dataframe

#endif // DATAFRAME_ARRAY_CAST_DICT_HPP
