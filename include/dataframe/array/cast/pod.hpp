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

#ifndef DATAFRAME_ARRAY_CAST_POD_HPP
#define DATAFRAME_ARRAY_CAST_POD_HPP

#include <dataframe/array/cast/primitive.hpp>

namespace dataframe {

template <typename T>
struct CastArrayVisitor<POD<T>> : ::arrow::ArrayVisitor {
    static_assert(std::is_standard_layout_v<T>);

    std::shared_ptr<::arrow::Array> result;

    CastArrayVisitor(
        std::shared_ptr<::arrow::Array> data, ::arrow::MemoryPool *)
        : result(std::move(data))
    {
    }

    ::arrow::Status Visit(const ::arrow::FixedSizeBinaryArray &array) override
    {
        return is_type<POD<T>>(array.type()) ?
            ::arrow::Status::OK() :
            ::arrow::Status(::arrow::StatusCode::NotImplemented,
                "Different byte width between POD types");
    }
};

} // namespace dataframe

#endif // DATAFRAME_ARRAY_CAST_POD_HPP
