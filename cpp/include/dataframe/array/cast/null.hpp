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

#ifndef DATAFRAME_ARRAY_CAST_NULL_HPP
#define DATAFRAME_ARRAY_CAST_NULL_HPP

#include <dataframe/array/cast/primitive.hpp>

namespace dataframe {

template <>
struct CastArrayVisitor<std::nullptr_t> : ::arrow::ArrayVisitor {
    std::shared_ptr<::arrow::Array> result;

    CastArrayVisitor(
        std::shared_ptr<::arrow::Array> data, ::arrow::MemoryPool *)
        : result(std::move(data))
    {
    }

    ::arrow::Status Visit(const ::arrow::NullArray &) override
    {
        return ::arrow::Status::OK();
    }
};

template <>
struct CastArrayVisitor<void> : CastArrayVisitor<std::nullptr_t> {
    using CastArrayVisitor<std::nullptr_t>::CastArrayVisitor;
};

} // namespace dataframe

#endif // DATAFRAME_ARRAY_CAST_NULL_HPP
