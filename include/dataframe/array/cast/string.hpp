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

#ifndef DATAFRAME_ARRAY_CAST_STRING_HPP
#define DATAFRAME_ARRAY_CAST_STRING_HPP

#include <dataframe/array/cast/primitive.hpp>

namespace dataframe {

namespace internal {

struct CastStringArrayVisitor : ::arrow::ArrayVisitor {
    std::shared_ptr<::arrow::Array> result;

    CastStringArrayVisitor(
        std::shared_ptr<::arrow::Array> data, ::arrow::MemoryPool *)
        : result(std::move(data))
    {
    }

    ::arrow::Status Visit(const ::arrow::StringArray &) override
    {
        return ::arrow::Status::OK();
    }

    ::arrow::Status Visit(const ::arrow::BinaryArray &) override
    {
        return ::arrow::Status::OK();
    }
};

} // namespace internal

template <>
struct CastArrayVisitor<std::string> : internal::CastStringArrayVisitor {
    using internal::CastStringArrayVisitor::CastStringArrayVisitor;
};

template <>
struct CastArrayVisitor<std::string_view> : internal::CastStringArrayVisitor {
    using internal::CastStringArrayVisitor::CastStringArrayVisitor;
};

template <>
struct CastArrayVisitor<Bytes> : internal::CastStringArrayVisitor {
    using internal::CastStringArrayVisitor::CastStringArrayVisitor;
};

} // namespace dataframe

#endif // DATAFRAME_ARRAY_CAST_STRING_HPP
