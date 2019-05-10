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

namespace dataframe {

namespace internal {

struct ArrayBindVisitor final : ::arrow::TypeVisitor {
    std::int64_t length;
    const std::vector<std::shared_ptr<::arrow::Array>> &chunks;
    std::shared_ptr<::arrow::Array> result;

    ArrayBindVisitor(const std::vector<std::shared_ptr<::arrow::Array>> &data)
        : length(0)
        , chunks(data)
    {
        auto type = chunks.front()->type();
        for (auto &chunk : chunks) {
            length += chunk->length();
            if (!type->Equals(chunk->type())) {
                throw DataFrameException("Bind arrays of different types");
            }
        }
    }

    ::arrow::Status Visit(const ::arrow::NullType &) final
    {
        result = std::make_shared<::arrow::NullArray>(length);

        return ::arrow::Status::OK();
    }

#define DF_DEFINE_VISITOR(Arrow)                                              \
    ::arrow::Status Visit(const ::arrow::Arrow##Type &) final                 \
    {                                                                         \
        ::arrow::Arrow##Builder builder(                                      \
            chunks.front()->type(), ::arrow::default_memory_pool());          \
                                                                              \
        ARROW_RETURN_NOT_OK(builder.Reserve(length));                         \
                                                                              \
        for (auto &chunk : chunks) {                                          \
            auto data = std::static_pointer_cast<                             \
                ::arrow::TypeTraits<::arrow::Arrow##Type>::ArrayType>(chunk); \
            auto n = data->length();                                          \
            auto p = data->raw_values();                                      \
            ARROW_RETURN_NOT_OK(builder.AppendValues(p, p + n));              \
        }                                                                     \
                                                                              \
        ARROW_RETURN_NOT_OK(builder.Finish(&result));                         \
                                                                              \
        return ::arrow::Status::OK();                                         \
    }

    DF_DEFINE_VISITOR(Int8)
    DF_DEFINE_VISITOR(Int16)
    DF_DEFINE_VISITOR(Int32)
    DF_DEFINE_VISITOR(Int64)
    DF_DEFINE_VISITOR(UInt8)
    DF_DEFINE_VISITOR(UInt16)
    DF_DEFINE_VISITOR(UInt32)
    DF_DEFINE_VISITOR(UInt64)
    DF_DEFINE_VISITOR(HalfFloat)
    DF_DEFINE_VISITOR(Float)
    DF_DEFINE_VISITOR(Double)
    DF_DEFINE_VISITOR(Date32)
    DF_DEFINE_VISITOR(Date64)
    DF_DEFINE_VISITOR(Timestamp)
    DF_DEFINE_VISITOR(Time32)
    DF_DEFINE_VISITOR(Time64)
    // DF_DEFINE_VISITOR(Interval)

#undef DF_DEFINE_VISITOR

#define DF_DEFINE_VISITOR(Arrow)                                              \
    ::arrow::Status Visit(const ::arrow::Arrow##Type &) final                 \
    {                                                                         \
        ::arrow::Arrow##Builder builder(                                      \
            chunks.front()->type(), ::arrow::default_memory_pool());          \
                                                                              \
        ARROW_RETURN_NOT_OK(builder.Reserve(length));                         \
                                                                              \
        for (auto &chunk : chunks) {                                          \
            auto data =                                                       \
                std::static_pointer_cast<::arrow::Arrow##Array>(chunk);       \
            auto n = data->length();                                          \
            for (std::int64_t i = 0; i != n; ++i) {                           \
                ARROW_RETURN_NOT_OK(builder.Append(data->GetView(i)));        \
            }                                                                 \
        }                                                                     \
                                                                              \
        ARROW_RETURN_NOT_OK(builder.Finish(&result));                         \
                                                                              \
        return ::arrow::Status::OK();                                         \
    }

    DF_DEFINE_VISITOR(Boolean)
    DF_DEFINE_VISITOR(Binary)
    DF_DEFINE_VISITOR(String)
    DF_DEFINE_VISITOR(FixedSizeBinary)
    DF_DEFINE_VISITOR(Decimal128)

#undef DF_DEFINE_VISITOR

    // DF_DEFINE_VISITOR(List)
    // DF_DEFINE_VISITOR(Struct)
    // DF_DEFINE_VISITOR(Union)
    // DF_DEFINE_VISITOR(Dictionary) // TODO
};

} // namespace internal

inline std::shared_ptr<::arrow::Array> bind_array(
    const std::vector<std::shared_ptr<::arrow::Array>> &chunks)
{
    if (chunks.empty()) {
        return nullptr;
    }

    internal::ArrayBindVisitor visitor(chunks);
    DF_ARROW_ERROR_HANDLER(chunks.front()->type()->Accept(&visitor));

    return visitor.result;
}

template <typename InputIter>
inline std::shared_ptr<::arrow::Array> bind_array(
    InputIter first, InputIter last)
{
    std::vector<std::shared_ptr<::arrow::Array>> chunks;
    for (auto iter = first; iter != last; ++iter) {
        chunks.emplace_back(*iter);
    }

    return bind_array(chunks);
}

} // namespace dataframe

#endif // DATAFRAME_ARRAY_BIND_HPP
