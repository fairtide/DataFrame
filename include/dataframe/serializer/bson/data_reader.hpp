//============================================================================
// Copyright 2019 Fairtide Pte. Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//============================================================================

#ifndef DATAFRAME_SERIALIZER_BSON_DATA_READER_HPP
#define DATAFRAME_SERIALIZER_BSON_DATA_READER_HPP

#include <dataframe/serializer/base.hpp>
#include <dataframe/serializer/bson/compress.hpp>
#include <dataframe/serializer/bson/schema.hpp>

namespace dataframe {

namespace bson {

class DataReader : public ::arrow::TypeVisitor
{
  public:
    DataReader(::arrow::ArrayData &data, const ::bsoncxx::document::view &view,
        ::arrow::MemoryPool *pool)
        : data_(data)
        , view_(view)
        , pool_(pool)
    {
    }

    ::arrow::Status Visit(const ::arrow::NullType &) final
    {
        data_.length = view_[Schema::DATA()].get_int64().value;

        return ::arrow::Status::OK();
    }

#define DF_DEFINE_VISITOR(TypeClass)                                          \
    ::arrow::Status Visit(const ::arrow::TypeClass##Type &) final             \
    {                                                                         \
        using T = typename ::arrow::TypeClass##Type::c_type;                  \
                                                                              \
        auto buffer =                                                         \
            decompress<T>(view_[Schema::DATA()].get_binary(), pool_);         \
                                                                              \
        data_.length = buffer->size() / static_cast<std::int64_t>(sizeof(T)); \
        data_.buffers.reserve(2);                                             \
        data_.buffers.push_back(make_mask());                                 \
        data_.buffers.push_back(std::move(buffer));                           \
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
    DF_DEFINE_VISITOR(Time32)
    DF_DEFINE_VISITOR(Time64)
    DF_DEFINE_VISITOR(Interval)

#undef DF_DEFINE_VISITOR

#define DF_DEFINE_VISITOR(TypeClass)                                          \
    ::arrow::Status Visit(const ::arrow::TypeClass##Type &) final             \
    {                                                                         \
        using T = typename ::arrow::TypeClass##Type::c_type;                  \
                                                                              \
        auto buffer =                                                         \
            decompress<T>(view_[Schema::DATA()].get_binary(), pool_);         \
                                                                              \
        auto n = buffer->size() / static_cast<std::int64_t>(sizeof(T));       \
        auto p = reinterpret_cast<T *>(                                       \
            dynamic_cast<::arrow::MutableBuffer &>(*buffer).mutable_data());  \
                                                                              \
        for (std::int64_t i = 1; i < n; ++i) {                                \
            p[i] += p[i - 1];                                                 \
        }                                                                     \
                                                                              \
        data_.length = n;                                                     \
        data_.buffers.reserve(2);                                             \
        data_.buffers.push_back(make_mask());                                 \
        data_.buffers.push_back(std::move(buffer));                           \
                                                                              \
        return ::arrow::Status::OK();                                         \
    }

    DF_DEFINE_VISITOR(Date32)
    DF_DEFINE_VISITOR(Date64)
    DF_DEFINE_VISITOR(Timestamp)

#undef DF_DEFINE_VISITOR

    ::arrow::Status Visit(const ::arrow::FixedSizeBinaryType &type) final
    {
        auto buffer = decompress(view_[Schema::DATA()].get_binary(), pool_);

        if (buffer->size() % type.byte_width() != 0) {
            throw DataFrameException("Incorrect buffer size for POD type");
        }

        data_.length = buffer->size() / type.byte_width();
        data_.buffers.reserve(2);
        data_.buffers.push_back(make_mask());
        data_.buffers.push_back(std::move(buffer));

        return ::arrow::Status::OK();
    }

    ::arrow::Status Visit(const ::arrow::BinaryType &) final
    {
        auto values = decompress(view_[Schema::DATA()].get_binary(), pool_);

        auto offsets = decompress<std::int32_t>(
            view_[Schema::OFFSET()].get_binary(), pool_);

        auto byte_width = static_cast<std::int64_t>(sizeof(std::int32_t));
        data_.length = offsets->size() / byte_width - 1;
        data_.buffers.reserve(3);
        data_.buffers.push_back(make_mask());
        data_.buffers.push_back(std::move(offsets));
        data_.buffers.push_back(std::move(values));

        return ::arrow::Status::OK();
    }

    ::arrow::Status Visit(const ::arrow::StringType &type) final
    {
        return Visit(static_cast<const ::arrow::BinaryType &>(type));
    }

    ::arrow::Status Visit(const ::arrow::ListType &type) final
    {
        auto offsets = decompress<std::int32_t>(
            view_[Schema::OFFSET()].get_binary(), pool_);

        auto byte_width = static_cast<std::int64_t>(sizeof(std::int32_t));
        data_.length = offsets->size() / byte_width - 1;

        data_.buffers.reserve(2);
        data_.buffers.push_back(make_mask());
        data_.buffers.push_back(std::move(offsets));

        ::arrow::ArrayData data(nullptr, 0);
        data.type = type.value_type();

        DataReader reader(
            data, view_[Schema::DATA()].get_document().view(), pool_);
        DF_ARROW_ERROR_HANDLER(data.type->Accept(&reader));
        data_.child_data.push_back(
            std::make_shared<::arrow::ArrayData>(std::move(data)));

        return ::arrow::Status::OK();
    }

    ::arrow::Status Visit(const ::arrow::StructType &type) final
    {
        auto data_view = view_[Schema::DATA()].get_document().view();
        data_.length = data_view[Schema::LENGTH()].get_int64().value;

        auto n = type.num_children();
        auto field_view = data_view[Schema::FIELDS()].get_document().view();
        for (auto i = 0; i != n; ++i) {
            auto field = type.child(i);

            ::arrow::ArrayData field_data(nullptr, 0);
            field_data.type = field->type();

            DataReader reader(field_data,
                field_view[field->name()].get_document().view(), pool_);
            DF_ARROW_ERROR_HANDLER(field_data.type->Accept(&reader));

            data_.child_data.push_back(
                std::make_shared<::arrow::ArrayData>(std::move(field_data)));
        }

        return ::arrow::Status::OK();
    }

  private:
    std::shared_ptr<::arrow::Buffer> make_mask()
    {
        if (!view_[Schema::MASK()]) {
            throw DataFrameException("Mask not found");
        }

        return nullptr;
    }

  private:
    ::arrow::ArrayData &data_;
    ::bsoncxx::document::view view_;
    ::arrow::MemoryPool *pool_;
};

} // namespace bson

} // namespace dataframe

#endif // DATAFRAME_SERIALIZER_BSON_DATA_READER_HPP
