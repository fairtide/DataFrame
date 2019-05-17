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

#include <dataframe/serializer/bson/compress.hpp>
#include <dataframe/serializer/bson/type_reader.hpp>

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

    ::arrow::Status Visit(const ::arrow::NullType &) override
    {
        data_.length = view_[Schema::DATA()].get_int64().value;
        data_.buffers.reserve(1);
        data_.buffers.push_back(nullptr);

        return ::arrow::Status::OK();
    }

    ::arrow::Status Visit(const ::arrow::BooleanType &) override
    {
        auto buffer = decompress(view_[Schema::DATA()].get_binary(), pool_);
        auto n = buffer->size();
        auto p = buffer->data();

        ::arrow::BooleanBuilder builder(pool_);
        DF_ARROW_ERROR_HANDLER(builder.Reserve(n));
        for (std::int64_t i = 0; i != n; ++i) {
            DF_ARROW_ERROR_HANDLER(builder.Append(static_cast<bool>(p[i])));
        }

        std::shared_ptr<::arrow::Array> ret;
        DF_ARROW_ERROR_HANDLER(builder.Finish(&ret));

        data_ = std::move(*ret->data());

        return ::arrow::Status::OK();
    }

#define DF_DEFINE_VISITOR(TypeClass)                                          \
    ::arrow::Status Visit(const ::arrow::TypeClass##Type &) override             \
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
    // DF_DEFINE_VISITOR(Interval)

#undef DF_DEFINE_VISITOR

#define DF_DEFINE_VISITOR(TypeClass)                                          \
    ::arrow::Status Visit(const ::arrow::TypeClass##Type &) override             \
    {                                                                         \
        using T = typename ::arrow::TypeClass##Type::c_type;                  \
                                                                              \
        auto buffer =                                                         \
            decompress<T>(view_[Schema::DATA()].get_binary(), pool_);         \
                                                                              \
        data_.length = decode_datetime<T>(buffer);                            \
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

    ::arrow::Status Visit(const ::arrow::FixedSizeBinaryType &type) override
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

    ::arrow::Status Visit(const ::arrow::Decimal128Type &type) override
    {
        return Visit(static_cast<const ::arrow::FixedSizeBinaryType &>(type));
    }

    ::arrow::Status Visit(const ::arrow::BinaryType &) override
    {
        auto values = decompress(view_[Schema::DATA()].get_binary(), pool_);

        auto offsets = decompress<std::int32_t>(
            view_[Schema::OFFSET()].get_binary(), pool_);

        data_.length = decode_offsets(offsets);
        data_.buffers.reserve(3);
        data_.buffers.push_back(make_mask());
        data_.buffers.push_back(std::move(offsets));
        data_.buffers.push_back(std::move(values));

        return ::arrow::Status::OK();
    }

    ::arrow::Status Visit(const ::arrow::StringType &type) override
    {
        return Visit(static_cast<const ::arrow::BinaryType &>(type));
    }

    ::arrow::Status Visit(const ::arrow::ListType &type) override
    {
        auto offsets = decompress<std::int32_t>(
            view_[Schema::OFFSET()].get_binary(), pool_);

        data_.length = decode_offsets(offsets);
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

    ::arrow::Status Visit(const ::arrow::StructType &type) override
    {
        auto data_view = view_[Schema::DATA()].get_document().view();
        data_.length = data_view[Schema::LENGTH()].get_int64().value;
        data_.buffers.reserve(1);
        data_.buffers.push_back(make_mask());

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

    ::arrow::Status Visit(const ::arrow::DictionaryType &type) override
    {
        auto data_view = view_[Schema::DATA()].get_document().view();

        auto index_view = data_view[Schema::INDEX()].get_document().view();
        ::arrow::ArrayData index_data(nullptr, 0);
        index_data.type = read_type(index_view);
        DataReader index_reader(index_data, index_view, pool_);
        DF_ARROW_ERROR_HANDLER(index_data.type->Accept(&index_reader));

        auto dict_view = data_view[Schema::DICT()].get_document().view();
        ::arrow::ArrayData dict_data(nullptr, 0);
        dict_data.type = read_type(dict_view);
        DataReader dict_reader(dict_data, dict_view, pool_);
        DF_ARROW_ERROR_HANDLER(dict_data.type->Accept(&dict_reader));

        auto dict_type = ::arrow::dictionary(index_data.type,
            ::arrow::MakeArray(
                std::make_shared<::arrow::ArrayData>(std::move(dict_data))),
            type.ordered());

        std::shared_ptr<::arrow::Array> dict_array;
        DF_ARROW_ERROR_HANDLER(::arrow::DictionaryArray::FromArrays(dict_type,
            ::arrow::MakeArray(
                std::make_shared<::arrow::ArrayData>(std::move(index_data))),
            &dict_array));

        data_ = std::move(*dict_array->data());

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
