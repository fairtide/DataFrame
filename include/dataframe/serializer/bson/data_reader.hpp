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
        data_.buffers.push_back(make_mask());

        return ::arrow::Status::OK();
    }

    ::arrow::Status Visit(const ::arrow::BooleanType &) override
    {
        auto buffer = decompress(view_[Schema::DATA()].get_binary(), pool_);
        auto n = buffer->size();
        auto p = buffer->data();

        std::shared_ptr<::arrow::Buffer> bitmap;
        DF_ARROW_ERROR_HANDLER(::arrow::AllocateBuffer(
            pool_, ::arrow::BitUtil::BytesForBits(n), &bitmap));

        auto bits =
            dynamic_cast<::arrow::MutableBuffer &>(*bitmap).mutable_data();

        if (bitmap->size() != 0) {
            bits[bitmap->size() - 1] = 0;
        }

        for (std::int64_t i = 0; i != n; ++i) {
            ::arrow::BitUtil::SetBitTo(bits, i, static_cast<bool>(p[i]));
        }

        data_.length = n;
        data_.buffers.reserve(2);
        data_.buffers.push_back(make_mask());
        data_.buffers.push_back(std::move(bitmap));

        return ::arrow::Status::OK();
    }

#define DF_DEFINE_VISITOR(TypeClass)                                          \
    ::arrow::Status Visit(const ::arrow::TypeClass##Type &) override          \
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
    ::arrow::Status Visit(const ::arrow::TypeClass##Type &) override          \
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

        data_.type = ::arrow::list(data.type);

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
        std::vector<std::shared_ptr<::arrow::Field>> fields;

        for (auto i = 0; i != n; ++i) {
            auto field = type.child(i);

            ::arrow::ArrayData field_data(nullptr, 0);
            field_data.type = field->type();

            DataReader reader(field_data,
                field_view[field->name()].get_document().view(), pool_);
            DF_ARROW_ERROR_HANDLER(field_data.type->Accept(&reader));

            fields.push_back(::arrow::field(field->name(), field_data.type));

            data_.child_data.push_back(
                std::make_shared<::arrow::ArrayData>(std::move(field_data)));
        }

        data_.type = ::arrow::struct_(fields);

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

        auto dict_type = ::arrow::dictionary(
            index_data.type, dict_data.type, type.ordered());

        std::shared_ptr<::arrow::Array> dict_array;
        DF_ARROW_ERROR_HANDLER(::arrow::DictionaryArray::FromArrays(dict_type,
            ::arrow::MakeArray(
                std::make_shared<::arrow::ArrayData>(std::move(index_data))),
            ::arrow::MakeArray(
                std::make_shared<::arrow::ArrayData>(std::move(dict_data))),
            &dict_array));

        data_ = std::move(*dict_array->data());

        return ::arrow::Status::OK();
    }

  private:
    std::shared_ptr<::arrow::Buffer> make_mask()
    {
        if (data_.type->id() == ::arrow::Type::NA) {
            data_.null_count = data_.length;
            return nullptr;
        }

        auto n = data_.length;
        if (n == 0) {
            data_.null_count = 0;
            return nullptr;
        }

        auto buffer = decompress(view_[Schema::MASK()].get_binary(), pool_);
        auto nbytes = buffer->size();
        auto bitmap =
            dynamic_cast<::arrow::MutableBuffer &>(*buffer).mutable_data();

        if (nbytes != ::arrow::BitUtil::BytesForBits(n)) {
            throw DataFrameException("Mask has incorrect length");
        }

        internal::swap_bit_order(bitmap[nbytes - 1]);
        auto count = ::arrow::internal::CountSetBits(bitmap, 0, n);

        if (count == data_.length) {
            data_.null_count = 0;
            return nullptr;
        }

        internal::swap_bit_order(static_cast<std::size_t>(nbytes - 1), bitmap);

        data_.null_count = data_.length - count;

        return buffer;
    }

  private:
    ::arrow::ArrayData &data_;
    ::bsoncxx::document::view view_;
    ::arrow::MemoryPool *pool_;
};

} // namespace bson

} // namespace dataframe

#endif // DATAFRAME_SERIALIZER_BSON_DATA_READER_HPP
