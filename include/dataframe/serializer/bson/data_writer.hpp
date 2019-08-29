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

#ifndef DATAFRAME_SERIALIZER_BSON_DATA_WRITER_HPP
#define DATAFRAME_SERIALIZER_BSON_DATA_WRITER_HPP

#include <dataframe/serializer/bson/compress.hpp>
#include <dataframe/serializer/bson/type_writer.hpp>

namespace dataframe {

namespace bson {

class DataWriter : public ::arrow::ArrayVisitor
{
  public:
    DataWriter(::bsoncxx::builder::basic::document &builder,
        Vector<std::uint8_t> &buffer1, Vector<std::uint8_t> &buffer2,
        int compression_level)
        : builder_(builder)
        , buffer1_(buffer1)
        , buffer2_(buffer2)
        , compression_level_(compression_level)
    {
    }

    ::arrow::Status Visit(const ::arrow::NullArray &array) override
    {
        using ::bsoncxx::builder::basic::kvp;

        builder_.append(
            kvp(Schema::DATA(), static_cast<std::int64_t>(array.length())));

        make_mask(builder_, array);

        TypeWriter type_writer(builder_);
        DF_ARROW_ERROR_HANDLER(array.type()->Accept(&type_writer));

        return ::arrow::Status::OK();
    }

    ::arrow::Status Visit(const ::arrow::BooleanArray &array) override
    {
        using ::bsoncxx::builder::basic::kvp;

        auto n = array.length();
        buffer1_.resize(static_cast<std::size_t>(n));
        for (std::int64_t i = 0; i != n; ++i) {
            buffer1_[static_cast<std::size_t>(i)] = array.GetView(i);
        }

        builder_.append(kvp(Schema::DATA(),
            compress(buffer1_, &buffer2_, compression_level_)));

        make_mask(builder_, array);

        TypeWriter type_writer(builder_);
        DF_ARROW_ERROR_HANDLER(array.type()->Accept(&type_writer));

        return ::arrow::Status::OK();
    }

#define DF_DEFINE_VISITOR(TypeName)                                           \
    ::arrow::Status Visit(const ::arrow::TypeName##Array &array) override     \
    {                                                                         \
        using ::bsoncxx::builder::basic::kvp;                                 \
                                                                              \
        builder_.append(kvp(Schema::DATA(),                                   \
            compress(array.length(), array.raw_values(), &buffer2_,           \
                compression_level_)));                                        \
                                                                              \
        make_mask(builder_, array);                                           \
                                                                              \
        TypeWriter type_writer(builder_);                                     \
        DF_ARROW_ERROR_HANDLER(array.type()->Accept(&type_writer));           \
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
    DF_DEFINE_VISITOR(Time32)
    DF_DEFINE_VISITOR(Time64)

#undef DF_DEFINE_VISITOR

#define DF_DEFINE_VISITOR(TypeName)                                           \
    ::arrow::Status Visit(const ::arrow::TypeName##Array &array) override     \
    {                                                                         \
        using ::bsoncxx::builder::basic::kvp;                                 \
                                                                              \
        builder_.append(kvp(Schema::DATA(),                                   \
            compress(array.length(), array.raw_values(), &buffer2_,           \
                compression_level_)));                                        \
                                                                              \
        make_mask(builder_, array);                                           \
                                                                              \
        TypeWriter type_writer(builder_);                                     \
        DF_ARROW_ERROR_HANDLER(array.type()->Accept(&type_writer));           \
                                                                              \
        return ::arrow::Status::OK();                                         \
    }

    DF_DEFINE_VISITOR(Float)
    DF_DEFINE_VISITOR(Double)

#undef DF_DEFINE_VISITOR

    // DF_DEFINE_VISITOR(Interval)

#define DF_DEFINE_VISITOR(TypeName)                                           \
    ::arrow::Status Visit(const ::arrow::TypeName##Array &array) override     \
    {                                                                         \
        using ::bsoncxx::builder::basic::kvp;                                 \
                                                                              \
        encode_datetime(array.length(), array.raw_values(), &buffer1_);       \
                                                                              \
        builder_.append(kvp(Schema::DATA(),                                   \
            compress(buffer1_, &buffer2_, compression_level_)));              \
                                                                              \
        make_mask(builder_, array);                                           \
                                                                              \
        TypeWriter type_writer(builder_);                                     \
        DF_ARROW_ERROR_HANDLER(array.type()->Accept(&type_writer));           \
                                                                              \
        return ::arrow::Status::OK();                                         \
    }

    DF_DEFINE_VISITOR(Date32)
    DF_DEFINE_VISITOR(Date64)
    DF_DEFINE_VISITOR(Timestamp)

#undef DF_DEFINE_VISITOR

    ::arrow::Status Visit(const ::arrow::FixedSizeBinaryArray &array) override
    {
        using ::bsoncxx::builder::basic::kvp;

        auto &type =
            dynamic_cast<const ::arrow::FixedSizeBinaryType &>(*array.type());

        builder_.append(kvp(Schema::DATA(),
            compress(array.length() * type.byte_width(), array.raw_values(),
                &buffer2_, compression_level_)));

        make_mask(builder_, array);

        TypeWriter type_writer(builder_);
        DF_ARROW_ERROR_HANDLER(array.type()->Accept(&type_writer));

        return ::arrow::Status::OK();
    }

    ::arrow::Status Visit(const ::arrow::Decimal128Array &array) override
    {
        return Visit(
            static_cast<const ::arrow::FixedSizeBinaryArray &>(array));
    }

    ::arrow::Status Visit(const ::arrow::BinaryArray &array) override
    {
        using ::bsoncxx::builder::basic::kvp;

        // data

        auto data_offset = array.value_offset(0);
        auto data_length = array.value_offset(array.length()) - data_offset;

        const std::uint8_t *data = nullptr;
        if (data_length != 0) {
            data = array.value_data()->data() + data_offset;
        }

        builder_.append(kvp(Schema::DATA(),
            compress(data_length, data, &buffer2_, compression_level_)));

        // mask

        make_mask(builder_, array);

        // type

        TypeWriter type_writer(builder_);
        DF_ARROW_ERROR_HANDLER(array.type()->Accept(&type_writer));

        // offset

        encode_offsets(array.length(), array.raw_value_offsets(), &buffer1_);

        builder_.append(kvp(Schema::OFFSET(),
            compress(buffer1_, &buffer2_, compression_level_)));

        return ::arrow::Status::OK();
    }

    ::arrow::Status Visit(const ::arrow::StringArray &array) override
    {
        return Visit(static_cast<const ::arrow::BinaryArray &>(array));
    }

    ::arrow::Status Visit(const ::arrow::ListArray &array) override
    {
        using ::bsoncxx::builder::basic::document;
        using ::bsoncxx::builder::basic::kvp;

        // data

        auto values_offset = array.value_offset(0);
        auto values_length =
            array.value_offset(array.length()) - values_offset;
        auto values = array.values()->Slice(values_offset, values_length);

        document data;
        DataWriter writer(data, buffer1_, buffer2_, compression_level_);
        DF_ARROW_ERROR_HANDLER(values->Accept(&writer));

        builder_.append(kvp(Schema::DATA(), data.extract()));

        // mask

        make_mask(builder_, array);

        // type

        TypeWriter type_writer(builder_);
        DF_ARROW_ERROR_HANDLER(array.type()->Accept(&type_writer));

        // offset

        encode_offsets(array.length(), array.raw_value_offsets(), &buffer1_);

        builder_.append(kvp(Schema::OFFSET(),
            compress(buffer1_, &buffer2_, compression_level_)));

        return ::arrow::Status::OK();
    }

    ::arrow::Status Visit(const ::arrow::StructArray &array) override
    {
        using ::bsoncxx::builder::basic::document;
        using ::bsoncxx::builder::basic::kvp;

        // data

        document fields;
        auto &type = dynamic_cast<const ::arrow::StructType &>(*array.type());
        auto n = type.num_children();
        for (auto i = 0; i != n; ++i) {
            auto field = type.child(i);

            if (field->name().empty()) {
                throw DataFrameException("empty field name");
            }

            auto field_data = array.field(i);
            if (field_data == nullptr) {
                throw DataFrameException(
                    "Field dat for " + field->name() + " not found");
            }

            document field_builder;

            DataWriter writer(
                field_builder, buffer1_, buffer2_, compression_level_);

            DF_ARROW_ERROR_HANDLER(field_data->Accept(&writer));

            fields.append(kvp(field->name(), field_builder.extract()));
        }

        document data;
        data.append(
            kvp(Schema::LENGTH(), static_cast<std::int64_t>(array.length())));
        data.append(kvp(Schema::FIELDS(), fields.extract()));

        builder_.append(kvp(Schema::DATA(), data.extract()));

        // mask

        make_mask(builder_, array);

        // type

        TypeWriter type_writer(builder_);
        DF_ARROW_ERROR_HANDLER(array.type()->Accept(&type_writer));

        return ::arrow::Status::OK();
    }

    ::arrow::Status Visit(const ::arrow::DictionaryArray &array) override
    {
        using ::bsoncxx::builder::basic::document;
        using ::bsoncxx::builder::basic::kvp;

        // data

        document index;
        DataWriter index_writer(index, buffer1_, buffer2_, compression_level_);
        DF_ARROW_ERROR_HANDLER(array.indices()->Accept(&index_writer));

        document dict;
        DataWriter dict_writer(dict, buffer1_, buffer2_, compression_level_);
        DF_ARROW_ERROR_HANDLER(array.dictionary()->Accept(&dict_writer));

        document data;
        data.append(kvp(Schema::INDEX(), index.extract()));
        data.append(kvp(Schema::DICT(), dict.extract()));

        builder_.append(kvp(Schema::DATA(), data.extract()));

        // mask

        make_mask(builder_, array);

        // type

        TypeWriter type_writer(builder_);
        DF_ARROW_ERROR_HANDLER(array.type()->Accept(&type_writer));

        return ::arrow::Status::OK();
    }

  private:
    void make_mask(::bsoncxx::builder::basic::document &builder,
        const ::arrow::Array &array)
    {
        using ::bsoncxx::builder::basic::kvp;

        auto n = array.length();
        auto m = ::arrow::BitUtil::BytesForBits(n);
        buffer1_.resize(static_cast<std::size_t>(m));

        if (array.type()->id() == ::arrow::Type::NA) {
            std::memset(buffer1_.data(), 0, buffer1_.size());
        } else {
            if (!buffer1_.empty()) {
                buffer1_.back() = 0;
            }

            if (array.null_count() == 0) {
                ::arrow::BitUtil::SetBitsTo(buffer1_.data(), 0, n, true);
            } else {
                ::arrow::internal::CopyBitmap(array.null_bitmap()->data(),
                    array.offset(), n, buffer1_.data(), 0, true);
            }

            internal::swap_bit_order(buffer1_.size(), buffer1_.data());
        }

        builder.append(kvp(Schema::MASK(),
            compress(buffer1_, &buffer2_, compression_level_)));
    }

  private:
    ::bsoncxx::builder::basic::document &builder_;
    Vector<std::uint8_t> &buffer1_;
    Vector<std::uint8_t> &buffer2_;
    int compression_level_;
};

} // namespace bson

} // namespace dataframe

#endif // DATAFRAME_SERIALIZER_BSON_DATA_WRITER_HPP
