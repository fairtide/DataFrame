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

class DataWriter final : public ::arrow::ArrayVisitor
{
  public:
    DataWriter(::bsoncxx::builder::basic::document &builder,
        Vector<std::uint8_t> &buffer1, Vector<std::uint8_t> &buffer2,
        int compression_level, bool ignore_float_na)
        : builder_(builder)
        , buffer1_(buffer1)
        , buffer2_(buffer2)
        , compression_level_(compression_level)
        , ignore_float_na_(ignore_float_na)
    {
    }

    ::arrow::Status Visit(const ::arrow::NullArray &array) final
    {
        builder_.append(::bsoncxx::builder::basic::kvp(
            Schema::DATA(), static_cast<std::int64_t>(array.length())));

        make_mask(builder_, array);

        TypeWriter type_writer(builder_);
        DF_ARROW_ERROR_HANDLER(array.type()->Accept(&type_writer));

        return ::arrow::Status::OK();
    }

    ::arrow::Status Visit(const ::arrow::BooleanArray &array) final
    {
        auto n = array.length();
        buffer1_.resize(static_cast<std::size_t>(n));
        for (std::int64_t i = 0; i != n; ++i) {
            buffer1_[static_cast<std::size_t>(i)] = array.GetView(i);
        }

        builder_.append(::bsoncxx::builder::basic::kvp(Schema::DATA(),
            compress(buffer1_, &buffer2_, compression_level_)));

        make_mask(builder_, array);

        TypeWriter type_writer(builder_);
        DF_ARROW_ERROR_HANDLER(array.type()->Accept(&type_writer));

        return ::arrow::Status::OK();
    }

#define DF_DEFINE_VISITOR(TypeName)                                           \
    ::arrow::Status Visit(const ::arrow::TypeName##Array &array) final        \
    {                                                                         \
        builder_.append(::bsoncxx::builder::basic::kvp(Schema::DATA(),        \
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
    ::arrow::Status Visit(const ::arrow::TypeName##Array &array) final        \
    {                                                                         \
        if (array.null_count() == 0) {                                        \
            builder_.append(::bsoncxx::builder::basic::kvp(Schema::DATA(),    \
                compress(array.length(), array.raw_values(), &buffer2_,       \
                    compression_level_)));                                    \
        } else {                                                              \
            using T = typename ::arrow::TypeName##Array::TypeClass::c_type;   \
            auto n = array.length();                                          \
            buffer1_.resize(sizeof(T) * static_cast<std::size_t>(n));         \
            auto p = reinterpret_cast<T *>(buffer1_.data());                  \
            auto v = array.raw_values();                                      \
            for (std::int64_t i = 0; i != n; ++i) {                           \
                p[i] = array.IsNull(i) ?                                      \
                    std::numeric_limits<T>::quiet_NaN() :                     \
                    v[i];                                                     \
            }                                                                 \
            builder_.append(::bsoncxx::builder::basic::kvp(Schema::DATA(),    \
                compress(n, p, &buffer2_, compression_level_)));              \
        }                                                                     \
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
    ::arrow::Status Visit(const ::arrow::TypeName##Array &array) final        \
    {                                                                         \
        encode_datetime(array.length(), array.raw_values(), &buffer1_);       \
                                                                              \
        builder_.append(::bsoncxx::builder::basic::kvp(Schema::DATA(),        \
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

    ::arrow::Status Visit(const ::arrow::FixedSizeBinaryArray &array) final
    {
        auto &type =
            dynamic_cast<const ::arrow::FixedSizeBinaryType &>(*array.type());

        builder_.append(::bsoncxx::builder::basic::kvp(Schema::DATA(),
            compress(array.length() * type.byte_width(), array.raw_values(),
                &buffer2_, compression_level_)));

        make_mask(builder_, array);

        TypeWriter type_writer(builder_);
        DF_ARROW_ERROR_HANDLER(array.type()->Accept(&type_writer));

        return ::arrow::Status::OK();
    }

    ::arrow::Status Visit(const ::arrow::Decimal128Array &array) final
    {
        return Visit(
            static_cast<const ::arrow::FixedSizeBinaryArray &>(array));
    }

    ::arrow::Status Visit(const ::arrow::BinaryArray &array) final
    {
        using ::bsoncxx::builder::basic::kvp;

        // data

        auto data = array.value_data()->data();
        auto data_offset = array.value_offset(0);
        auto data_length = array.value_offset(array.length()) - data_offset;

        builder_.append(kvp(Schema::DATA(),
            compress(data_length, data + data_offset, &buffer2_,
                compression_level_)));

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

    ::arrow::Status Visit(const ::arrow::StringArray &array) final
    {
        return Visit(static_cast<const ::arrow::BinaryArray &>(array));
    }

    ::arrow::Status Visit(const ::arrow::ListArray &array) final
    {
        using ::bsoncxx::builder::basic::kvp;

        // data

        auto values_offset = array.value_offset(0);
        auto values_length =
            array.value_offset(array.length()) - values_offset;
        auto values = array.values()->Slice(values_offset, values_length);

        ::bsoncxx::builder::basic::document data;
        DataWriter writer(
            data, buffer1_, buffer2_, compression_level_, ignore_float_na_);
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

    ::arrow::Status Visit(const ::arrow::StructArray &array) final
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

            DataWriter writer(field_builder, buffer1_, buffer2_,
                compression_level_, ignore_float_na_);

            DF_ARROW_ERROR_HANDLER(field_data->Accept(&writer));

            fields.append(::bsoncxx::builder::basic::kvp(
                field->name(), field_builder.extract()));
        }

        document data;
        data.append(
            kvp(Schema::LENGTH(), static_cast<std::int64_t>(array.length())));
        data.append(kvp(Schema::FIELDS(), fields.extract()));

        builder_.append(
            ::bsoncxx::builder::basic::kvp(Schema::DATA(), data.extract()));

        // mask

        make_mask(builder_, array);

        // type

        TypeWriter type_writer(builder_);
        DF_ARROW_ERROR_HANDLER(array.type()->Accept(&type_writer));

        return ::arrow::Status::OK();
    }

    ::arrow::Status Visit(const ::arrow::DictionaryArray &array) final
    {
        using ::bsoncxx::builder::basic::document;
        using ::bsoncxx::builder::basic::kvp;

        // data

        document index;
        DataWriter index_writer(
            index, buffer1_, buffer2_, compression_level_, ignore_float_na_);
        DF_ARROW_ERROR_HANDLER(array.indices()->Accept(&index_writer));

        document dict;
        DataWriter dict_writer(
            dict, buffer1_, buffer2_, compression_level_, ignore_float_na_);
        DF_ARROW_ERROR_HANDLER(array.dictionary()->Accept(&dict_writer));

        document data;
        data.append(kvp(Schema::INDEX(), index.extract()));
        data.append(kvp(Schema::DICT(), dict.extract()));

        builder_.append(
            ::bsoncxx::builder::basic::kvp(Schema::DATA(), data.extract()));

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
        auto n = static_cast<std::size_t>(array.length());
        buffer1_.resize((n + 7) / 8);

        if (array.type()->id() == ::arrow::Type::NA) {
            std::fill_n(buffer1_.data(), buffer1_.size(), 0);
        } else {
            if (array.null_count() != 0) {
                if (!ignore_float_na_ ||
                    (array.type()->id() != ::arrow::Type::FLOAT &&
                        array.type()->id() != ::arrow::Type::DOUBLE)) {
                    throw DataFrameException("Nullable array not supported");
                }
            }

            std::fill_n(buffer1_.data(), buffer1_.size(),
                std::numeric_limits<std::uint8_t>::max());

            if (!buffer1_.empty() && n % 8 != 0) {
                auto last = buffer1_.back();
                auto nzero = (8 - (n % 8));
                buffer1_.back() =
                    static_cast<std::uint8_t>((last >> nzero) << nzero);
            }
        }

        auto mask = compress(buffer1_, &buffer2_, compression_level_);

        builder.append(::bsoncxx::builder::basic::kvp(Schema::MASK(), mask));
    }

  private:
    ::bsoncxx::builder::basic::document &builder_;
    Vector<std::uint8_t> &buffer1_;
    Vector<std::uint8_t> &buffer2_;
    int compression_level_;
    bool ignore_float_na_;
};

} // namespace bson

} // namespace dataframe

#endif // DATAFRAME_SERIALIZER_BSON_DATA_WRITER_HPP
