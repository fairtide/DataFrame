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

#ifndef DATAFRAME_SERIALIZER_BSON_WRITER_HPP
#define DATAFRAME_SERIALIZER_BSON_WRITER_HPP

#include <dataframe/serializer/base.hpp>
#include <dataframe/serializer/bson/compress.hpp>
#include <dataframe/serializer/bson/schema.hpp>

namespace dataframe {

namespace bson {

class ColumnWriter final : public ::arrow::ArrayVisitor
{
  public:
    ColumnWriter(int compression_level)
        : compression_level_(compression_level)
    {
    }

    ::bsoncxx::document::value write(const ::arrow::Array &array)
    {
        DF_ARROW_ERROR_HANDLER(array.Accept(this));

        return std::move(*column_);
    }

  private:
#define DF_DEFINE_BSON_COLUMN_WRITER_VISITOR(Type)                            \
    ::arrow::Status Visit(const ::arrow::Type##Array &array) final            \
    {                                                                         \
        return make_column(array);                                            \
    }

    // DF_DEFINE_BSON_COLUMN_WRITER_VISITOR(Null)
    // DF_DEFINE_BSON_COLUMN_WRITER_VISITOR(Boolean)
    DF_DEFINE_BSON_COLUMN_WRITER_VISITOR(Int8)
    DF_DEFINE_BSON_COLUMN_WRITER_VISITOR(Int16)
    DF_DEFINE_BSON_COLUMN_WRITER_VISITOR(Int32)
    DF_DEFINE_BSON_COLUMN_WRITER_VISITOR(Int64)
    DF_DEFINE_BSON_COLUMN_WRITER_VISITOR(UInt8)
    DF_DEFINE_BSON_COLUMN_WRITER_VISITOR(UInt16)
    DF_DEFINE_BSON_COLUMN_WRITER_VISITOR(UInt32)
    DF_DEFINE_BSON_COLUMN_WRITER_VISITOR(UInt64)
    DF_DEFINE_BSON_COLUMN_WRITER_VISITOR(HalfFloat)
    DF_DEFINE_BSON_COLUMN_WRITER_VISITOR(Float)
    DF_DEFINE_BSON_COLUMN_WRITER_VISITOR(Double)
    // DF_DEFINE_BSON_COLUMN_WRITER_VISITOR(Binary)
    // DF_DEFINE_BSON_COLUMN_WRITER_VISITOR(String)
    // DF_DEFINE_BSON_COLUMN_WRITER_VISITOR(Date32)
    // DF_DEFINE_BSON_COLUMN_WRITER_VISITOR(Date64)
    // DF_DEFINE_BSON_COLUMN_WRITER_VISITOR(Timestamp)
    // DF_DEFINE_BSON_COLUMN_WRITER_VISITOR(Time32)
    // DF_DEFINE_BSON_COLUMN_WRITER_VISITOR(Time64)
    // DF_DEFINE_BSON_COLUMN_WRITER_VISITOR(Interval)
    // DF_DEFINE_BSON_COLUMN_WRITER_VISITOR(Decimal128)
    // DF_DEFINE_BSON_COLUMN_WRITER_VISITOR(FixedSizeBinary)
    // DF_DEFINE_BSON_COLUMN_WRITER_VISITOR(List)
    // DF_DEFINE_BSON_COLUMN_WRITER_VISITOR(Struct)
    // DF_DEFINE_BSON_COLUMN_WRITER_VISITOR(Union)
    // DF_DEFINE_BSON_COLUMN_WRITER_VISITOR(Dictionary)

#undef DF_DEFINE_BSON_COLUMN_WRITER_VISITOR

    template <typename ArrayType>
    ::arrow::Status make_column(const ArrayType &array)
    {
        using ::bsoncxx::builder::basic::document;
        using ::bsoncxx::builder::basic::kvp;

        document col;
        make_mask(col, array);
        make_data(col, array);
        make_type(col, array);

        column_ = std::make_unique<::bsoncxx::document::value>(col.extract());

        return ::arrow::Status::OK();
    }

    void make_mask(::bsoncxx::builder::basic::document &builder,
        const ::arrow::Array &array)
    {
        if (array.null_count() != 0) {
            throw std::runtime_error("Nullable array not supported");
        }

        auto n = static_cast<std::size_t>(array.length());

        buffer1_.resize((n + 7) / 8);

        std::fill_n(buffer1_.data(), buffer1_.size(),
            std::numeric_limits<std::uint8_t>::max());

        if (!buffer1_.empty() && n % 8 != 0) {
            auto last = buffer1_.back();
            auto nzero = (8 - (n % 8));
            buffer1_.back() =
                static_cast<std::uint8_t>((last >> nzero) << nzero);
        }

        auto mask = compress(static_cast<std::int64_t>(buffer1_.size()),
            buffer1_.data(), &buffer2_, compression_level_);

        builder.append(::bsoncxx::builder::basic::kvp(Schema::MASK(), mask));
    }

#define DF_DEFINE_BSON_COLUMN_WRITER_MAKE_DATA(TypeName)                      \
    void make_data(::bsoncxx::builder::basic::document &builder,              \
        const ::arrow::TypeName##Array &array)                                \
    {                                                                         \
        builder.append(::bsoncxx::builder::basic::kvp(Schema::DATA(),         \
            compress(array.length(), array.raw_values(), &buffer2_,           \
                compression_level_)));                                        \
    }

    // DF_DEFINE_BSON_COLUMN_WRITER_MAKE_DATA(Null)
    // DF_DEFINE_BSON_COLUMN_WRITER_MAKE_DATA(Boolean)
    DF_DEFINE_BSON_COLUMN_WRITER_MAKE_DATA(Int8)
    DF_DEFINE_BSON_COLUMN_WRITER_MAKE_DATA(Int16)
    DF_DEFINE_BSON_COLUMN_WRITER_MAKE_DATA(Int32)
    DF_DEFINE_BSON_COLUMN_WRITER_MAKE_DATA(Int64)
    DF_DEFINE_BSON_COLUMN_WRITER_MAKE_DATA(UInt8)
    DF_DEFINE_BSON_COLUMN_WRITER_MAKE_DATA(UInt16)
    DF_DEFINE_BSON_COLUMN_WRITER_MAKE_DATA(UInt32)
    DF_DEFINE_BSON_COLUMN_WRITER_MAKE_DATA(UInt64)
    DF_DEFINE_BSON_COLUMN_WRITER_MAKE_DATA(HalfFloat)
    DF_DEFINE_BSON_COLUMN_WRITER_MAKE_DATA(Float)
    DF_DEFINE_BSON_COLUMN_WRITER_MAKE_DATA(Double)
    // DF_DEFINE_BSON_COLUMN_WRITER_MAKE_DATA(Date32)
    // DF_DEFINE_BSON_COLUMN_WRITER_MAKE_DATA(Date64)
    // DF_DEFINE_BSON_COLUMN_WRITER_MAKE_DATA(Timestamp)
    // DF_DEFINE_BSON_COLUMN_WRITER_MAKE_DATA(Time32)
    // DF_DEFINE_BSON_COLUMN_WRITER_MAKE_DATA(Time64)
    // DF_DEFINE_BSON_COLUMN_WRITER_MAKE_DATA(Interval)
    // DF_DEFINE_BSON_COLUMN_WRITER_MAKE_DATA(Decimal128)
    // DF_DEFINE_BSON_COLUMN_WRITER_MAKE_DATA(FixedSizeBinary)
    // DF_DEFINE_BSON_COLUMN_WRITER_MAKE_DATA(Binary)
    // DF_DEFINE_BSON_COLUMN_WRITER_MAKE_DATA(String)
    // DF_DEFINE_BSON_COLUMN_WRITER_MAKE_DATA(List)
    // DF_DEFINE_BSON_COLUMN_WRITER_MAKE_DATA(Struct)
    // DF_DEFINE_BSON_COLUMN_WRITER_MAKE_DATA(Union)
    // DF_DEFINE_BSON_COLUMN_WRITER_MAKE_DATA(Dictionary)

#undef DF_DEFINE_BSON_COLUMN_WRITER_MAKE_DATA

#define DF_DEFINE_BSON_COLUMN_WRITER_MAKE_TYPE(TypeName, type)                \
    void make_type(::bsoncxx::builder::basic::document &builder,              \
        const ::arrow::TypeName##Array &)                                     \
    {                                                                         \
        builder.append(::bsoncxx::builder::basic::kvp(Schema::TYPE(), type)); \
    }

    DF_DEFINE_BSON_COLUMN_WRITER_MAKE_TYPE(Null, "null")
    DF_DEFINE_BSON_COLUMN_WRITER_MAKE_TYPE(Boolean, "bool")
    DF_DEFINE_BSON_COLUMN_WRITER_MAKE_TYPE(Int8, "int8")
    DF_DEFINE_BSON_COLUMN_WRITER_MAKE_TYPE(Int16, "int16")
    DF_DEFINE_BSON_COLUMN_WRITER_MAKE_TYPE(Int32, "int32")
    DF_DEFINE_BSON_COLUMN_WRITER_MAKE_TYPE(Int64, "int64")
    DF_DEFINE_BSON_COLUMN_WRITER_MAKE_TYPE(UInt8, "uint8")
    DF_DEFINE_BSON_COLUMN_WRITER_MAKE_TYPE(UInt16, "uint16")
    DF_DEFINE_BSON_COLUMN_WRITER_MAKE_TYPE(UInt32, "uint32")
    DF_DEFINE_BSON_COLUMN_WRITER_MAKE_TYPE(UInt64, "uint64")
    DF_DEFINE_BSON_COLUMN_WRITER_MAKE_TYPE(HalfFloat, "float16")
    DF_DEFINE_BSON_COLUMN_WRITER_MAKE_TYPE(Float, "float32")
    DF_DEFINE_BSON_COLUMN_WRITER_MAKE_TYPE(Double, "float64")
    DF_DEFINE_BSON_COLUMN_WRITER_MAKE_TYPE(Date32, "date")
    DF_DEFINE_BSON_COLUMN_WRITER_MAKE_TYPE(Date64, "datestamp")
    DF_DEFINE_BSON_COLUMN_WRITER_MAKE_TYPE(Timestamp, "timestamp")
    DF_DEFINE_BSON_COLUMN_WRITER_MAKE_TYPE(Time32, "time")
    DF_DEFINE_BSON_COLUMN_WRITER_MAKE_TYPE(Time64, "time")
    DF_DEFINE_BSON_COLUMN_WRITER_MAKE_TYPE(Interval, "interval")
    DF_DEFINE_BSON_COLUMN_WRITER_MAKE_TYPE(Decimal128, "decimal")
    DF_DEFINE_BSON_COLUMN_WRITER_MAKE_TYPE(FixedSizeBinary, "pod")
    DF_DEFINE_BSON_COLUMN_WRITER_MAKE_TYPE(Binary, "binary")
    DF_DEFINE_BSON_COLUMN_WRITER_MAKE_TYPE(String, "utf8")
    DF_DEFINE_BSON_COLUMN_WRITER_MAKE_TYPE(List, "list")
    DF_DEFINE_BSON_COLUMN_WRITER_MAKE_TYPE(Struct, "struct")
    DF_DEFINE_BSON_COLUMN_WRITER_MAKE_TYPE(Union, "union")
    DF_DEFINE_BSON_COLUMN_WRITER_MAKE_TYPE(Dictionary, "dictionary")

#undef DF_DEFINE_BSON_COLUMN_WRITER_MAKE_TYPE

  private:
    int compression_level_;
    Vector<std::uint8_t> buffer1_;
    Vector<std::uint8_t> buffer2_;
    std::unique_ptr<::bsoncxx::document::value> column_;
};

} // namespace bson

class BSONWriter : public Writer
{
  public:
    BSONWriter(int compression_level = 0)
        : column_writer_(compression_level)
    {
    }

    std::size_t size() const final
    {
        return data_ == nullptr ? 0 : data_->view().length();
    }

    const std::uint8_t *data() const final
    {
        return data_ == nullptr ? 0 : data_->view().data();
    }

    void write(const DataFrame &data) override
    {
        ::bsoncxx::builder::basic::document builder;

        auto ncol = data.ncol();
        for (std::size_t i = 0; i != ncol; ++i) {
            auto col = data[i];
            builder.append(::bsoncxx::builder::basic::kvp(
                col.name(), column_writer_.write(*col.data())));
        }

        data_ =
            std::make_unique<::bsoncxx::document::value>(builder.extract());
    }

    ::bsoncxx::document::value extract() { return std::move(*data_); }

  protected:
    bson::ColumnWriter column_writer_;
    std::unique_ptr<::bsoncxx::document::value> data_;
};

} // namespace dataframe

#endif // DATAFRAME_SERIALIZER_BSON_WRITER_HPP
