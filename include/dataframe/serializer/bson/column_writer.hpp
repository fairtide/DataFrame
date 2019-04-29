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

#ifndef DATAFRAME_SERIALIZER_BSON_COLUMN_WRITER_HPP
#define DATAFRAME_SERIALIZER_BSON_COLUMN_WRITER_HPP

#include <dataframe/serializer/bson/data_writer.hpp>
#include <dataframe/serializer/bson/type_writer.hpp>

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
#define DF_DEFINE_VISITOR(Type)                                               \
    ::arrow::Status Visit(const ::arrow::Type##Array &array) final            \
    {                                                                         \
        using ::bsoncxx::builder::basic::document;                            \
        using ::bsoncxx::builder::basic::kvp;                                 \
                                                                              \
        document col;                                                         \
        make_mask(col, array);                                                \
                                                                              \
        DataWriter data(col, buffer1_, buffer2_, compression_level_);         \
        DF_ARROW_ERROR_HANDLER(array.Accept(&data));                          \
                                                                              \
        TypeWriter type(col);                                                 \
        DF_ARROW_ERROR_HANDLER(array.type()->Accept(&type));                  \
                                                                              \
        column_ =                                                             \
            std::make_unique<::bsoncxx::document::value>(col.extract());      \
                                                                              \
        return ::arrow::Status::OK();                                         \
    }

    DF_DEFINE_VISITOR(Null)
    // DF_DEFINE_VISITOR(Boolean)
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
    DF_DEFINE_VISITOR(Interval)
    // DF_DEFINE_VISITOR(Decimal128)
    DF_DEFINE_VISITOR(FixedSizeBinary)
    DF_DEFINE_VISITOR(Binary)
    DF_DEFINE_VISITOR(String)
    DF_DEFINE_VISITOR(List)
    DF_DEFINE_VISITOR(Struct)
    // DF_DEFINE_VISITOR(Union)
    // DF_DEFINE_VISITOR(Dictionary)

#undef DF_DEFINE_VISITOR

    void make_mask(::bsoncxx::builder::basic::document &builder,
        const ::arrow::Array &array)
    {
        auto n = static_cast<std::size_t>(array.length());
        buffer1_.resize((n + 7) / 8);

        if (array.type()->id() == ::arrow::Type::NA) {
            std::fill_n(buffer1_.data(), buffer1_.size(), 0);
        } else {
            if (array.null_count() != 0) {
                throw DataFrameException("Nullable array not supported");
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

        auto mask = compress(static_cast<std::int64_t>(buffer1_.size()),
            buffer1_.data(), &buffer2_, compression_level_);

        builder.append(::bsoncxx::builder::basic::kvp(Schema::MASK(), mask));
    }

  private:
    int compression_level_;
    Vector<std::uint8_t> buffer1_;
    Vector<std::uint8_t> buffer2_;
    std::unique_ptr<::bsoncxx::document::value> column_;
};

} // namespace bson

} // namespace dataframe

#endif // DATAFRAME_SERIALIZER_BSON_COLUMN_WRITER_HPP
