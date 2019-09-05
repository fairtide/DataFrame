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

class ColumnWriter : public ::arrow::ArrayVisitor
{
  public:
    ColumnWriter(int compression_level, ::arrow::MemoryPool *pool)
        : compression_level_(compression_level)
        , buffer1_(Allocator<std::uint8_t>(pool))
        , buffer2_(Allocator<std::uint8_t>(pool))
    {
    }

    ::bsoncxx::document::value write(const ::arrow::Array &array)
    {
        DF_ARROW_ERROR_HANDLER(array.Accept(this));

        return std::move(*column_);
    }

    int compression_level() const { return compression_level_; }

  private:
#define DF_DEFINE_VISITOR(Type)                                               \
    ::arrow::Status Visit(const ::arrow::Type##Array &array) override         \
    {                                                                         \
        using ::bsoncxx::builder::basic::document;                            \
        using ::bsoncxx::builder::basic::kvp;                                 \
                                                                              \
        document col;                                                         \
                                                                              \
        DataWriter data(col, buffer1_, buffer2_, compression_level_);         \
        DF_ARROW_ERROR_HANDLER(array.Accept(&data));                          \
                                                                              \
        column_ =                                                             \
            std::make_unique<::bsoncxx::document::value>(col.extract());      \
                                                                              \
        return ::arrow::Status::OK();                                         \
    }

    DF_DEFINE_VISITOR(Null)
    DF_DEFINE_VISITOR(Boolean)
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
    DF_DEFINE_VISITOR(FixedSizeBinary)
    DF_DEFINE_VISITOR(Binary)
    DF_DEFINE_VISITOR(String)
    DF_DEFINE_VISITOR(List)
    DF_DEFINE_VISITOR(Struct)
    DF_DEFINE_VISITOR(Dictionary)

#undef DF_DEFINE_VISITOR

  private:
    int compression_level_;
    Vector<std::uint8_t> buffer1_;
    Vector<std::uint8_t> buffer2_;
    std::unique_ptr<::bsoncxx::document::value> column_;
};

} // namespace bson

} // namespace dataframe

#endif // DATAFRAME_SERIALIZER_BSON_COLUMN_WRITER_HPP
