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

#ifndef DATAFRAME_SERIALIZER_BSON_TYPE_WRITER_HPP
#define DATAFRAME_SERIALIZER_BSON_TYPE_WRITER_HPP

#include <dataframe/serializer/bson/schema.hpp>

namespace dataframe {

namespace bson {

class TypeWriter : public ::arrow::TypeVisitor
{
  public:
    TypeWriter(::bsoncxx::builder::basic::document &builder)
        : builder_(builder)
    {
    }

#define DF_DEFINE_VISITOR(TypeName, type)                                     \
    ::arrow::Status Visit(const ::arrow::TypeName##Type &) override           \
    {                                                                         \
        builder_.append(                                                      \
            ::bsoncxx::builder::basic::kvp(Schema::TYPE(), type));            \
                                                                              \
        return ::arrow::Status::OK();                                         \
    }

    DF_DEFINE_VISITOR(Null, "null")
    DF_DEFINE_VISITOR(Boolean, "bool")
    DF_DEFINE_VISITOR(Int8, "int8")
    DF_DEFINE_VISITOR(Int16, "int16")
    DF_DEFINE_VISITOR(Int32, "int32")
    DF_DEFINE_VISITOR(Int64, "int64")
    DF_DEFINE_VISITOR(UInt8, "uint8")
    DF_DEFINE_VISITOR(UInt16, "uint16")
    DF_DEFINE_VISITOR(UInt32, "uint32")
    DF_DEFINE_VISITOR(UInt64, "uint64")
    DF_DEFINE_VISITOR(HalfFloat, "float16")
    DF_DEFINE_VISITOR(Float, "float32")
    DF_DEFINE_VISITOR(Double, "float64")
    DF_DEFINE_VISITOR(Date32, "date[d]")
    DF_DEFINE_VISITOR(Date64, "date[ms]")
    DF_DEFINE_VISITOR(Binary, "bytes")
    DF_DEFINE_VISITOR(String, "utf8")

#undef DF_DEFINE_VISITOR

    ::arrow::Status Visit(const ::arrow::TimestampType &type) override
    {
        using ::bsoncxx::builder::basic::kvp;

        switch (type.unit()) {
            case ::arrow::TimeUnit::SECOND:
                builder_.append(kvp(Schema::TYPE(), "timestamp[s]"));
                break;
            case ::arrow::TimeUnit::MILLI:
                builder_.append(kvp(Schema::TYPE(), "timestamp[ms]"));
                break;
            case ::arrow::TimeUnit::MICRO:
                builder_.append(kvp(Schema::TYPE(), "timestamp[us]"));
                break;
            case ::arrow::TimeUnit::NANO:
                builder_.append(kvp(Schema::TYPE(), "timestamp[ns]"));
                break;
        }

        return ::arrow::Status::OK();
    }

    ::arrow::Status Visit(const ::arrow::Time32Type &type) override
    {
        using ::bsoncxx::builder::basic::kvp;

        switch (type.unit()) {
            case ::arrow::TimeUnit::SECOND:
                builder_.append(kvp(Schema::TYPE(), "time[s]"));
                break;
            case ::arrow::TimeUnit::MILLI:
                builder_.append(kvp(Schema::TYPE(), "time[ms]"));
                break;
            case ::arrow::TimeUnit::MICRO:
                throw DataFrameException("Unexpected Time32 unit");
                break;
            case ::arrow::TimeUnit::NANO:
                throw DataFrameException("Unexpected Time32 unit");
                break;
        }

        return ::arrow::Status::OK();
    }

    ::arrow::Status Visit(const ::arrow::Time64Type &type) override
    {
        using ::bsoncxx::builder::basic::kvp;

        switch (type.unit()) {
            case ::arrow::TimeUnit::SECOND:
                throw DataFrameException("Unexpected Time64 unit");
                break;
            case ::arrow::TimeUnit::MILLI:
                throw DataFrameException("Unexpected Time64 unit");
                break;
            case ::arrow::TimeUnit::MICRO:
                builder_.append(kvp(Schema::TYPE(), "time[us]"));
                break;
            case ::arrow::TimeUnit::NANO:
                builder_.append(kvp(Schema::TYPE(), "time[ns]"));
                break;
        }

        return ::arrow::Status::OK();
    }

    // ::arrow::Status Visit(const ::arrow::IntervalType &type) override
    // {
    //     using ::bsoncxx::builder::basic::kvp;

    //     switch (type.unit()) {
    //         case ::arrow::IntervalType::Unit::YEAR_MONTH:
    //             builder_.append(kvp(Schema::TYPE(), "interval[ym]"));
    //             break;
    //         case ::arrow::IntervalType::Unit::DAY_TIME:
    //             builder_.append(kvp(Schema::TYPE(), "interval[dt]"));
    //             break;
    //     }

    //     return ::arrow::Status::OK();
    // }

    ::arrow::Status Visit(const ::arrow::FixedSizeBinaryType &type) override
    {
        using ::bsoncxx::builder::basic::kvp;

        builder_.append(kvp(Schema::TYPE(), "pod"));
        builder_.append(kvp(
            Schema::PARAM(), static_cast<std::int32_t>(type.byte_width())));

        return ::arrow::Status::OK();
    }

    ::arrow::Status Visit(const ::arrow::Decimal128Type &type) override
    {
        using ::bsoncxx::builder::basic::kvp;

        builder_.append(kvp(Schema::TYPE(), "decimal"));

        ::bsoncxx::builder::basic::document param;
        param.append(kvp(
            Schema::PRECISION(), static_cast<std::int32_t>(type.precision())));
        param.append(
            kvp(Schema::SCALE(), static_cast<std::int32_t>(type.scale())));

        builder_.append(kvp(Schema::PARAM(), param.extract()));

        return ::arrow::Status::OK();
    }

    ::arrow::Status Visit(const ::arrow::ListType &type) override
    {
        ::bsoncxx::builder::basic::document param;
        TypeWriter writer(param);
        DF_ARROW_ERROR_HANDLER(type.value_type()->Accept(&writer));

        builder_.append(
            ::bsoncxx::builder::basic::kvp(Schema::TYPE(), "list"));
        builder_.append(
            ::bsoncxx::builder::basic::kvp(Schema::PARAM(), param.extract()));

        return ::arrow::Status::OK();
    }

    ::arrow::Status Visit(const ::arrow::StructType &type) override
    {
        using ::bsoncxx::builder::basic::kvp;

        ::bsoncxx::builder::basic::array param;

        auto n = type.num_children();
        for (auto i = 0; i != n; ++i) {
            auto field = type.child(i);

            if (field->name().empty()) {
                throw DataFrameException("empty field name");
            }

            ::bsoncxx::builder::basic::document field_builder;
            field_builder.append(kvp(Schema::NAME(), field->name()));
            TypeWriter writer(field_builder);
            DF_ARROW_ERROR_HANDLER(field->type()->Accept(&writer));
            param.append(field_builder.extract());
        }

        builder_.append(
            ::bsoncxx::builder::basic::kvp(Schema::TYPE(), "struct"));
        builder_.append(
            ::bsoncxx::builder::basic::kvp(Schema::PARAM(), param.extract()));

        return ::arrow::Status::OK();
    }

    ::arrow::Status Visit(const ::arrow::DictionaryType &type) override
    {
        using ::bsoncxx::builder::basic::kvp;

        if (type.ordered()) {
            builder_.append(kvp(Schema::TYPE(), "ordered"));
        } else {
            builder_.append(kvp(Schema::TYPE(), "factor"));
        }

        return ::arrow::Status::OK();
    }

  private:
    ::bsoncxx::builder::basic::document &builder_;
};

} // namespace bson

} // namespace dataframe

#endif // DATAFRAME_SERIALIZER_BSON_TYPE_WRITER_HPP
