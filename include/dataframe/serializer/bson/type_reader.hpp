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

#ifndef DATAFRAME_SERIALIZER_BSON_TYPE_READER_HPP
#define DATAFRAME_SERIALIZER_BSON_TYPE_READER_HPP

#include <dataframe/serializer/bson/schema.hpp>

namespace dataframe {

namespace bson {

inline std::shared_ptr<::arrow::DataType> read_type(
    const ::bsoncxx::document::view &view)
{
    auto b_type = view[Schema::TYPE()].get_utf8().value;

    std::string_view type(b_type.data(), b_type.size());

    if (type == "null") {
        return ::arrow::null();
    }

    if (type == "bool") {
        return ::arrow::boolean();
    }

    if (type == "int8") {
        return ::arrow::int8();
    }

    if (type == "int16") {
        return ::arrow::int16();
    }

    if (type == "int32") {
        return ::arrow::int32();
    }

    if (type == "int64") {
        return ::arrow::int64();
    }

    if (type == "uint8") {
        return ::arrow::uint8();
    }

    if (type == "uint16") {
        return ::arrow::uint16();
    }

    if (type == "uint32") {
        return ::arrow::uint32();
    }

    if (type == "uint64") {
        return ::arrow::uint64();
    }

    if (type == "float16") {
        return ::arrow::float16();
    }

    if (type == "float32") {
        return ::arrow::float32();
    }

    if (type == "float64") {
        return ::arrow::float64();
    }

    if (type == "date") {
        return ::arrow::date32();
    }

    if (type == "date[ms]") {
        return ::arrow::date64();
    }

    if (type == "timestamp[s]") {
        return ::arrow::timestamp(::arrow::TimeUnit::SECOND);
    }

    if (type == "timestamp[ms]") {
        return ::arrow::timestamp(::arrow::TimeUnit::MILLI);
    }

    if (type == "timestamp[us]") {
        return ::arrow::timestamp(::arrow::TimeUnit::MICRO);
    }

    if (type == "timestamp") {
        return ::arrow::timestamp(::arrow::TimeUnit::NANO);
    }

    if (type == "time32[s]") {
        return ::arrow::time32(::arrow::TimeUnit::SECOND);
    }

    if (type == "time32[ms]") {
        return ::arrow::time32(::arrow::TimeUnit::MILLI);
    }

    if (type == "time32[us]") {
        return ::arrow::time32(::arrow::TimeUnit::MICRO);
    }

    if (type == "time32[ns]") {
        return ::arrow::time32(::arrow::TimeUnit::NANO);
    }

    if (type == "time64[s]") {
        return ::arrow::time64(::arrow::TimeUnit::SECOND);
    }

    if (type == "time64[ms]") {
        return ::arrow::time64(::arrow::TimeUnit::MILLI);
    }

    if (type == "time64[us]") {
        return ::arrow::time64(::arrow::TimeUnit::MICRO);
    }

    if (type == "time64[ns]") {
        return ::arrow::time64(::arrow::TimeUnit::NANO);
    }

    // if (type == "interval[ym]") {
    //     std::make_shared<::arrow::IntervalType>(
    //         ::arrow::IntervalType::Unit::YEAR_MONTH);
    // }

    // if (type == "interval[dt]") {
    //     std::make_shared<::arrow::IntervalType>(
    //         ::arrow::IntervalType::Unit::DAY_TIME);
    // }

    if (type == "utf8") {
        return ::arrow::utf8();
    }

    if (type == "bytes") {
        return ::arrow::binary();
    }

    if (type == "factor") {
        return ::arrow::dictionary(nullptr, nullptr, false);
    }

    if (type == "ordered") {
        return ::arrow::dictionary(nullptr, nullptr, true);
    }

    auto tp = view[Schema::PARAM()];

    if (type == "pod") {
        return ::arrow::fixed_size_binary(tp.get_int32().value);
    }

    if (type == "decimal") {
        auto precision = tp[Schema::PRECISION()].get_int32().value;
        auto scale = tp[Schema::SCALE()].get_int32().value;
        return ::arrow::decimal(precision, scale);
    }

    if (type == "list") {
        return ::arrow::list(read_type(tp.get_document().view()));
    }

    if (type == "struct") {
        auto param = tp.get_array().value;

        std::vector<std::shared_ptr<::arrow::Field>> fields;
        for (auto &&field : param) {
            auto field_type = field.get_document().view();
            fields.push_back(::arrow::field(
                field_type[Schema::NAME()].get_utf8().value.data(),
                read_type(field_type)));
        }

        return ::arrow::struct_(std::move(fields));
    }

    throw DataFrameException("Unkown type " + std::string(type));
}

} // namespace bson

} // namespace dataframe

#endif // DATAFRAME_SERIALIZER_BSON_TYPE_READER_HPP
