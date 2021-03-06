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
    auto tp = view[Schema::PARAM()];

    std::string_view type(b_type.data(), b_type.size());

    auto read_timestamp_type = [&tp](auto unit) {
        return ::arrow::timestamp(
            unit, tp ? tp.get_utf8().value.data() : std::string());
    };

    auto read_dict_type = [&tp](auto ordered) {
        auto index_type = tp ?
            read_type(tp[Schema::INDEX()].get_document().view()) :
            ::arrow::int32();

        auto dict_type = tp ?
            read_type(tp[Schema::DICT()].get_document().view()) :
            ::arrow::utf8();

        return ::arrow::dictionary(index_type, dict_type, ordered);
    };

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

    if (type == "date[d]") {
        return ::arrow::date32();
    }

    if (type == "date[ms]") {
        return ::arrow::date64();
    }

    if (type == "timestamp[s]") {
        return read_timestamp_type(::arrow::TimeUnit::SECOND);
    }

    if (type == "timestamp[ms]") {
        return read_timestamp_type(::arrow::TimeUnit::MILLI);
    }

    if (type == "timestamp[us]") {
        return read_timestamp_type(::arrow::TimeUnit::MICRO);
    }

    if (type == "timestamp[ns]") {
        return read_timestamp_type(::arrow::TimeUnit::NANO);
    }

    if (type == "time[s]") {
        return ::arrow::time32(::arrow::TimeUnit::SECOND);
    }

    if (type == "time[ms]") {
        return ::arrow::time32(::arrow::TimeUnit::MILLI);
    }

    if (type == "time[us]") {
        return ::arrow::time64(::arrow::TimeUnit::MICRO);
    }

    if (type == "time[ns]") {
        return ::arrow::time64(::arrow::TimeUnit::NANO);
    }

    if (type == "utf8") {
        return ::arrow::utf8();
    }

    if (type == "bytes") {
        return ::arrow::binary();
    }

    if (type == "factor") {
        return read_dict_type(false);
    }

    if (type == "ordered") {
        return read_dict_type(true);
    }

    if (type == "opaque") {
        return ::arrow::fixed_size_binary(tp.get_int32().value);
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
