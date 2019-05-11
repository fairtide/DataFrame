// ============================================================================
// Copyright 2019 Fairtide Pte. Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// ============================================================================

#include <dataframe/serializer/json.hpp>

#include "make_data.hpp"

#include <iostream>
#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

#include <catch2/catch.hpp>

struct RowOutput {
    RowOutput() = default;

    ~RowOutput()
    {
        ::dataframe::JSONRowWriter writer("data");

        writer.write(data.rows(0, 6));
        auto json_str = writer.str();

        ::rapidjson::Document json_doc;
        json_doc.Parse(json_str.c_str());

        ::rapidjson::StringBuffer json_buffer;
        ::rapidjson::PrettyWriter<::rapidjson::StringBuffer> json_writer(
            json_buffer);
        json_doc.Accept(json_writer);

        std::ofstream out("JSONRowWriter.json");
        out << json_buffer.GetString() << std::endl;
        out.close();
    }

    ::dataframe::DataFrame data;
};

struct ColOutput {
    ColOutput() = default;

    ~ColOutput()
    {
        ::dataframe::JSONColumnWriter writer;

        writer.write(data.rows(0, 6));
        auto json_str = writer.str();

        ::rapidjson::Document json_doc;
        json_doc.Parse(json_str.c_str());

        ::rapidjson::StringBuffer json_buffer;
        ::rapidjson::PrettyWriter<::rapidjson::StringBuffer> json_writer(
            json_buffer);
        json_doc.Accept(json_writer);

        std::ofstream out("JSONColumnWriter.json");
        out << json_buffer.GetString() << std::endl;
        out.close();
    }

    ::dataframe::DataFrame data;
};

static RowOutput row_output;
static ColOutput col_output;

TEMPLATE_TEST_CASE("JSON Row Serializer", "[serializer][template]",
    std::int8_t, std::int16_t, std::int32_t, std::int64_t, std::uint8_t,
    std::uint16_t, std::uint32_t, std::uint64_t,
    ::dataframe::Datestamp<::dataframe::DateUnit::Day>,
    ::dataframe::Datestamp<::dataframe::DateUnit::Millisecond>,
    ::dataframe::Timestamp<::dataframe::TimeUnit::Second>,
    ::dataframe::Timestamp<::dataframe::TimeUnit::Millisecond>,
    ::dataframe::Timestamp<::dataframe::TimeUnit::Microsecond>,
    ::dataframe::Timestamp<::dataframe::TimeUnit::Nanosecond>,
    ::dataframe::Time<::dataframe::TimeUnit::Second>,
    ::dataframe::Time<::dataframe::TimeUnit::Millisecond>,
    ::dataframe::Time<::dataframe::TimeUnit::Microsecond>,
    ::dataframe::Time<::dataframe::TimeUnit::Nanosecond>, std::string)
{
    // TODO void, bool, Dict, Decimal, FixedBinary

    ::dataframe::DataFrame dat;
    std::size_t n = 1000;
    dat["test"].emplace<TestType>(make_data<TestType>(n));
    row_output.data[dat["test"].data()->type()->ToString()] =
        dat["test"].data();
}

TEMPLATE_TEST_CASE("JSON Column Serializer", "[serializer][template]",
    std::int8_t, std::int16_t, std::int32_t, std::int64_t, std::uint8_t,
    std::uint16_t, std::uint32_t, std::uint64_t,
    ::dataframe::Datestamp<::dataframe::DateUnit::Day>,
    ::dataframe::Datestamp<::dataframe::DateUnit::Millisecond>,
    ::dataframe::Timestamp<::dataframe::TimeUnit::Second>,
    ::dataframe::Timestamp<::dataframe::TimeUnit::Millisecond>,
    ::dataframe::Timestamp<::dataframe::TimeUnit::Microsecond>,
    ::dataframe::Timestamp<::dataframe::TimeUnit::Nanosecond>,
    ::dataframe::Time<::dataframe::TimeUnit::Second>,
    ::dataframe::Time<::dataframe::TimeUnit::Millisecond>,
    ::dataframe::Time<::dataframe::TimeUnit::Microsecond>,
    ::dataframe::Time<::dataframe::TimeUnit::Nanosecond>, std::string)
{
    // TODO void, bool, Dict, Decimal, FixedBinary

    ::dataframe::DataFrame dat;
    std::size_t n = 1000;
    dat["test"].emplace<TestType>(make_data<TestType>(n));
    col_output.data[dat["test"].data()->type()->ToString()] =
        dat["test"].data();
}
