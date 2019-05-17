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

#include <dataframe/serializer/bson.hpp>

#include "test_serializer.hpp"

#include <bsoncxx/json.hpp>
#include <iostream>
#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

struct Output {
    Output() = default;

    ~Output()
    {
        ::dataframe::BSONWriter writer;

        writer.write(data);
        auto bson_doc = writer.extract();

        auto json_str = ::bsoncxx::to_json(
            bson_doc.view(), ::bsoncxx::ExtendedJsonMode::k_canonical);

        ::rapidjson::Document json_doc;
        json_doc.Parse(json_str.c_str());

        ::rapidjson::StringBuffer json_buffer;
        ::rapidjson::PrettyWriter<::rapidjson::StringBuffer> json_writer(
            json_buffer);
        json_doc.Accept(json_writer);

        std::ofstream out("BSONWriter.json");
        out << json_buffer.GetString() << std::endl;
        out.close();
    }

    ::dataframe::DataFrame data;
};

static Output output;

TEMPLATE_TEST_CASE("BSON", "[serializer][template]", TEST_TYPES)
{
    auto array = ::dataframe::make_array<TestType>(make_data<TestType>(5));
    output.data[array->type()->ToString()] = array;
    TestSerializer<TestType, ::dataframe::BSONReader,
        ::dataframe::BSONWriter>();
}
