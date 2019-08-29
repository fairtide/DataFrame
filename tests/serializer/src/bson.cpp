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

        std::ofstream bin("BSONWriter.bson", std::ios::out | std::ios::binary);
        bin.write(reinterpret_cast<const char *>(bson_doc.view().data()),
            static_cast<std::streamsize>(bson_doc.view().length()));
        bin.close();
    }

    ::dataframe::DataFrame data;
};

static Output output;

TEST_CASE("BSON swap bit order", "[serializer]")
{
    using traits = std::numeric_limits<std::uint8_t>;

    for (unsigned i = traits::min(); i <= traits::max(); ++i) {
        auto ibyte = static_cast<std::uint8_t>(i);
        auto jbyte = ibyte;

        ::dataframe::bson::internal::swap_bit_order(jbyte);

        std::bitset<8> ibits(ibyte);
        std::bitset<8> jbits(jbyte);

        std::vector<bool> iflags(8);
        for (std::size_t k = 0; k != 8; ++k) {
            iflags[k] = ibits[k];
        }

        std::vector<bool> jflags(8);
        for (std::size_t k = 0; k != 8; ++k) {
            jflags[k] = jbits[k];
        }

        CHECK(std::equal(
            iflags.begin(), iflags.end(), jflags.rbegin(), jflags.rend()));
    }
}

TEMPLATE_TEST_CASE("BSON", "[serializer][template]", TEST_TYPES)
{
    auto array = ::dataframe::make_array<TestType>(make_data<TestType>(5));
    output.data[array->type()->ToString()] = array;
    TestSerializer<TestType, ::dataframe::BSONReader,
        ::dataframe::BSONWriter>();
}
