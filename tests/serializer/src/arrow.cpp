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

#include <dataframe/serializer.hpp>

#include <catch2/catch.hpp>

TEMPLATE_PRODUCT_TEST_CASE("Test Arrow serializer", "[serializer]", std::tuple,
    (::dataframe::RecordBatchStreamWriter,
        ::dataframe::RecordBatchStreamReader),
    (::dataframe::RecordBatchStreamFileWriter,
        ::dataframe::RecordBatchStreamFileReader),
    (::dataframe::FeatherFileWriter, ::dataframe::FeatherFileReader))
{
    std::vector<std::uint8_t> values({0, 1, 2, 3, 4, 5, 6, 7});
    ::dataframe::DataFrame df;
    df["Int8"].emplace<std::int8_t>(values);
    df["Int16"].emplace<std::int16_t>(values);
    df["Int32"].emplace<std::int32_t>(values);
    df["Int64"].emplace<std::int64_t>(values);
    df["Int64"].emplace<std::int64_t>(values);
    df["UInt8"].emplace<std::uint8_t>(values);
    df["UInt16"].emplace<std::uint16_t>(values);
    df["UInt32"].emplace<std::uint32_t>(values);
    df["UInt64"].emplace<std::uint64_t>(values);
    df["UInt64"].emplace<std::uint64_t>(values);

    std::tuple_element_t<0, TestType> writer;
    writer.write(df);
    auto str = writer.str();
    std::tuple_element_t<1, TestType> reader;
    auto ret = reader.read(str);
    EXPECT_EQ(ret, df);
}
