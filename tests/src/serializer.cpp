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

#include <dataframe/dataframe.hpp>
#include <dataframe/serializer/json.hpp>
#include <fstream>
#include <gtest/gtest.h>

inline ::dataframe::DataFrame generate_dataframe()
{
    ::dataframe::DataFrame df;
    df["UInt8"] = std::vector<std::uint8_t>({0, 1, 2, 3, 4, 5, 6, 7});
    df["Int8"] = INT8_C(8);
    df["UInt16"] = UINT16_C(16);
    df["Int16"] = INT16_C(16);
    df["UInt32"] = UINT32_C(32);
    df["Int32"] = INT32_C(32);
    df["UInt64"] = UINT64_C(64);
    df["Int64"] = INT64_C(64);
    df["Float32"] = 32.0f;
    df["Float64"] = 64.0f;
    df["Date"] = ::dataframe::Date(2018, 1, 1);
    df["Timestamp"] = ::dataframe::Timestamp(::dataframe::Date(2018, 1, 1));
    df["String"] = "string";

    ::dataframe::CategoricalArray categorical;
    for (std::size_t i = 0; i != df.nrow(); ++i) {
        categorical.push_back("categorical:" + std::to_string(i % 4));
    }
    df["Categorical"] = categorical;

    return df;
}

TEST(Serializer, RecordBatchStream)
{
    auto df = generate_dataframe();
    ::dataframe::RecordBatchStreamWriter writer;
    writer.write(df);
    auto str = writer.str();
    ::dataframe::RecordBatchStreamReader reader;
    auto ret = reader.read(str);
    EXPECT_EQ(ret, df);
}

TEST(Serializer, RecordBatchFile)
{
    auto df = generate_dataframe();
    ::dataframe::RecordBatchFileWriter writer;
    writer.write(df);
    auto str = writer.str();
    ::dataframe::RecordBatchFileReader reader;
    auto ret = reader.read(str);
    EXPECT_EQ(ret, df);
}

TEST(Serializer, Feather)
{
    auto df = generate_dataframe();
    ::dataframe::FeatherWriter writer;
    writer.write(df);
    auto str = writer.str();
    ::dataframe::FeatherReader reader;
    auto ret = reader.read(str);
    EXPECT_EQ(ret, df);
}

TEST(Serializer, JSONRow)
{
    auto df = generate_dataframe();
    ::dataframe::JSONRowWriter writer("data");
    writer.write(df);
    std::ofstream out("serializer_row.json");
    out << writer.str() << std::endl;
}

TEST(Serializer, JSONColumn)
{
    auto df = generate_dataframe();
    ::dataframe::JSONColumnWriter writer("data");
    writer.write(df);
    std::ofstream out("serializer_column.json");
    out << writer.str() << std::endl;
}

TEST(Serializer, CSV)
{
    auto df = generate_dataframe();
    ::dataframe::CSVWriter writer;
    writer.write(df);
    std::ofstream out("serializer.csv");
    out << writer.str() << std::endl;
}
