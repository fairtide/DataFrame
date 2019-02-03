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
#include <gtest/gtest.h>

TEST(Serializer, RecordBatchStream)
{
    ::dataframe::DataFrame df;
    df["x"] = std::vector<double>({1.1});
    df["y"] = 20;
    df["z"] = "abc";
    ::dataframe::RecordBatchStreamWriter writer;
    writer.write(df);
    auto str = writer.str();
    ::dataframe::RecordBatchStreamReader reader;
    auto ret = reader.read(str);
    EXPECT_EQ(df["x"].view<double>(), ret["x"].view<double>());
    EXPECT_EQ(df["y"].view<int>(), ret["y"].view<int>());
    EXPECT_EQ(df["z"].as<std::string>(), ret["z"].as<std::string>());
}

TEST(Serializer, RecordBatchFile)
{
    ::dataframe::DataFrame df;
    df["x"] = std::vector<double>({1.1});
    df["y"] = 20;
    df["z"] = "abc";
    ::dataframe::RecordBatchFileWriter writer;
    writer.write(df);
    auto str = writer.str();
    ::dataframe::RecordBatchFileReader reader;
    auto ret = reader.read(str);
    EXPECT_EQ(df["x"].view<double>(), ret["x"].view<double>());
    EXPECT_EQ(df["y"].view<int>(), ret["y"].view<int>());
    EXPECT_EQ(df["z"].as<std::string>(), ret["z"].as<std::string>());
}

TEST(Serializer, Feather)
{
    ::dataframe::DataFrame df;
    df["x"] = std::vector<double>({1.1});
    df["y"] = 20;
    df["z"] = "abc";
    ::dataframe::FeatherWriter writer;
    writer.write(df);
    auto str = writer.str();
    ::dataframe::FeatherReader reader;
    auto ret = reader.read(str);
    EXPECT_EQ(df["x"].view<double>(), ret["x"].view<double>());
    EXPECT_EQ(df["y"].view<int>(), ret["y"].view<int>());
    EXPECT_EQ(df["z"].as<std::string>(), ret["z"].as<std::string>());
}

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);

    return RUN_ALL_TESTS();
}
