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

#include <dataframe/rcpp.hpp>
#include <RInside.h>
#include <gtest/gtest.h>

TEST(Rcpp, DataFrame)
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
    df["Float"] = 32.0f;
    df["Double"] = 64.0f;
    df["Date"] = ::dataframe::Date(2018, 1, 1);
    df["Timestamp"] = ::dataframe::Timestamp(::dataframe::Date(2018, 1, 1));
    df["String"] = "string";
    df["Bool"] = true;

    ::dataframe::CategoricalArray categorical;
    for (std::size_t i = 0; i != df.nrow(); ++i) {
        categorical.push_back("categorical:" + std::to_string(i % 4));
    }
    df["Categorical"] = categorical;

    ::Rcpp::List list;
    ::dataframe::cast_dataframe(df, &list);

    auto ret = ::dataframe::make_dataframe(list);

    EXPECT_TRUE(ret["UInt8"].is_int32());
    EXPECT_TRUE(ret["Int8"].is_int32());
    EXPECT_TRUE(ret["UInt16"].is_int32());
    EXPECT_TRUE(ret["Int16"].is_int32());
    EXPECT_TRUE(ret["UInt32"].is_int32());
    EXPECT_TRUE(ret["Int32"].is_int32());

    EXPECT_TRUE(ret["UInt64"].is_double());
    EXPECT_TRUE(ret["Int64"].is_double());
    EXPECT_TRUE(ret["Float"].is_double());
    EXPECT_TRUE(ret["Double"].is_double());

    EXPECT_TRUE(ret["Date"].is_date());
    EXPECT_TRUE(ret["Timestamp"].is_timestamp());
    EXPECT_TRUE(ret["String"].is_string());
    EXPECT_TRUE(ret["Bool"].is_bool());
    EXPECT_TRUE(ret["Categorical"].is_categorical());

    EXPECT_EQ(ret["UInt8"].as<int>(), df["UInt8"].as<int>());
    EXPECT_EQ(ret["Int8"].as<int>(), df["Int8"].as<int>());
    EXPECT_EQ(ret["UInt16"].as<int>(), df["UInt16"].as<int>());
    EXPECT_EQ(ret["Int16"].as<int>(), df["Int16"].as<int>());
    EXPECT_EQ(ret["UInt32"].as<int>(), df["UInt32"].as<int>());
    EXPECT_EQ(ret["Int32"].as<int>(), df["Int32"].as<int>());

    EXPECT_EQ(ret["UInt64"].as<double>(), df["UInt64"].as<double>());
    EXPECT_EQ(ret["Int64"].as<double>(), df["Int64"].as<double>());
    EXPECT_EQ(ret["Float"].as<double>(), df["Float"].as<double>());
    EXPECT_EQ(ret["Double"].as<double>(), df["Double"].as<double>());

    EXPECT_EQ(ret["Date"].as<::dataframe::Date>(),
        df["Date"].as<::dataframe::Date>());
    EXPECT_EQ(ret["Timestamp"].as<::dataframe::Timestamp>(),
        df["Timestamp"].as<::dataframe::Timestamp>());
    EXPECT_EQ(ret["String"].as<std::string_view>(),
        df["String"].as<std::string_view>());
    EXPECT_EQ(ret["Bool"].as<bool>(), df["Bool"].as<bool>());
    EXPECT_EQ(ret["Categorical"].as<std::string_view>(),
        df["Categorical"].as<std::string_view>());
}

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);

    RInside R;

    return RUN_ALL_TESTS();
}
