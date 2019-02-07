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
#include <gtest/gtest.h>

TEST(Concatenate, BindRows)
{
    std::vector<std::int8_t> vec = {1, 3, 5, 7, 2, 4, 6, 8};

    std::vector<::dataframe::Date> date = {
        ::dataframe::Date(2018, 1, 1), //
        ::dataframe::Date(2018, 1, 3), //
        ::dataframe::Date(2018, 1, 5), //
        ::dataframe::Date(2018, 1, 7), //
        ::dataframe::Date(2018, 1, 2), //
        ::dataframe::Date(2018, 1, 4), //
        ::dataframe::Date(2018, 1, 6), //
        ::dataframe::Date(2018, 1, 8)  //
    };

    std::vector<::dataframe::Timestamp> timestamp = {
        ::dataframe::Timestamp(::dataframe::Date(2018, 1, 1)), //
        ::dataframe::Timestamp(::dataframe::Date(2018, 1, 3)), //
        ::dataframe::Timestamp(::dataframe::Date(2018, 1, 5)), //
        ::dataframe::Timestamp(::dataframe::Date(2018, 1, 7)), //
        ::dataframe::Timestamp(::dataframe::Date(2018, 1, 2)), //
        ::dataframe::Timestamp(::dataframe::Date(2018, 1, 4)), //
        ::dataframe::Timestamp(::dataframe::Date(2018, 1, 6)), //
        ::dataframe::Timestamp(::dataframe::Date(2018, 1, 8))  //
    };

    std::vector<std::string> string = {"1", "3", "5", "7", "2", "4", "6", "8"};

    ::dataframe::CategoricalArray categorical;
    categorical.emplace_back("odd");
    categorical.emplace_back("odd");
    categorical.emplace_back("odd");
    categorical.emplace_back("odd");
    categorical.emplace_back("even");
    categorical.emplace_back("even");
    categorical.emplace_back("even");
    categorical.emplace_back("even");

    ::dataframe::DataFrame df1;
    df1["UInt8"] = std::vector<std::uint8_t>(vec.begin(), vec.end());
    df1["Int8"] = std::vector<std::int8_t>(vec.begin(), vec.end());
    df1["UInt16"] = std::vector<std::uint16_t>(vec.begin(), vec.end());
    df1["Int16"] = std::vector<std::int16_t>(vec.begin(), vec.end());
    df1["UInt32"] = std::vector<std::uint32_t>(vec.begin(), vec.end());
    df1["Int32"] = std::vector<std::int32_t>(vec.begin(), vec.end());
    df1["UInt64"] = std::vector<std::uint64_t>(vec.begin(), vec.end());
    df1["Int64"] = std::vector<std::int64_t>(vec.begin(), vec.end());
    df1["Float"] = std::vector<float>(vec.begin(), vec.end());
    df1["Double"] = std::vector<double>(vec.begin(), vec.end());
    df1["Date"] = date;
    df1["Timestamp"] = timestamp;
    df1["String"] = string;
    df1["Categorical"] = categorical;

    std::reverse(vec.begin(), vec.end());
    std::reverse(date.begin(), date.end());
    std::reverse(timestamp.begin(), timestamp.end());
    std::reverse(string.begin(), string.end());

    ::dataframe::DataFrame df2;
    df2["UInt8"] = std::vector<std::uint8_t>(vec.begin(), vec.end());
    df2["Int8"] = std::vector<std::int8_t>(vec.begin(), vec.end());
    df2["UInt16"] = std::vector<std::uint16_t>(vec.begin(), vec.end());
    df2["Int16"] = std::vector<std::int16_t>(vec.begin(), vec.end());
    df2["UInt32"] = std::vector<std::uint32_t>(vec.begin(), vec.end());
    df2["Int32"] = std::vector<std::int32_t>(vec.begin(), vec.end());
    df2["UInt64"] = std::vector<std::uint64_t>(vec.begin(), vec.end());
    df2["Int64"] = std::vector<std::int64_t>(vec.begin(), vec.end());
    df2["Float"] = std::vector<float>(vec.begin(), vec.end());
    df2["Double"] = std::vector<double>(vec.begin(), vec.end());
    df2["Date"] = date;
    df2["Timestamp"] = timestamp;
    df2["String"] = string;
    df2["Categorical"] = categorical;

    auto df = ::dataframe::bind_rows({df1, df2});

    EXPECT_EQ(df.nrow(), df1.nrow() + df2.nrow());
    EXPECT_EQ(df.ncol(), df1.ncol());
    EXPECT_EQ(df.rows(0, df1.nrow()), df1);
    EXPECT_EQ(df.rows(df1.nrow(), df.nrow()), df2);
}

TEST(Concatenate, BindCols)
{
    std::vector<std::int8_t> vec = {1, 3, 5, 7, 2, 4, 6, 8};

    std::vector<::dataframe::Date> date = {
        ::dataframe::Date(2018, 1, 1), //
        ::dataframe::Date(2018, 1, 3), //
        ::dataframe::Date(2018, 1, 5), //
        ::dataframe::Date(2018, 1, 7), //
        ::dataframe::Date(2018, 1, 2), //
        ::dataframe::Date(2018, 1, 4), //
        ::dataframe::Date(2018, 1, 6), //
        ::dataframe::Date(2018, 1, 8)  //
    };

    std::vector<::dataframe::Timestamp> timestamp = {
        ::dataframe::Timestamp(::dataframe::Date(2018, 1, 1)), //
        ::dataframe::Timestamp(::dataframe::Date(2018, 1, 3)), //
        ::dataframe::Timestamp(::dataframe::Date(2018, 1, 5)), //
        ::dataframe::Timestamp(::dataframe::Date(2018, 1, 7)), //
        ::dataframe::Timestamp(::dataframe::Date(2018, 1, 2)), //
        ::dataframe::Timestamp(::dataframe::Date(2018, 1, 4)), //
        ::dataframe::Timestamp(::dataframe::Date(2018, 1, 6)), //
        ::dataframe::Timestamp(::dataframe::Date(2018, 1, 8))  //
    };

    std::vector<std::string> string = {"1", "3", "5", "7", "2", "4", "6", "8"};

    ::dataframe::CategoricalArray categorical;
    categorical.emplace_back("odd");
    categorical.emplace_back("odd");
    categorical.emplace_back("odd");
    categorical.emplace_back("odd");
    categorical.emplace_back("even");
    categorical.emplace_back("even");
    categorical.emplace_back("even");
    categorical.emplace_back("even");

    ::dataframe::DataFrame df1;
    df1["UInt8_1"] = std::vector<std::uint8_t>(vec.begin(), vec.end());
    df1["Int8_1"] = std::vector<std::int8_t>(vec.begin(), vec.end());
    df1["UInt16_1"] = std::vector<std::uint16_t>(vec.begin(), vec.end());
    df1["Int16_1"] = std::vector<std::int16_t>(vec.begin(), vec.end());
    df1["UInt32_1"] = std::vector<std::uint32_t>(vec.begin(), vec.end());
    df1["Int32_1"] = std::vector<std::int32_t>(vec.begin(), vec.end());
    df1["UInt64_1"] = std::vector<std::uint64_t>(vec.begin(), vec.end());
    df1["Int64_1"] = std::vector<std::int64_t>(vec.begin(), vec.end());
    df1["Float_1"] = std::vector<float>(vec.begin(), vec.end());
    df1["Double_1"] = std::vector<double>(vec.begin(), vec.end());
    df1["Date_1"] = date;
    df1["Timestamp_1"] = timestamp;
    df1["String_1"] = string;
    df1["Categorical_1"] = categorical;

    std::reverse(vec.begin(), vec.end());
    std::reverse(date.begin(), date.end());
    std::reverse(timestamp.begin(), timestamp.end());
    std::reverse(string.begin(), string.end());

    ::dataframe::DataFrame df2;
    df2["UInt8_2"] = std::vector<std::uint8_t>(vec.begin(), vec.end());
    df2["Int8_2"] = std::vector<std::int8_t>(vec.begin(), vec.end());
    df2["UInt16_2"] = std::vector<std::uint16_t>(vec.begin(), vec.end());
    df2["Int16_2"] = std::vector<std::int16_t>(vec.begin(), vec.end());
    df2["UInt32_2"] = std::vector<std::uint32_t>(vec.begin(), vec.end());
    df2["Int32_2"] = std::vector<std::int32_t>(vec.begin(), vec.end());
    df2["UInt64_2"] = std::vector<std::uint64_t>(vec.begin(), vec.end());
    df2["Int64_2"] = std::vector<std::int64_t>(vec.begin(), vec.end());
    df2["Float_2"] = std::vector<float>(vec.begin(), vec.end());
    df2["Double_2"] = std::vector<double>(vec.begin(), vec.end());
    df2["Date_2"] = date;
    df2["Timestamp_2"] = timestamp;
    df2["String_2"] = string;
    df2["Categorical_2"] = categorical;

    auto df = ::dataframe::bind_cols({df1, df2});

    EXPECT_EQ(df.ncol(), df1.ncol() + df2.ncol());
    EXPECT_EQ(df.nrow(), df1.nrow());
    EXPECT_EQ(df.cols(0, df1.ncol()), df1);
    EXPECT_EQ(df.cols(df1.ncol(), df.ncol()), df2);
}

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);

    return RUN_ALL_TESTS();
}
