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

struct DF {
    ::dataframe::DataFrame orig;
    ::dataframe::DataFrame sorted;
    ::dataframe::DataFrame rsorted;
};

inline DF generate_dataframe()
{
    DF ret;

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

    ret.orig["UInt8"] = std::vector<std::uint8_t>(vec.begin(), vec.end());
    ret.orig["Int8"] = std::vector<std::int8_t>(vec.begin(), vec.end());
    ret.orig["UInt16"] = std::vector<std::uint16_t>(vec.begin(), vec.end());
    ret.orig["Int16"] = std::vector<std::int16_t>(vec.begin(), vec.end());
    ret.orig["UInt32"] = std::vector<std::uint32_t>(vec.begin(), vec.end());
    ret.orig["Int32"] = std::vector<std::int32_t>(vec.begin(), vec.end());
    ret.orig["UInt64"] = std::vector<std::uint64_t>(vec.begin(), vec.end());
    ret.orig["Int64"] = std::vector<std::int64_t>(vec.begin(), vec.end());
    ret.orig["Float"] = std::vector<float>(vec.begin(), vec.end());
    ret.orig["Double"] = std::vector<double>(vec.begin(), vec.end());
    ret.orig["Date"] = date;
    ret.orig["Timestamp"] = timestamp;
    ret.orig["String"] = string;
    ret.orig["Categorical"] = categorical;

    std::sort(vec.begin(), vec.end());
    std::sort(date.begin(), date.end());
    std::sort(timestamp.begin(), timestamp.end());
    std::sort(string.begin(), string.end());

    ::dataframe::CategoricalArray categorical_sorted;
    categorical_sorted.emplace_back("odd");
    categorical_sorted.emplace_back("even");
    categorical_sorted.emplace_back("odd");
    categorical_sorted.emplace_back("even");
    categorical_sorted.emplace_back("odd");
    categorical_sorted.emplace_back("even");
    categorical_sorted.emplace_back("odd");
    categorical_sorted.emplace_back("even");

    ret.sorted["UInt8"] = std::vector<std::uint8_t>(vec.begin(), vec.end());
    ret.sorted["Int8"] = std::vector<std::int8_t>(vec.begin(), vec.end());
    ret.sorted["UInt16"] = std::vector<std::uint16_t>(vec.begin(), vec.end());
    ret.sorted["Int16"] = std::vector<std::int16_t>(vec.begin(), vec.end());
    ret.sorted["UInt32"] = std::vector<std::uint32_t>(vec.begin(), vec.end());
    ret.sorted["Int32"] = std::vector<std::int32_t>(vec.begin(), vec.end());
    ret.sorted["UInt64"] = std::vector<std::uint64_t>(vec.begin(), vec.end());
    ret.sorted["Int64"] = std::vector<std::int64_t>(vec.begin(), vec.end());
    ret.sorted["Float"] = std::vector<float>(vec.begin(), vec.end());
    ret.sorted["Double"] = std::vector<double>(vec.begin(), vec.end());
    ret.sorted["Date"] = date;
    ret.sorted["Timestamp"] = timestamp;
    ret.sorted["String"] = string;
    ret.sorted["Categorical"] = categorical_sorted;

    std::reverse(vec.begin(), vec.end());
    std::reverse(date.begin(), date.end());
    std::reverse(timestamp.begin(), timestamp.end());
    std::reverse(string.begin(), string.end());

    ::dataframe::CategoricalArray categorical_rsorted;
    categorical_rsorted.emplace_back("even");
    categorical_rsorted.emplace_back("odd");
    categorical_rsorted.emplace_back("even");
    categorical_rsorted.emplace_back("odd");
    categorical_rsorted.emplace_back("even");
    categorical_rsorted.emplace_back("odd");
    categorical_rsorted.emplace_back("even");
    categorical_rsorted.emplace_back("odd");

    ret.rsorted["UInt8"] = std::vector<std::uint8_t>(vec.begin(), vec.end());
    ret.rsorted["Int8"] = std::vector<std::int8_t>(vec.begin(), vec.end());
    ret.rsorted["UInt16"] = std::vector<std::uint16_t>(vec.begin(), vec.end());
    ret.rsorted["Int16"] = std::vector<std::int16_t>(vec.begin(), vec.end());
    ret.rsorted["UInt32"] = std::vector<std::uint32_t>(vec.begin(), vec.end());
    ret.rsorted["Int32"] = std::vector<std::int32_t>(vec.begin(), vec.end());
    ret.rsorted["UInt64"] = std::vector<std::uint64_t>(vec.begin(), vec.end());
    ret.rsorted["Int64"] = std::vector<std::int64_t>(vec.begin(), vec.end());
    ret.rsorted["Float"] = std::vector<float>(vec.begin(), vec.end());
    ret.rsorted["Double"] = std::vector<double>(vec.begin(), vec.end());
    ret.rsorted["Date"] = date;
    ret.rsorted["Timestamp"] = timestamp;
    ret.rsorted["String"] = string;
    ret.rsorted["Categorical"] = categorical_rsorted;

    return ret;
}

TEST(Sort, UInt8)
{
    auto ret = generate_dataframe();
    auto sorted = ::dataframe::sort(ret.orig, "UInt8");
    auto rsorted = ::dataframe::sort(ret.orig, "UInt8", true);
    EXPECT_EQ(sorted, ret.sorted);
    EXPECT_EQ(rsorted, ret.rsorted);
}

TEST(Sort, Int8)
{
    auto ret = generate_dataframe();
    auto sorted = ::dataframe::sort(ret.orig, "Int8");
    auto rsorted = ::dataframe::sort(ret.orig, "Int8", true);
    EXPECT_EQ(sorted, ret.sorted);
    EXPECT_EQ(rsorted, ret.rsorted);
}

TEST(Sort, UInt16)
{
    auto ret = generate_dataframe();
    auto sorted = ::dataframe::sort(ret.orig, "UInt16");
    auto rsorted = ::dataframe::sort(ret.orig, "UInt16", true);
    EXPECT_EQ(sorted, ret.sorted);
    EXPECT_EQ(rsorted, ret.rsorted);
}

TEST(Sort, Int16)
{
    auto ret = generate_dataframe();
    auto sorted = ::dataframe::sort(ret.orig, "Int16");
    auto rsorted = ::dataframe::sort(ret.orig, "Int16", true);
    EXPECT_EQ(sorted, ret.sorted);
    EXPECT_EQ(rsorted, ret.rsorted);
}

TEST(Sort, UInt32)
{
    auto ret = generate_dataframe();
    auto sorted = ::dataframe::sort(ret.orig, "UInt32");
    auto rsorted = ::dataframe::sort(ret.orig, "UInt32", true);
    EXPECT_EQ(sorted, ret.sorted);
    EXPECT_EQ(rsorted, ret.rsorted);
}

TEST(Sort, Int32)
{
    auto ret = generate_dataframe();
    auto sorted = ::dataframe::sort(ret.orig, "Int32");
    auto rsorted = ::dataframe::sort(ret.orig, "Int32", true);
    EXPECT_EQ(sorted, ret.sorted);
    EXPECT_EQ(rsorted, ret.rsorted);
}

TEST(Sort, UInt64)
{
    auto ret = generate_dataframe();
    auto sorted = ::dataframe::sort(ret.orig, "UInt64");
    auto rsorted = ::dataframe::sort(ret.orig, "UInt64", true);
    EXPECT_EQ(sorted, ret.sorted);
    EXPECT_EQ(rsorted, ret.rsorted);
}

TEST(Sort, Int64)
{
    auto ret = generate_dataframe();
    auto sorted = ::dataframe::sort(ret.orig, "Int64");
    auto rsorted = ::dataframe::sort(ret.orig, "Int64", true);
    EXPECT_EQ(sorted, ret.sorted);
    EXPECT_EQ(rsorted, ret.rsorted);
}

TEST(Sort, Float)
{
    auto ret = generate_dataframe();
    auto sorted = ::dataframe::sort(ret.orig, "Float");
    auto rsorted = ::dataframe::sort(ret.orig, "Float", true);
    EXPECT_EQ(sorted, ret.sorted);
    EXPECT_EQ(rsorted, ret.rsorted);
}

TEST(Sort, Double)
{
    auto ret = generate_dataframe();
    auto sorted = ::dataframe::sort(ret.orig, "Double");
    auto rsorted = ::dataframe::sort(ret.orig, "Double", true);
    EXPECT_EQ(sorted, ret.sorted);
    EXPECT_EQ(rsorted, ret.rsorted);
}

TEST(Sort, Date)
{
    auto ret = generate_dataframe();
    auto sorted = ::dataframe::sort(ret.orig, "Date");
    auto rsorted = ::dataframe::sort(ret.orig, "Date", true);
    EXPECT_EQ(sorted, ret.sorted);
    EXPECT_EQ(rsorted, ret.rsorted);
}

TEST(Sort, Timestamp)
{
    auto ret = generate_dataframe();
    auto sorted = ::dataframe::sort(ret.orig, "Timestamp");
    auto rsorted = ::dataframe::sort(ret.orig, "Timestamp", true);
    EXPECT_EQ(sorted, ret.sorted);
    EXPECT_EQ(rsorted, ret.rsorted);
}

TEST(Sort, String)
{
    auto ret = generate_dataframe();
    auto sorted = ::dataframe::sort(ret.orig, "String");
    auto rsorted = ::dataframe::sort(ret.orig, "String", true);
    EXPECT_EQ(sorted, ret.sorted);
    EXPECT_EQ(rsorted, ret.rsorted);
}

TEST(Sort, Categorical)
{
    auto ret = generate_dataframe();
    EXPECT_THROW(::dataframe::sort(ret.orig, "Categorical"),
        ::dataframe::DataFrameException);
    EXPECT_THROW(::dataframe::sort(ret.orig, "Categorical", true),
        ::dataframe::DataFrameException);
}
