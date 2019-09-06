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

#include <dataframe/table/sort.hpp>

#include <catch2/catch.hpp>

struct DF {
    ::dataframe::DataFrame orig;
    ::dataframe::DataFrame sorted;
    ::dataframe::DataFrame rsorted;
};

inline DF make_dataframe()
{
    using Date = ::dataframe::Datestamp<::dataframe::DateUnit::Day>;

    using Timestamp =
        ::dataframe::Timestamp<::dataframe::TimeUnit::Nanosecond>;

    DF ret;

    std::vector<std::int8_t> vec = {1, 3, 5, 7, 2, 4, 6, 8};

    std::vector<Date> date = {
        Date(1), //
        Date(3), //
        Date(5), //
        Date(7), //
        Date(2), //
        Date(4), //
        Date(6), //
        Date(8)  //
    };

    std::vector<Timestamp> timestamp = {
        Timestamp(1), //
        Timestamp(3), //
        Timestamp(5), //
        Timestamp(7), //
        Timestamp(2), //
        Timestamp(4), //
        Timestamp(6), //
        Timestamp(8)  //
    };

    std::vector<std::string> string = {"1", "3", "5", "7", "2", "4", "6", "8"};

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

    std::sort(vec.begin(), vec.end());
    std::sort(date.begin(), date.end());
    std::sort(timestamp.begin(), timestamp.end());
    std::sort(string.begin(), string.end());

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

    std::reverse(vec.begin(), vec.end());
    std::reverse(date.begin(), date.end());
    std::reverse(timestamp.begin(), timestamp.end());
    std::reverse(string.begin(), string.end());

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

    return ret;
}

#define DEFINE_TEST_CASE(Name)                                                \
    TEST_CASE("Sort DataFrame by " #Name, "[sort]")                           \
    {                                                                         \
        auto ret = make_dataframe();                                          \
        auto sorted = ::dataframe::sort(ret.orig, #Name);                     \
        auto rsorted = ::dataframe::sort(ret.orig, #Name, true);              \
        CHECK(sorted == ret.sorted);                                          \
        CHECK(rsorted == ret.rsorted);                                        \
    }

DEFINE_TEST_CASE(Int8)
DEFINE_TEST_CASE(Int16)
DEFINE_TEST_CASE(Int32)
DEFINE_TEST_CASE(Int64)
DEFINE_TEST_CASE(UInt8)
DEFINE_TEST_CASE(UInt16)
DEFINE_TEST_CASE(UInt32)
DEFINE_TEST_CASE(UInt64)
DEFINE_TEST_CASE(Float)
DEFINE_TEST_CASE(Double)
DEFINE_TEST_CASE(Date)
DEFINE_TEST_CASE(Timestamp)
DEFINE_TEST_CASE(String)
